package api

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"LoadBalanceProvider/src/auth"
	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/config"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/proxy"
)

func capacityTestHandler(t *testing.T) *HTTPAPI {
	t.Helper()
	cfg := &domain.ProxyConfig{RetryCount: 1}
	for _, id := range []string{"a", "b"} {
		cfg.Providers = append(cfg.Providers, domain.LLMProviderConfig{
			ID: id, Name: id, Kind: "openai", Type: "openai", Enabled: true, BaseURL: "https://8.8.8.8", MaxConcurrent: 4,
			Models: []domain.LLMModelConfig{{Name: "smoke", MaxInputTokens: 100000, MaxOutputTokens: 8192, Capabilities: []string{"chat", "responses", "tools"}}},
		})
	}
	h := &HTTPAPI{Client: proxy.NewClient(), Balancer: balancer.NewLoadBalancer(cfg), AdvancedSettingsConfigPath: filepath.Join(t.TempDir(), "advanced.json")}
	settings := config.DefaultAdvancedSettingsConfig()
	settings.ProviderRetryWaitSeconds = 0
	h.cacheAdvancedSettings(settings)
	return h
}

func TestCapacityRebindSafetyAndPersistence(t *testing.T) {
	for _, scenario := range []string{"full-history", "key-forced", "body-forced", "previous-only", "delivered"} {
		t.Run(scenario, func(t *testing.T) {
			h := capacityTestHandler(t)
			r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			r = r.WithContext(proxy.WithResponseRouteOwner(r.Context(), "test-owner"))
			payload := map[string]interface{}{"model": "smoke", "stream": true, "prompt_cache_key": "conversation", "input": []interface{}{
				map[string]interface{}{"role": "user", "content": "hello"},
				map[string]interface{}{"type": "reasoning", "encrypted_content": "old-account-only"},
			}}
			if scenario == "key-forced" {
				r = r.WithContext(context.WithValue(r.Context(), requestAPIKeyContextKey{}, auth.APIKeyView{KeyType: auth.APIKeyTypeChat, ProviderID: "a", Model: "AUTO", ReasoningEffort: "AUTO"}))
			}
			if scenario == "body-forced" {
				payload["provider_id"] = "a"
			}
			if scenario == "previous-only" {
				payload["previous_response_id"], payload["input"] = "resp_previous", "continue"
				h.Client.RecordPromptCacheRoute("resp_previous", "a", "smoke", proxy.ResponseRouteOwner(r))
			}
			body, _ := json.Marshal(payload)
			route := responseTurnRoute(body, r)
			if err := h.bindTurnBeforeDispatch(route, r, "a", "smoke"); err != nil {
				t.Fatal(err)
			}
			var providers []string
			h.Client.HTTPClient = &http.Client{Transport: failoverSmokeTransport(func(req *http.Request) (*http.Response, error) {
				providers = append(providers, req.Header.Get("X-Proxy-Provider"))
				raw, _ := io.ReadAll(req.Body)
				response := "data: {\"type\":\"error\",\"message\":\"Selected model is at capacity. Please try a different model.\"}\n\n"
				if len(providers) > 1 {
					if strings.Contains(string(raw), "old-account-only") {
						t.Error("account-specific reasoning crossed providers")
					}
					response = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ok\",\"status\":\"completed\",\"output\":[]}}\n\n"
				} else if scenario == "delivered" {
					response = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"already delivered\"}\n\n" + response
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(response))}, nil
			})}
			w := httptest.NewRecorder()
			h.handleResponsesProxy(w, r, body)
			if scenario != "full-history" {
				if len(providers) != 1 || providers[0] != "a" {
					t.Fatalf("unsafe failover: providers=%v response=%s", providers, w.Body.String())
				}
				return
			}
			if len(providers) != 2 || providers[0] != "a" || providers[1] != "b" || !strings.Contains(w.Body.String(), "resp_ok") {
				t.Fatalf("capacity failover failed: providers=%v response=%s", providers, w.Body.String())
			}
			restarted := &HTTPAPI{Client: proxy.NewClient(), Balancer: h.Balancer, AdvancedSettingsConfigPath: h.AdvancedSettingsConfigPath}
			for _, key := range []string{route, "recovered:" + route} {
				bound, ok, err := restarted.lookupDurableTurnRoute(key, proxy.ResponseRouteOwner(r))
				if err != nil || !ok || bound.ProviderID != "b" {
					t.Fatalf("rebound route not durable: %s %v", key, err)
				}
			}
			if len(h.reconnectBudgets.entries) != 0 {
				t.Fatal("successful failover retained retry budget")
			}
		})
	}
}

func TestExhaustedReplayUsesProtocolTerminal(t *testing.T) {
	for _, stream := range []bool{true, false} {
		h := capacityTestHandler(t)
		r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		r = r.WithContext(proxy.WithResponseRouteOwner(r.Context(), "owner"))
		body, _ := json.Marshal(map[string]interface{}{"model": "smoke", "stream": stream, "prompt_cache_key": "turn", "input": "hello"})
		identified := withReconnectIdentity(r, body)
		key := identified.Context().Value(reconnectIdentityKey{}).(string)
		entry, _ := h.reconnectBudgets.acquire(key, 2)
		entry.attempts = 2
		h.reconnectBudgets.release(key, entry, false)
		deadline := entry.retryAt
		w := httptest.NewRecorder()
		h.handleResponsesProxy(w, r, body)
		if w.Header().Get("Retry-After") == "" || !entry.retryAt.Equal(deadline) {
			t.Fatal("cooldown header missing or blocked replay extended deadline")
		}
		if stream && (w.Code != 200 || !strings.Contains(w.Body.String(), "response.failed")) {
			t.Fatalf("stream retry budget returned hard error: %d %s", w.Code, w.Body.String())
		}
		if !stream && w.Code != 429 {
			t.Fatalf("non-stream retry budget status=%d", w.Code)
		}
		entry.retryAt = time.Now().Add(-time.Second)
		probe, rejection := h.reconnectBudgets.acquire(key, 2)
		if rejection != nil || probe.limit-probe.attempts != 1 {
			t.Fatal("cooldown replenished more than one probe")
		}
	}
}

func TestCapacityCooldownProbeCanRebind(t *testing.T) {
	h := capacityTestHandler(t)
	r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	r = r.WithContext(proxy.WithResponseRouteOwner(r.Context(), "owner"))
	body := []byte(`{"model":"smoke","stream":true,"prompt_cache_key":"probe","input":"hello"}`)
	if err := h.bindTurnBeforeDispatch(responseTurnRoute(body, r), r, "a", "smoke"); err != nil {
		t.Fatal(err)
	}
	key := withReconnectIdentity(r, body).Context().Value(reconnectIdentityKey{}).(string)
	entry, _ := h.reconnectBudgets.acquire(key, 2)
	entry.attempts, entry.rebindFrom, entry.usedProviders = 2, "a", []string{"a"}
	h.reconnectBudgets.release(key, entry, false)
	entry.retryAt = time.Now().Add(-time.Second)
	calls := 0
	h.Client.HTTPClient = &http.Client{Transport: failoverSmokeTransport(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.Header.Get("X-Proxy-Provider") != "b" {
			t.Error("recovery probe returned to exhausted provider")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"probe_ok\",\"status\":\"completed\",\"output\":[]}}\n\n"))}, nil
	})}
	w := httptest.NewRecorder()
	h.handleResponsesProxy(w, r, body)
	if calls != 1 || !strings.Contains(w.Body.String(), "probe_ok") || len(h.reconnectBudgets.entries) != 0 {
		t.Fatalf("probe reset budget or failed rebind: calls=%d body=%s", calls, w.Body.String())
	}
}

func TestCapacityTurnGateDoesNotQueueDuplicateReplay(t *testing.T) {
	h := capacityTestHandler(t)
	body := []byte(`{"model":"smoke","stream":true,"prompt_cache_key":"duplicate","input":"hello"}`)
	request := func() *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		return r.WithContext(proxy.WithResponseRouteOwner(r.Context(), "owner"))
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	h.Client.HTTPClient = &http.Client{Transport: failoverSmokeTransport(func(req *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-release:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n"))}, nil
	})}
	go func() {
		defer close(done)
		h.handleResponsesProxy(httptest.NewRecorder(), request(), body)
	}()
	defer func() { close(release); <-done }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first request did not reach upstream")
	}
	w := httptest.NewRecorder()
	h.handleResponsesProxy(w, request(), body)
	if w.Code != 429 || !strings.Contains(w.Body.String(), "request_in_progress") || calls.Load() != 1 {
		t.Fatalf("duplicate replay queued or dispatched: calls=%d body=%s", calls.Load(), w.Body.String())
	}
}

func TestCapacityTurnGateHeartbeatCarriesIntoResponse(t *testing.T) {
	h := capacityTestHandler(t)
	body := []byte(`{"model":"smoke","stream":true,"prompt_cache_key":"waiting","input":"hello"}`)
	r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	r = r.WithContext(proxy.WithResponseRouteOwner(r.Context(), "owner"))
	release, err := h.acquireTurnGate(responseTurnRoute(body, r), r)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	h.Client.HTTPClient = &http.Client{Transport: failoverSmokeTransport(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"after_gate\",\"status\":\"completed\",\"output\":[]}}\n\n"))}, nil
	})}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		raw, _ := io.ReadAll(req.Body)
		h.handleResponsesProxy(w, req.WithContext(proxy.WithResponseRouteOwner(req.Context(), "owner")), raw)
	}))
	defer func() { release(); server.Close() }()
	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Post(server.URL+"/v1/responses", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	first, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(first, "response.ping") {
		t.Fatalf("queued request did not receive heartbeat: %q %v", first, err)
	}
	release()
	rest, err := io.ReadAll(reader)
	if err != nil || response.StatusCode != 200 || !strings.Contains(string(rest), "after_gate") {
		t.Fatalf("response did not continue after gate heartbeat: status=%d body=%s err=%v", response.StatusCode, rest, err)
	}
}
