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
		// 節流不是上游故障：送 response.failed 會讓 Codex 判定回合中斷
		// （顯示 stream disconnected before completion）並立刻重連，
		// 等於把節流變成更吵的重試。必須以「完成」型終止事件交付原因。
		if stream {
			body := w.Body.String()
			if w.Code != 200 || !strings.Contains(body, "response.completed") {
				t.Fatalf("stream retry budget returned hard error: %d %s", w.Code, body)
			}
			if strings.Contains(body, "response.failed") {
				t.Fatalf("throttling must not be reported as an upstream failure: %s", body)
			}
			if !strings.Contains(body, "秒後重試") {
				t.Fatalf("client cannot read the throttle reason: %s", body)
			}
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

func TestUnsafeReplayAfterGateHeartbeatUsesCompletedNotice(t *testing.T) {
	h := capacityTestHandler(t)
	r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	r = r.WithContext(proxy.WithResponseRouteOwner(r.Context(), "owner"))
	r = r.WithContext(context.WithValue(r.Context(), turnGateHeadersKey{}, true))
	body := []byte(`{"model":"smoke","stream":true,"prompt_cache_key":"unsafe","input":"hello"}`)
	key := withReconnectIdentity(r, body).Context().Value(reconnectIdentityKey{}).(string)
	entry, _ := h.reconnectBudgets.acquire(key, 2)
	entry.attempts, entry.delivered = 1, true
	h.reconnectBudgets.release(key, entry, false)
	w := httptest.NewRecorder()
	h.handleResponsesProxy(w, r, body)
	if strings.Contains(w.Body.String(), "response.failed") || !strings.Contains(w.Body.String(), "response.completed") || !strings.Contains(w.Body.String(), "完整工具呼叫") {
		t.Fatalf("unsafe replay did not produce a completed notice: %s", w.Body.String())
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
	if !entry.retryAt.IsZero() {
		t.Fatal("pending rebind entered local cooldown")
	}
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

// -------------------------------------------------------------------------------------
// 未送達內容的容量拒絕不消耗代理跨重連額度，仍保留每輪嘗試上限。
func TestCapacityRejectionDoesNotConsumeReplayBudget(t *testing.T) {
	h := capacityTestHandler(t)
	r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	r = r.WithContext(proxy.WithResponseRouteOwner(r.Context(), "owner"))
	body := []byte(`{"model":"smoke","stream":true,"prompt_cache_key":"refund","input":"hello"}`)
	key := withReconnectIdentity(r, body).Context().Value(reconnectIdentityKey{}).(string)

	calls := 0
	h.Client.HTTPClient = &http.Client{Transport: failoverSmokeTransport(func(req *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}},
			Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"Selected model is at capacity. Please try a different model.\"}}}\n\n"))}, nil
	})}

	w := httptest.NewRecorder()
	h.handleResponsesProxy(w, r, body)
	if calls == 0 {
		t.Fatal("no upstream attempt was made")
	}
	if calls > 2 {
		t.Fatalf("refund bypassed per-request attempt limit: %d", calls)
	}
	// 真正的上游故障仍須保留失敗語意。
	if !strings.Contains(w.Body.String(), "response.failed") || strings.Contains(w.Body.String(), "response.completed") {
		t.Fatalf("upstream failure was reported as completion: %s", w.Body.String())
	}
	// 全部嘗試皆未送達內容，額度退回；若曾首次排隊仍保留等待紀錄。
	if entry := h.reconnectBudgets.entries[key]; entry != nil {
		if entry.attempts != 0 {
			t.Fatalf("capacity rejections consumed %d replay attempts", entry.attempts)
		}
		// 允許重新選路時不另加代理冷卻。
		if entry.rebindFrom != "" && !entry.retryAt.IsZero() {
			t.Fatalf("a pending rebind must not be parked behind a cooldown: rebindFrom=%s retryAt=%v", entry.rebindFrom, entry.retryAt)
		}
	}

	// 關鍵：用戶端重連時必須拿到完整額度，而不是一則「額度已用盡」的節流訊息。
	next, rejection := h.reconnectBudgets.acquire(key, 3)
	if rejection != nil {
		t.Fatalf("a capacity storm must not block the next reconnect: %s", rejection.code)
	}
	if next.attempts != 0 {
		t.Fatalf("reconnect started with %d attempts already spent", next.attempts)
	}
}

// -------------------------------------------------------------------------------------
// 重播封鎖只該針對「客戶端可能已經執行過的工具呼叫」。
// 用「有 bytes 送出」當判準，會讓串到一半斷掉的回合變成永久死路：
// 客戶端重送被 400 擋住，再送再擋，agent 完全走不下去。
func TestReplayBlockedOnlyByDeliveredToolCall(t *testing.T) {
	prose := newDeferredResponseWriter(httptest.NewRecorder(), true)
	if _, err := prose.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"半句話\"}\n\n")); err != nil {
		t.Fatal(err)
	}
	if !prose.ContentWritten() {
		t.Fatal("text should count as delivered content")
	}
	if prose.ToolCallDelivered() {
		t.Fatal("plain text must not trigger the tool replay guard")
	}

	reasoning := newDeferredResponseWriter(httptest.NewRecorder(), true)
	if _, err := reasoning.Write([]byte("data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"thinking\"}\n\n")); err != nil {
		t.Fatal(err)
	}
	if reasoning.ToolCallDelivered() {
		t.Fatal("reasoning summaries must stay replayable")
	}

	// 此策略假設客戶端等待完整工具呼叫才執行。
	partial := newDeferredResponseWriter(httptest.NewRecorder(), true)
	if _, err := partial.Write([]byte("data: {\"type\":\"response.function_call_arguments.delta\",\"delta\":\"{\\\"path\\\"\"}\n\n")); err != nil {
		t.Fatal(err)
	}
	if partial.ToolCallDelivered() {
		t.Fatal("an incomplete tool call must not trigger the tool replay guard")
	}

	for _, done := range []string{
		"data: {\"type\":\"response.function_call_arguments.done\",\"arguments\":\"{}\"}\n\n",
		"data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"local_shell_call\",\"id\":\"c1\"}}\n\n",
	} {
		w := newDeferredResponseWriter(httptest.NewRecorder(), true)
		if _, err := w.Write([]byte(done)); err != nil {
			t.Fatal(err)
		}
		if !w.ToolCallDelivered() {
			t.Fatalf("a completed tool call must block replay: %s", done)
		}
	}
}

func TestReplayLedgerTracksDeliveredTools(t *testing.T) {
	for _, kind := range []string{"response.output_text.delta", "response.reasoning_summary_text.delta", "response.function_call_arguments.delta", "response.function_call_arguments.done"} {
		t.Run(kind, func(t *testing.T) {
			h := capacityTestHandler(t)
			r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			r = r.WithContext(proxy.WithResponseRouteOwner(r.Context(), "owner"))
			body := []byte(`{"model":"smoke","stream":true,"prompt_cache_key":"ledger","input":"hello"}`)
			key := withReconnectIdentity(r, body).Context().Value(reconnectIdentityKey{}).(string)
			calls := 0
			h.Client.HTTPClient = &http.Client{Transport: failoverSmokeTransport(func(req *http.Request) (*http.Response, error) {
				calls++
				payload, _ := json.Marshal(map[string]string{"type": kind, "delta": "text", "arguments": "{}"})
				stream := "data: " + string(payload) + "\n\ndata: {\"type\":\"error\",\"message\":\"Our servers are currently overloaded\"}\n\n"
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(stream))}, nil
			})}
			h.handleResponsesProxy(httptest.NewRecorder(), r, body)
			if calls != 1 {
				t.Fatalf("replayed inside an already-started stream: %d", calls)
			}
			entry, rejection := h.reconnectBudgets.acquire(key, 2)
			if strings.HasSuffix(kind, ".done") {
				if rejection == nil || rejection.code != "request_replay_unsafe" {
					t.Fatal("delivered tool did not block reconnect replay")
				}
			} else {
				if rejection != nil || entry.delivered {
					t.Fatalf("non-tool content blocked reconnect: %+v", rejection)
				}
				h.reconnectBudgets.release(key, entry, false)
			}
		})
	}
}
