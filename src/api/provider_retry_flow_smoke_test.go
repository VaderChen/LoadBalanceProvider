package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/config"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/proxy"
)

func TestAdmissionWaitDoesNotConsumeFailureBackoff(t *testing.T) {
	cfg := &domain.ProxyConfig{RetryCount: 1, Providers: []domain.LLMProviderConfig{{
		ID: "only", Name: "only", Kind: "openai", Type: "openai", Enabled: true,
		BaseURL: "https://8.8.8.8", MaxConcurrent: 4,
		Models: []domain.LLMModelConfig{{Name: "smoke", MaxInputTokens: 100000, MaxOutputTokens: 8192, Capabilities: []string{"chat", "responses"}}},
	}}}
	var calls atomic.Int32
	client := proxy.NewClient()
	client.HTTPClient = &http.Client{Transport: failoverSmokeTransport(func(r *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return &http.Response{StatusCode: 500, Header: http.Header{"Content-Type": {"application/json"}, "Retry-After": {"1"}}, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"server_error","message":"internal server error"}}`))}, nil
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_recovered\",\"status\":\"completed\",\"output\":[]}}\n\n"))}, nil
	})}
	h := &HTTPAPI{Client: client, Balancer: balancer.NewLoadBalancer(cfg), AdvancedSettingsConfigPath: filepath.Join(t.TempDir(), "advanced.json")}
	settings := config.DefaultAdvancedSettingsConfig()
	settings.ProviderRetryWaitSeconds = 1
	h.cacheAdvancedSettings(settings)
	h.Balancer.Providers[0].MarkTemporaryUnavailable(0, 600*time.Millisecond)
	w := httptest.NewRecorder()
	h.handleResponsesProxy(w, httptest.NewRequest(http.MethodPost, "/v1/responses", nil), []byte(`{"model":"smoke","stream":true,"input":"hello"}`))
	if calls.Load() != 2 || !strings.Contains(w.Body.String(), "resp_recovered") {
		t.Fatalf("initial queue consumed retry backoff: calls=%d body=%s", calls.Load(), w.Body.String())
	}
}

func TestProviderRetryFlowSmoke(t *testing.T) {
	for _, tc := range []struct {
		name         string
		rounds       int
		afterContent bool
		wantCalls    int
	}{
		{"round-limit", 0, false, 1},
		{"capacity-failover-without-wait", 1, false, 2},
		{"no-replay-after-content", 2, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &domain.ProxyConfig{RetryCount: 2}
			for _, id := range []string{"a", "b"} {
				cfg.Providers = append(cfg.Providers, domain.LLMProviderConfig{ID: id, Name: id, Kind: "openai", Type: "openai", Enabled: true, BaseURL: "https://8.8.8.8", MaxConcurrent: 4,
					Models: []domain.LLMModelConfig{{Name: "smoke", MaxInputTokens: 100000, MaxOutputTokens: 8192, Capabilities: []string{"chat", "responses"}}}})
			}
			var calls atomic.Int32
			client := proxy.NewClient()
			client.HTTPClient = &http.Client{Transport: failoverSmokeTransport(func(r *http.Request) (*http.Response, error) {
				n := calls.Add(1)
				status, body := 200, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ok\",\"status\":\"completed\",\"output\":[]}}\n\n"
				if n == 1 {
					status, body = 503, `{"error":{"code":"server_error","message":"internal server error"}}`
					if tc.afterContent {
						status = 200
						body = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\",\"message\":\"internal server error\"}}}\n\n"
					}
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}
			h := &HTTPAPI{Client: client, Balancer: balancer.NewLoadBalancer(cfg), AdvancedSettingsConfigPath: filepath.Join(t.TempDir(), "advanced.json")}
			settings := config.DefaultAdvancedSettingsConfig()
			settings.ProviderRetryRounds = tc.rounds
			settings.ProviderRetrySourcesPerRound = 1
			settings.ProviderRetryWaitSeconds = 0
			h.cacheAdvancedSettings(settings)
			w := httptest.NewRecorder()
			h.handleResponsesProxy(w, httptest.NewRequest(http.MethodPost, "/v1/responses", nil), []byte(`{"model":"smoke","stream":true,"input":"hello"}`))
			if int(calls.Load()) != tc.wantCalls {
				t.Fatalf("calls=%d want=%d response=%s", calls.Load(), tc.wantCalls, w.Body.String())
			}
			if tc.wantCalls == 2 && !strings.Contains(w.Body.String(), "resp_ok") {
				t.Fatalf("missing successful response: %s", w.Body.String())
			}
			if tc.afterContent && !strings.Contains(w.Body.String(), "response.failed") {
				t.Fatal("failure after content was hidden")
			}
		})
	}
}
