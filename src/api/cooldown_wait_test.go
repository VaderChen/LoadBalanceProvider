package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/proxy"
)

func TestCooldownWaitBudgetAndCancelSmoke(t *testing.T) {
	r := httptest.NewRecorder()
	w := newDeferredResponseWriter(r, true)
	err := &balancer.NoAvailableProviderError{TemporaryOverload: true, RetryAfter: 20 * time.Millisecond}
	spent, ready := waitForProviderCooldown(context.Background(), w, []byte(": ping\n\n"), err, time.Second)
	if !ready || spent < err.RetryAfter || !r.Flushed || w.ContentWritten() {
		t.Fatalf("invalid keepalive wait: %v %v", spent, ready)
	}
	if spent, ready := waitForProviderCooldown(context.Background(), w, []byte(": ping\n\n"), err, time.Millisecond); ready || spent < time.Millisecond {
		t.Fatal("較長冷卻應有限等待，不提前放行或立即退出")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ready := waitForProviderCooldown(ctx, w, []byte(": ping\n\n"), err, time.Second); ready {
		t.Fatal("ignored cancellation")
	}
}

func TestProviderCooldownResumeSmoke(t *testing.T) {
	cfg := &domain.ProxyConfig{Providers: []domain.LLMProviderConfig{{
		ID: "a", Name: "a", Kind: "openai", Type: "openai", Enabled: true,
		BaseURL: "https://8.8.8.8", MaxConcurrent: 4,
		Models: []domain.LLMModelConfig{{Name: "smoke", MaxInputTokens: 100000, MaxOutputTokens: 8192, Capabilities: []string{"chat", "responses"}}},
	}}}
	client := proxy.NewClient()
	client.HTTPClient = &http.Client{Transport: failoverSmokeTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ok\",\"status\":\"completed\",\"output\":[]}}\n\n"))}, nil
	})}
	h := &HTTPAPI{Client: client, Balancer: balancer.NewLoadBalancer(cfg)}
	h.Balancer.ProvidersSnapshot()[0].MarkModelUnavailable("smoke", 0, 30*time.Millisecond)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	h.handleResponsesProxy(w, r, []byte(`{"model":"smoke","stream":true,"input":"hello"}`))
	output := w.Body.String()
	if !strings.Contains(output, "response.ping") || !strings.Contains(output, "resp_ok") || strings.Contains(output, "response.failed") {
		t.Fatalf("cooled provider did not recover on the same response: %s", output)
	}
}
