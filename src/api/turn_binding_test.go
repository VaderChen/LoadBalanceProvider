package api

import (
	"LoadBalanceProvider/src/balancer"
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/proxy"
)

func TestResponseTurnRouteBoundaries(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/responses", nil)
	first := responseTurnRoute([]byte(`{"prompt_cache_key":"session","input":[{"role":"user","content":"question"}]}`), r)
	continued := responseTurnRoute([]byte(`{"prompt_cache_key":"session","input":[{"role":"user","content":"question"},{"type":"custom_tool_call_output","call_id":"c1","output":"done"}]}`), r)
	next := responseTurnRoute([]byte(`{"prompt_cache_key":"session","input":[{"role":"user","content":"question"},{"role":"user","content":"question"}]}`), r)
	if first == "" || first != continued || first != next {
		t.Fatal("without an explicit turn ID, history changes must not change the provider binding")
	}
	r.Header.Set("X-Proxy-Turn-ID", "turn-2")
	if first == responseTurnRoute([]byte(`{"prompt_cache_key":"session","input":"question"}`), r) {
		t.Fatal("explicit new turn must create a new binding")
	}
	r.Header.Del("X-Proxy-Turn-ID")
	otherOwner := r.WithContext(proxy.WithResponseRouteOwner(r.Context(), "other"))
	if first == responseTurnRoute([]byte(`{"prompt_cache_key":"session","input":[{"role":"user","content":"question"}]}`), otherOwner) {
		t.Fatal("turn binding must be isolated by authenticated owner")
	}
}

func TestFixedProviderToolContinuationWithoutRoute(t *testing.T) {
	h := &HTTPAPI{Client: proxy.NewClient()}
	r := httptest.NewRequest("POST", "/v1/responses", nil)
	req := domain.ChatCompletionRequest{ProviderID: "original"}
	_, err := h.applyTurnBinding(&req, []byte(`{"input":[{"type":"function_call_output","call_id":"c1","output":"done"}]}`), r)
	if err != nil || req.ProviderID != "original" {
		t.Fatalf("explicit provider must remain usable for tool results: %v", err)
	}
}

func TestIncrementalRequestKeepsPreviousProvider(t *testing.T) {
	h := &HTTPAPI{Client: proxy.NewClient()}
	r := httptest.NewRequest("POST", "/v1/responses", nil)
	r.Header.Set("X-Proxy-Turn-ID", "next")
	h.Client.RecordPromptCacheRoute("resp_previous", "original", "model", proxy.ResponseRouteOwner(r))
	req := domain.ChatCompletionRequest{}
	body := []byte(`{"prompt_cache_key":"session","previous_response_id":"resp_previous","input":"next question"}`)
	_, err := h.applyTurnBinding(&req, body, r)
	if err != nil || req.ProviderID != "original" {
		t.Fatalf("incremental history must remain on its original provider: %v", err)
	}
}

func TestTurnBindingCannotSwitchProvider(t *testing.T) {
	h := durableBindingTestHandler(t)
	r := httptest.NewRequest("POST", "/v1/responses", nil)
	if err := h.bindTurnBeforeDispatch("turn-binding:test", r, "a", "model"); err != nil {
		t.Fatal(err)
	}
	if err := h.bindTurnBeforeDispatch("turn-binding:test", r, "b", "model"); err == nil {
		t.Fatal("a second provider must never receive the same turn")
	}
	if err := h.bindTurnBeforeDispatch("turn-binding:test", r, "a", "model"); err != nil {
		t.Fatal(err)
	}
}

func durableBindingTestHandler(t *testing.T) *HTTPAPI {
	t.Helper()
	return &HTTPAPI{Client: proxy.NewClient(), AdvancedSettingsConfigPath: filepath.Join(t.TempDir(), "advanced.json"),
		Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{
			{ID: "a", Kind: "openai", Type: "openai", BaseURL: "https://example.com", APIKey: "test-key", Enabled: true},
			{ID: "b", Kind: "openai", Type: "openai", BaseURL: "https://example.com", APIKey: "test-key-b", Enabled: true},
		}})}
}

func TestTurnBindingRestoresAfterRestart(t *testing.T) {
	h := durableBindingTestHandler(t)
	r := httptest.NewRequest("POST", "/v1/responses", nil)
	if err := h.bindTurnBeforeDispatch("turn-binding:restart", r, "a", "model"); err != nil {
		t.Fatal(err)
	}
	restarted := &HTTPAPI{Client: proxy.NewClient(), Balancer: h.Balancer, AdvancedSettingsConfigPath: h.AdvancedSettingsConfigPath}
	target, ok, err := restarted.lookupDurableTurnRoute("turn-binding:restart", proxy.ResponseRouteOwner(r))
	if err != nil || !ok || target.ProviderID != "a" {
		t.Fatalf("restore failed: %v %v %#v", err, ok, target)
	}
	if _, ok, err := restarted.lookupDurableTurnRoute("turn-binding:restart", "different-owner"); err != nil || ok {
		t.Fatal("binding crossed owner boundary")
	}
}

func TestLocalTurnConflictDoesNotPenalizeProvider(t *testing.T) {
	provider := &balancer.ProviderRuntime{}
	recordProviderForwardFailure(provider, turnError("local conflict"), nil, time.Second, time.Second, time.Second)
	if provider.FailureCount() != 0 {
		t.Fatal("local binding conflict must not count as upstream failure")
	}
}

func TestTurnGateCancellationAndCleanup(t *testing.T) {
	h := &HTTPAPI{}
	r := httptest.NewRequest("POST", "/v1/responses", nil)
	release, err := h.acquireTurnGate("same", r)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(r.Context())
	cancel()
	if _, err := h.acquireTurnGate("same", r.WithContext(ctx)); err == nil {
		t.Fatal("cancelled waiter must stop")
	}
	release()
	release()
	if len(h.turnGates) != 0 {
		t.Fatal("gate was not reclaimed")
	}
}
