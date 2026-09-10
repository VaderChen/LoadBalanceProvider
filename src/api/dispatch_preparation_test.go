package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/proxy"
	"LoadBalanceProvider/src/telemetry"
)

func TestDispatchPreparationPreservesReplayRoute(t *testing.T) {
	for _, rebind := range []bool{false, true} {
		name := "same-provider"
		if rebind {
			name = "capacity-rebind"
		}
		t.Run(name, func(t *testing.T) {
			h := capacityTestHandler(t)
			r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			r = r.WithContext(proxy.WithResponseRouteOwner(r.Context(), "owner"))
			r = r.WithContext(context.WithValue(r.Context(), reconnectIdentityKey{}, "request"))
			entry, _ := h.reconnectBudgets.acquire("request", 2)
			entry.provider, entry.model, entry.attempts, entry.probeAttempts = "a", "smoke", 1, 1
			if rebind {
				entry.provider, entry.model, entry.rebindFrom = "", "", "a"
				entry.usedProviders = []string{"a"}
			}
			h.reconnectBudgets.release("request", entry, false)
			if err := h.bindTurnBeforeDispatch("turn", r, "a", "smoke"); err != nil {
				t.Fatal(err)
			}
			want := "a"
			if rebind {
				want = "b"
			}
			calls := 0
			forward := func(_ context.Context, w http.ResponseWriter, target *balancer.ProviderRuntime, _ *domain.LLMModelConfig, _ domain.RequestProfile, _ balancer.SelectionMeta) (proxy.ChatMetrics, error) {
				calls++
				if target.Config.ID != want || entry.provider != want || entry.rebindFrom != "" {
					t.Fatalf("dispatch/replay mismatch: target=%s entry=%+v", target.Config.ID, entry)
				}
				w.WriteHeader(http.StatusOK)
				return proxy.ChatMetrics{}, nil
			}
			req := domain.ChatCompletionRequest{ProviderID: "a", Provider: "a", Model: "smoke"}
			invoke := func(prepare func(*balancer.ProviderRuntime, *domain.LLMModelConfig) error) *httptest.ResponseRecorder {
				w := httptest.NewRecorder()
				h.executeProviderRequest(w, r, req, time.Now(), telemetry.RequestSignals{}, proxy.ResponsesFailureTerminal, proxy.ResponsesRefusalTerminal, nil, func(string) bool { return true }, prepare, forward)
				return w
			}
			w := invoke(func(*balancer.ProviderRuntime, *domain.LLMModelConfig) error {
				return errors.New("storage unavailable")
			})
			if calls != 0 || w.Code != http.StatusServiceUnavailable || entry.attempts != 1 || entry.probeAttempts != 1 || entry.active {
				t.Fatalf("failed preparation consumed dispatch: calls=%d status=%d entry=%+v", calls, w.Code, entry)
			}
			if rebind && (entry.provider != "" || entry.rebindFrom != "a") {
				t.Fatalf("pending rebind lost: %+v", entry)
			}
			for _, p := range h.Balancer.ProviderStatus() {
				if p["active"] != int64(0) {
					t.Fatal("preparation leaked provider slot")
				}
			}
			w = invoke(func(p *balancer.ProviderRuntime, m *domain.LLMModelConfig) error {
				if rebind {
					return h.rebindTurnBeforeDispatch("turn", r, "a", p.Config.ID, m.Name)
				}
				return h.bindTurnBeforeDispatch("turn", r, p.Config.ID, m.Name)
			})
			if calls != 1 || w.Code != http.StatusOK || len(h.reconnectBudgets.entries) != 0 {
				t.Fatalf("recovery failed: calls=%d status=%d body=%s", calls, w.Code, w.Body.String())
			}
		})
	}
}
