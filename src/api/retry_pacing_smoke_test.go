package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/config"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/proxy"
	"LoadBalanceProvider/src/telemetry"
)

func TestRetryPacingSmokeCrossProvider(t *testing.T) {
	h := capacityTestHandler(t)
	s := config.DefaultAdvancedSettingsConfig()
	s.ProviderRetryWaitSeconds = 35
	h.cacheAdvancedSettings(s)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	r := httptest.NewRequest("POST", "/v1/responses", nil).WithContext(ctx)
	var times []time.Time
	var ids []string
	h.executeProviderRequest(httptest.NewRecorder(), r, domain.ChatCompletionRequest{Model: "smoke", Stream: true}, time.Now(), telemetry.RequestSignals{}, proxy.ResponsesFailureTerminal, proxy.ResponsesRefusalTerminal, proxy.ResponsesStreamHeartbeat(), func(string) bool { return true }, nil,
		func(_ context.Context, w http.ResponseWriter, p *balancer.ProviderRuntime, _ *domain.LLMModelConfig, _ domain.RequestProfile, _ balancer.SelectionMeta) (proxy.ChatMetrics, error) {
			times = append(times, time.Now())
			ids = append(ids, p.Config.ID)
			if len(times) == 1 {
				return proxy.ChatMetrics{}, &proxy.ProviderStatusError{StatusCode: 503}
			}
			return proxy.ChatMetrics{}, nil
		})
	if len(times) != 2 {
		t.Fatalf("派送次數錯誤: %d", len(times))
	}
	if ids[0] == ids[1] || times[1].Sub(times[0]) < 30*time.Second {
		t.Fatalf("備援未等待: ids=%v interval=%s", ids, times[1].Sub(times[0]))
	}
	for _, p := range h.Balancer.Providers {
		if p.ActiveCount() != 0 {
			t.Fatal("名額未釋放")
		}
	}
	t.Logf("跨來源 %s -> %s 間隔 %s", ids[0], ids[1], times[1].Sub(times[0]))
}

func TestRetryPacingSmokeOrdinaryReconnect(t *testing.T) {
	for _, prior := range []int{0, 1, 2, 3, 4} {
		h := capacityTestHandler(t)
		r := httptest.NewRequest("POST", "/v1/responses", nil)
		r = r.WithContext(proxy.WithResponseRouteOwner(r.Context(), "owner"))
		r = withReconnectIdentity(r, []byte(`{"model":"smoke","input":"hello"}`))
		key := r.Context().Value(reconnectIdentityKey{}).(string)
		entry, _ := h.reconnectBudgets.acquire(key, 2)
		entry.ordinaryFailures = prior
		entry.admissionWaited = time.Nanosecond
		h.reconnectBudgets.release(key, entry, false)
		calls := 0
		invoke := func() {
			h.executeProviderRequest(httptest.NewRecorder(), r, domain.ChatCompletionRequest{Model: "smoke"}, time.Now(), telemetry.RequestSignals{}, proxy.ResponsesFailureTerminal, proxy.ResponsesRefusalTerminal, nil, nil, nil,
				func(context.Context, http.ResponseWriter, *balancer.ProviderRuntime, *domain.LLMModelConfig, domain.RequestProfile, balancer.SelectionMeta) (proxy.ChatMetrics, error) {
					calls++
					return proxy.ChatMetrics{}, errors.New("connection reset by peer")
				})
		}
		before := time.Now()
		invoke()
		want := time.Duration(min(prior+1, 4)) * 15 * time.Second
		delay := entry.nextDispatchAt.Sub(before)
		if calls != 1 || delay < want || delay > want+time.Second {
			t.Fatalf("prior=%d calls=%d delay=%s want=%s", prior, calls, delay, want)
		}
		deadline := entry.nextDispatchAt
		invoke()
		if calls != 1 || !entry.nextDispatchAt.Equal(deadline) {
			t.Fatal("重連繞過等待或重設期限")
		}
	}
}
