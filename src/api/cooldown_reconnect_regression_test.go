package api

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/proxy"
)

func TestLongCooldownWaitStopsOnDownstreamFailure(t *testing.T) {
	w := newDeferredResponseWriter(failedFlushWriter{httptest.NewRecorder(), io.ErrClosedPipe}, true)
	_, ready, err := waitForProviderCooldownResult(context.Background(), w, proxy.ResponsesStreamHeartbeat(), &balancer.NoAvailableProviderError{TemporaryOverload: true, RetryAfter: time.Minute}, time.Second)
	if ready || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("下游失敗未停止等待: ready=%v err=%v", ready, err)
	}
}

func TestWaitingBudgetRecoversWithoutChangingProviderOrWorkAttempts(t *testing.T) {
	h := &HTTPAPI{}
	e, _ := h.reconnectBudgets.acquire("waiting", 3)
	e.provider, e.model, e.attempts, e.probeAttempts = "original", "model", 1, 1
	e.waited = time.Second
	deadline := time.Now().Add(40 * time.Millisecond)
	e.probeWindowAt = deadline.Add(-reconnectRetryCooldown)
	e.waitRetryAt = deadline
	h.reconnectBudgets.release("waiting", e, false)
	w := newDeferredResponseWriter(httptest.NewRecorder(), true)
	started := time.Now()
	probe, rejection, err := h.acquireReconnectBudget(context.Background(), "waiting", 3, w, proxy.ResponsesStreamHeartbeat(), 5*time.Millisecond, "test")
	if err != nil || probe != nil || rejection == nil || rejection.code != "request_wait_throttled" || time.Since(started) < 5*time.Millisecond || !w.Committed() {
		t.Fatalf("未有限等待至預算用盡: %+v %v", rejection, err)
	}
	if e.waitRetryAt != deadline || e.attempts != 1 || e.active {
		t.Fatal("等待延長期限、消耗次數或保留租約")
	}
	w = newDeferredResponseWriter(httptest.NewRecorder(), true)
	probe, rejection, err = h.acquireReconnectBudget(context.Background(), "waiting", 3, w, proxy.ResponsesStreamHeartbeat(), time.Second, "test")
	if err != nil || rejection != nil || probe == nil {
		t.Fatalf("期限到期未恢復資格: %+v %v", rejection, err)
	}
	defer h.reconnectBudgets.release("waiting", probe, true)
	if time.Now().Before(deadline) || probe.provider != "original" || probe.model != "model" || probe.attempts != 1 || probe.waited != 0 || probe.probeAttempts != 0 {
		t.Fatalf("恢復後來源或工作額度不正確: %+v", probe)
	}
}
