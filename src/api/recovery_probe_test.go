package api

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"LoadBalanceProvider/src/proxy"
)

func TestRecoveryProbeGuards(t *testing.T) {
	for _, mode := range []string{"eligible", "used", "tools", "rebind", "budget", "not_exhausted"} {
		t.Run(mode, func(t *testing.T) {
			e := &reconnectBudget{active: true, provider: "a", limit: 3, attempts: 3}
			remaining := time.Second
			switch mode {
			case "tools":
				e.delivered = true
			case "rebind":
				e.rebindFrom = "a"
			case "budget":
				remaining = 0
			case "not_exhausted":
				e.attempts = 2
			}
			w := newDeferredResponseWriter(httptest.NewRecorder(), true)
			if canContinueRecoveryProbe(e, w, mode == "used", remaining) != (mode == "eligible") {
				t.Fatal("恢復探測安全條件不符")
			}
		})
	}
}

func TestRecoveryProbeCancellationReleasesLease(t *testing.T) {
	h := &HTTPAPI{}
	e, _ := h.reconnectBudgets.acquire("key", 3)
	e.provider, e.attempts = "a", 3
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newDeferredResponseWriter(cancelHeartbeatWriter{httptest.NewRecorder(), cancel}, true)
	next, _, err := h.continueRecoveryProbe(ctx, "key", e, w, proxy.ResponsesStreamHeartbeat(), time.Second, "test")
	if next != nil || !errors.Is(err, context.Canceled) || e.active || e.attempts != 3 || e.provider != "a" {
		t.Fatalf("取消後租約或額度不正確: %+v %v", e, err)
	}
}

func TestProviderChangeResetsOnlyWorkAttempts(t *testing.T) {
	for _, target := range []string{"a", "b"} {
		e := &reconnectBudget{provider: "", rebindFrom: "a", attempts: 3, limit: 3, probeAttempts: 3, waited: time.Second, usedProviders: []string{"a"}}
		reset := resetReboundProviderAttempts(e, target)
		if reset != (target == "b") || (reset && e.attempts != 0) || (!reset && e.attempts != 3) || e.probeAttempts != 3 || e.waited != time.Second || len(e.usedProviders) != 1 {
			t.Fatalf("改綁額度重設不正確: %+v", e)
		}
	}
}
