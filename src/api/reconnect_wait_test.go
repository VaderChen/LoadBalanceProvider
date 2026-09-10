package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"LoadBalanceProvider/src/proxy"
)

func TestReconnectCooldownWaitResumesOnSameConnection(t *testing.T) {
	h := capacityTestHandler(t)
	settings := h.currentAdvancedSettings()
	settings.ProviderRetryWaitSeconds = 1
	h.cacheAdvancedSettings(settings)
	r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	r = r.WithContext(proxy.WithResponseRouteOwner(r.Context(), "owner"))
	body := []byte(`{"model":"smoke","stream":true,"prompt_cache_key":"wait-resume","input":"hello"}`)
	if err := h.bindTurnBeforeDispatch(responseTurnRoute(body, r), r, "a", "smoke"); err != nil {
		t.Fatal(err)
	}
	key := withReconnectIdentity(r, body).Context().Value(reconnectIdentityKey{}).(string)
	entry, _ := h.reconnectBudgets.acquire(key, 2)
	entry.attempts, entry.probeAttempts, entry.provider, entry.model = 2, 2, "a", "smoke"
	h.reconnectBudgets.release(key, entry, false)
	entry.retryAt = time.Now().Add(100 * time.Millisecond)
	deadline := entry.retryAt
	calls := 0
	h.Client.HTTPClient = &http.Client{Transport: failoverSmokeTransport(func(req *http.Request) (*http.Response, error) {
		calls++
		if time.Now().Before(deadline) || req.Header.Get("X-Proxy-Provider") != "a" || entry.attempts != entry.limit {
			t.Error("恢復探測提前送出、改變綁定或補滿了額度")
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_resumed\",\"status\":\"completed\",\"output\":[]}}\n\n"))}, nil
	})}
	w := httptest.NewRecorder()
	h.handleResponsesProxy(w, r, body)
	if calls != 1 || !strings.Contains(w.Body.String(), "response.ping") || !strings.Contains(w.Body.String(), "resp_resumed") || strings.Contains(w.Body.String(), "response.failed") || strings.Contains(w.Body.String(), "秒後重試") {
		t.Fatalf("沒有在同一連線恢復真正回應: calls=%d body=%s", calls, w.Body.String())
	}
	if len(h.reconnectBudgets.entries) != 0 || h.Balancer.ProvidersSnapshot()[0].ActiveCount() != 0 {
		t.Fatal("成功後追蹤紀錄或名額未回收")
	}
}

func TestReconnectProbeWindowWaitRestoresBudget(t *testing.T) {
	h := &HTTPAPI{}
	entry, _ := h.reconnectBudgets.acquire("probe", 3)
	entry.probeAttempts = 3
	entry.provider, entry.model = "original", "model"
	h.reconnectBudgets.release("probe", entry, false)
	entry.probeWindowAt = time.Now().Add(-reconnectRetryCooldown + 20*time.Millisecond)
	w := newDeferredResponseWriter(httptest.NewRecorder(), true)
	probe, rejection, err := h.acquireReconnectBudget(context.Background(), "probe", 3, w, proxy.ResponsesStreamHeartbeat(), time.Second, "test")
	if err != nil || rejection != nil || probe == nil {
		t.Fatalf("探測窗口未恢復: %v %+v", err, rejection)
	}
	defer h.reconnectBudgets.release("probe", probe, true)
	if probe.probeAttempts != 0 || probe.attempts != 0 || probe.provider != "original" || w.ContentWritten() || !w.Committed() {
		t.Fatal("等待消耗額度、交付內容或改變了原 Provider")
	}
}

type cancelHeartbeatWriter struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
}

func (w cancelHeartbeatWriter) Write(data []byte) (int, error) {
	w.cancel()
	return w.ResponseRecorder.Write(data)
}

func TestReconnectWaitStopsOnCancelOrDownstreamFailure(t *testing.T) {
	for _, mode := range []string{"cancel", "write_failure"} {
		t.Run(mode, func(t *testing.T) {
			h := &HTTPAPI{}
			entry, _ := h.reconnectBudgets.acquire("waiting", 3)
			entry.attempts = 3
			h.reconnectBudgets.release("waiting", entry, false)
			entry.retryAt = time.Now().Add(time.Second)
			deadline := entry.retryAt
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := error(context.Canceled)
			var target http.ResponseWriter = cancelHeartbeatWriter{httptest.NewRecorder(), cancel}
			if mode == "write_failure" {
				want = io.ErrClosedPipe
				target = failedFlushWriter{httptest.NewRecorder(), want}
			}
			w := newDeferredResponseWriter(target, true)
			probe, _, err := h.acquireReconnectBudget(ctx, "waiting", 3, w, proxy.ResponsesStreamHeartbeat(), 2*time.Second, "test")
			if !errors.Is(err, want) || probe != nil || entry.active || entry.attempts != 3 || entry.retryAt != deadline {
				t.Fatalf("等待取消後仍保留租約、送出或重設額度: %v", err)
			}
		})
	}
}

func TestReconnectWaitHonorsBoundsAndToolSafety(t *testing.T) {
	for _, mode := range []string{"no_wait", "over_budget", "unsafe", "in_progress"} {
		t.Run(mode, func(t *testing.T) {
			h := &HTTPAPI{}
			entry, _ := h.reconnectBudgets.acquire("bounded", 3)
			entry.attempts = 3
			entry.delivered = mode == "unsafe"
			if mode != "in_progress" {
				h.reconnectBudgets.release("bounded", entry, false)
			}
			budget := 10 * time.Millisecond
			if mode == "no_wait" {
				budget = 0
			}
			want := "request_retry_exhausted"
			if mode == "unsafe" {
				want = "request_replay_unsafe"
			} else if mode == "in_progress" {
				want = "request_in_progress"
			}
			recorder := httptest.NewRecorder()
			w := newDeferredResponseWriter(recorder, true)
			probe, rejection, err := h.acquireReconnectBudget(context.Background(), "bounded", 3, w, proxy.ResponsesStreamHeartbeat(), budget, "test")
			if err != nil || probe != nil || rejection == nil || rejection.code != want || w.Committed() != (mode == "over_budget") || w.ContentWritten() {
				t.Fatalf("越過等待或工具安全限制: %v %+v", err, rejection)
			}
		})
	}
}

func TestRetryExhaustedAfterHeartbeatKeepsFailureSemantics(t *testing.T) {
	h := capacityTestHandler(t)
	r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	r = r.WithContext(proxy.WithResponseRouteOwner(r.Context(), "owner"))
	r = r.WithContext(context.WithValue(r.Context(), turnGateHeadersKey{}, true))
	body := []byte(`{"model":"smoke","stream":true,"prompt_cache_key":"gate-throttle","input":"hello"}`)
	key := withReconnectIdentity(r, body).Context().Value(reconnectIdentityKey{}).(string)
	entry, _ := h.reconnectBudgets.acquire(key, 2)
	entry.attempts = 2
	h.reconnectBudgets.release(key, entry, false)
	w := httptest.NewRecorder()
	h.handleResponsesProxy(w, r, body)
	if !strings.Contains(w.Body.String(), "response.failed") || strings.Contains(w.Body.String(), "response.completed") {
		t.Fatalf("已送心跳的節流被偽裝成完成: %s", w.Body.String())
	}
}
