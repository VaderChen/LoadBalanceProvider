package api

import (
	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/proxy"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDowntimeSmokeDiagnosticAndForm(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Taipei")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().In(loc)
	p := domain.LLMProviderConfig{ID: "resting", BaseURL: "https://8.8.8.8", ChatCompletionsPath: "/v1/chat/completions", Downtime: &domain.ProviderDowntime{Enabled: true, Start: now.Add(-time.Hour).Format("15:04"), End: now.Add(time.Hour).Format("15:04")}}
	form := providerConfigToForm(p)
	if form.Downtime == nil || !formToProviderConfig(form).Downtime.Enabled {
		t.Fatal("表單遺失排程")
	}
	form.Downtime = nil
	if !mergeProviderConfig(p, form).Downtime.Enabled {
		t.Fatal("舊客戶端清除了排程")
	}
	calls := 0
	client := proxy.NewClient()
	client.HTTPClient = &http.Client{Transport: failoverSmokeTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: [DONE]\n\n"))}, nil
	})}
	h := &HTTPAPI{Client: client, Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{p}})}
	body := []byte(`{"provider_id":"resting","model":"smoke","messages":[{"role":"user","content":"hello"}]}`)
	w := httptest.NewRecorder()
	h.handleDiagnosticChat(w, httptest.NewRequest("POST", "/api/diagnostics/chat", nil), body)
	if w.Code != 503 || !strings.Contains(w.Body.String(), "provider_scheduled_downtime") || calls != 0 {
		t.Fatalf("停機未攔截: status=%d calls=%d body=%s", w.Code, calls, w.Body.String())
	}
	if w.Header().Get("X-Diagnostic-Origin") != "proxy" {
		t.Fatal("錯誤被標為上游")
	}
	p.Downtime.Enabled = false
	h.Balancer.ReloadConfig(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{p}})
	w = httptest.NewRecorder()
	h.handleDiagnosticChat(w, httptest.NewRequest("POST", "/api/diagnostics/chat", nil), body)
	if w.Code != 200 || calls != 1 || w.Body.String() != "data: [DONE]\n\n" {
		t.Fatalf("手動停用來源無法直接測試: status=%d calls=%d body=%s", w.Code, calls, w.Body.String())
	}
}
