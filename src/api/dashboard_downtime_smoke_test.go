package api

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/domain"
)

func TestDowntimeSmokeDashboardPayload(t *testing.T) {
	providers := []domain.LLMProviderConfig{
		{ID: "day", Downtime: &domain.ProviderDowntime{Enabled: true, Start: "04:00", End: "05:00"}},
		{ID: "overnight", Downtime: &domain.ProviderDowntime{Enabled: true, Start: "23:00", End: "01:00"}},
		{ID: "off", Downtime: &domain.ProviderDowntime{Start: "04:00", End: "05:00"}},
		{ID: "legacy"},
	}
	h := &HTTPAPI{Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{Providers: providers})}
	// 只測快照傳遞，避免觸發背景帳號與歷史讀取。
	h.dashboardCache.nextRefresh = time.Now().Add(time.Hour)
	w := httptest.NewRecorder()
	h.handleDashboardSnapshot(w)
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var result struct {
		Providers []dashboardProvider `json:"providers"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Providers) != len(providers) {
		t.Fatal("來源數量不符")
	}
	byID := make(map[string]dashboardProvider)
	for _, p := range result.Providers {
		byID[p.ID] = p
	}
	for _, expected := range providers {
		got := byID[expected.ID].Downtime
		if expected.Downtime != nil {
			if got == nil || *got != *expected.Downtime {
				t.Fatalf("%s 排程遺失或變更: %+v", expected.ID, got)
			}
		} else if got != nil && got.Enabled {
			t.Fatal("舊來源被自動啟用排程")
		}
	}
}
