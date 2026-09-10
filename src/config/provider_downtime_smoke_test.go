package config

import (
	"LoadBalanceProvider/src/domain"
	"path/filepath"
	"testing"
)

func TestDowntimeSmokePersistence(t *testing.T) {
	cfg := &domain.ProxyConfig{Providers: []domain.LLMProviderConfig{{ID: "a"}, {ID: "b", Downtime: &domain.ProviderDowntime{Enabled: true, Start: "23:00", End: "01:00"}}}}
	path := filepath.Join(t.TempDir(), "proxy.json")
	if err := SaveProxyConfig(path, cfg); err != nil {
		t.Fatal(err)
	}
	got, err := LoadProxyConfig("", path)
	if err != nil {
		t.Fatal(err)
	}
	a, b := got.Providers[0].Downtime, got.Providers[1].Downtime
	if a == nil || a.Enabled || a.Start != "04:00" || a.End != "05:00" {
		t.Fatalf("預設錯誤: %+v", a)
	}
	if b == nil || !b.Enabled || b.Start != "23:00" || b.End != "01:00" {
		t.Fatalf("獨立排程未保存: %+v", b)
	}
	got.Providers[1].Downtime.End = "23:00"
	if SaveProxyConfig(path, got) == nil {
		t.Fatal("無效排程仍可保存")
	}
}
