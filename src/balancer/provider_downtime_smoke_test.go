package balancer

import (
	"LoadBalanceProvider/src/domain"
	"testing"
	"time"
)

func TestDowntimeSmokeSelection(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Taipei")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().In(loc)
	p := testSessionProvider("resting")
	p.Downtime = &domain.ProviderDowntime{Enabled: true, Start: now.Add(-time.Hour).Format("15:04"), End: now.Add(time.Hour).Format("15:04")}
	b := NewLoadBalancer(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{p, testSessionProvider("available")}})
	request := &domain.ChatCompletionRequest{Model: "AUTO", Messages: []domain.ChatMessage{{Role: "user", Content: "hello"}}}
	for i := 0; i < 10; i++ {
		selected, _, _, _, err := b.Select(request)
		if err != nil {
			t.Fatal(err)
		}
		if selected.Config.ID != "available" {
			t.Fatal("選中停機來源")
		}
	}
	if b.ProviderAvailableForSelection("resting") {
		t.Fatal("停機來源仍可沿用綁定")
	}
	request.ProviderID = "resting"
	if _, _, _, _, err := b.Select(request); err == nil {
		t.Fatal("指定來源繞過停機限制")
	}
	if b.Providers[0].tryStartRequest() {
		t.Fatal("停機來源仍可取得派送名額")
	}
	p.Downtime.Enabled = false
	b.ReloadConfig(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{p}})
	if _, _, _, _, err := b.Select(request); err != nil {
		t.Fatalf("關閉排程未恢復: %v", err)
	}
}
