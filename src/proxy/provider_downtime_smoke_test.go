package proxy

import (
	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/domain"
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

type downtimeSmokeTransport struct{ calls int }

func (s *downtimeSmokeTransport) RoundTrip(*http.Request) (*http.Response, error) {
	s.calls++
	return nil, errors.New("停機測試不應呼叫上游")
}

func TestDowntimeSmokeUpstreamGuards(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Taipei")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().In(loc)
	cfg := &domain.LLMProviderConfig{Enabled: true, BaseURL: "https://8.8.8.8", Downtime: &domain.ProviderDowntime{Enabled: true, Start: now.Add(-time.Hour).Format("15:04"), End: now.Add(time.Hour).Format("15:04")}}
	p := &balancer.ProviderRuntime{Config: cfg}
	transport := &downtimeSmokeTransport{}
	c := NewClient()
	c.HTTPClient = &http.Client{Transport: transport}
	ctx := context.Background()
	if providerShouldRefreshUsage(p) {
		t.Fatal("背景用量更新未停用")
	}
	if err := c.refreshProviderUsageForProvider(ctx, p, true); err != nil {
		t.Fatal(err)
	}
	if err := c.TestProviderMinimalChat(ctx, p); err == nil {
		t.Fatal("最小探測未停用")
	}
	if _, err := c.OpenDiagnosticChat(ctx, *cfg, domain.ChatCompletionRequest{}); !errors.Is(err, domain.ErrProviderScheduledDowntime) {
		t.Fatalf("直接對話未停用: %v", err)
	}
	req, err := http.NewRequest("POST", "https://8.8.8.8/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := doCodexHTTPRequest(c.HTTPClient, req, p, true); !errors.Is(err, domain.ErrProviderScheduledDowntime) {
		t.Fatalf("Codex 派送未停用: %v", err)
	}
	if transport.calls != 0 {
		t.Fatalf("仍送出 %d 次請求", transport.calls)
	}
}
