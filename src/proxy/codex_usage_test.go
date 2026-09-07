package proxy

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/codexauth"
	"LoadBalanceProvider/src/domain"
)

func TestCodexAccountUsageParsing(t *testing.T) {
	for _, tc := range []struct {
		body  string
		valid bool
	}{
		{`{"rate_limit":{"primary_window":{"used_percent":0,"limit_window_seconds":604800},"secondary_window":null}}`, true},
		{`{"rate_limit":{"primary_window":{"used_percent":100},"secondary_window":{"used_percent":25.5}}}`, true},
		{`{"rate_limit":{"primary_window":null,"secondary_window":{"used_percent":1}}}`, true},
		{`{}`, false},
		{`{"rate_limit":{"primary_window":{"used_percent":10}}}`, false},
		{`{"rate_limit":{"primary_window":null,"secondary_window":null}}`, false},
		{`{"rate_limit":{"primary_window":{},"secondary_window":null}}`, false},
		{`{"rate_limit":{"primary_window":{"used_percent":101},"secondary_window":null}}`, false},
		{`{"rate_limit":{"primary_window":{"used_percent":-1},"secondary_window":null}}`, false},
		{`{"rate_limit":{"primary_window":{"used_percent":"10"},"secondary_window":null}}`, false},
	} {
		headers, err := codexAccountUsageHeaders([]byte(tc.body))
		if (err == nil) != tc.valid {
			t.Fatalf("body=%s headers=%v err=%v", tc.body, headers, err)
		}
	}
}

func TestCodexUsageRefreshUsesGETAndPreservesFallback(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := codexauth.NewStore("").Save(codexauth.OAuthTokenRecord{
		ProviderID: "usage-test", AccessToken: "test-token", ExpiresAt: time.Now().Add(time.Hour).Format(time.RFC3339),
	}); err != nil {
		t.Fatal(err)
	}
	p := &balancer.ProviderRuntime{Config: &domain.LLMProviderConfig{ID: "usage-test", Kind: "codex", Type: "openai", Enabled: true, BaseURL: "https://chatgpt.com"}}
	client := NewClient()
	calls := 0
	status := http.StatusOK
	body := `{"rate_limit":{"primary_window":{"used_percent":20},"secondary_window":null}}`
	client.HTTPClient = &http.Client{Transport: timeoutSmokeTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method != http.MethodGet || r.URL.String() != "https://chatgpt.com/backend-api/wham/usage" {
			t.Fatalf("unexpected usage request: %s %s", r.Method, r.URL)
		}
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	if err := client.refreshOpenAICodexOAuthUsage(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	p.RecordUsageHeaders(http.Header{"X-Codex-Primary-Used-Percent": {"30"}})
	if p.UsageSnapshot().OverallRemainingPercent() != 80 {
		t.Fatal("headers replaced fresh account usage")
	}
	status = http.StatusServiceUnavailable
	if err := client.refreshOpenAICodexOAuthUsage(context.Background(), p); err == nil || p.UsageSnapshot().OverallRemainingPercent() != 70 {
		t.Fatalf("failed API did not retain fallback: %v", err)
	}
	status, body = http.StatusOK, `{}`
	before := p.UsageSnapshot()
	if err := client.refreshOpenAICodexOAuthUsage(context.Background(), p); err == nil || !p.UsageSnapshot().UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatal("missing API usage was treated as successful refresh")
	}
	if calls != 3 {
		t.Fatalf("unexpected API requests: %d", calls)
	}
}
