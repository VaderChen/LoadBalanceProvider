package balancer

import (
	"net/http"
	"testing"
	"time"
)

func TestAccountUsagePrimaryAndHeaderFallback(t *testing.T) {
	p := &ProviderRuntime{}
	headers := func(used string) http.Header {
		return http.Header{"X-Codex-Primary-Used-Percent": {used}}
	}
	p.RecordUsageHeaders(headers("10"))
	if !p.RecordCodexAccountUsage(headers("20"), time.Minute) {
		t.Fatal("account usage rejected")
	}
	started := time.Now()
	p.RecordUsageHeaders(headers("30"))
	if p.UsageSnapshot().OverallRemainingPercent() != 80 {
		t.Fatal("header overwrote primary API source")
	}
	p.AccountUsageUnavailable(started)
	if p.UsageSnapshot().OverallRemainingPercent() != 70 {
		t.Fatal("API failure did not activate recent header fallback")
	}
	p.RecordUsageHeaders(headers("40"))
	if p.UsageSnapshot().OverallRemainingPercent() != 60 {
		t.Fatal("header fallback stopped updating")
	}
	p.RecordCodexAccountUsage(headers("50"), time.Minute)
	p.AccountUsageUnavailable(started)
	if p.UsageSnapshot().OverallRemainingPercent() != 50 {
		t.Fatal("old failure replaced newer API success")
	}
	p.accountUsageUntil = time.Now().Add(-time.Second)
	p.RecordUsageHeaders(headers("60"))
	if p.UsageSnapshot().OverallRemainingPercent() != 40 {
		t.Fatal("expired API source blocked header fallback")
	}
}

func TestAccountUsageWindowRemovalAndProbeSchedule(t *testing.T) {
	p := &ProviderRuntime{}
	p.RecordUsageHeaders(http.Header{"X-Codex-Primary-Used-Percent": {"10"}, "X-Codex-Secondary-Used-Percent": {"70"}})
	if !p.RecordCodexAccountUsage(http.Header{"X-Codex-Primary-Used-Percent": {"20"}}, time.Minute) || p.UsageSnapshot().OverallRemainingPercent() != 80 {
		t.Fatal("authoritative API could not remove obsolete window")
	}
	before := p.UsageSnapshot()
	if p.RecordCodexAccountUsage(http.Header{}, time.Minute) || !p.UsageSnapshot().UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatal("missing usage replaced valid observation")
	}
	if !p.ShouldProbeAccountUsage(time.Now(), time.Minute) {
		t.Fatal("fresh response headers suppressed account API polling")
	}
	p.MarkUsageProbeAttempt(time.Now())
	if p.ShouldProbeAccountUsage(time.Now(), time.Minute) {
		t.Fatal("API polling ignored its own interval")
	}
}
