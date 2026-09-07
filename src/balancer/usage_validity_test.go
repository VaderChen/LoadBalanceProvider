package balancer

import (
	"net/http"
	"testing"
)

func TestUsageValidityRejectsMissingAndInvalidValues(t *testing.T) {
	for _, headers := range []map[string]string{
		{"x-ratelimit-limit-requests": "100"},
		{"x-codex-primary-reset-at": "1800000000"},
		{"x-codex-primary-used-percent": "NaN"},
		{"x-codex-primary-used-percent": "101"},
		{"x-ratelimit-limit-tokens": "100", "x-ratelimit-remaining-tokens": "invalid"},
	} {
		s := buildProviderUsageSnapshot(headers)
		if _, known := s.KnownRemainingPercent(); known || s.HasUsageInfo() {
			t.Fatalf("invalid usage accepted: %v", headers)
		}
	}
	for _, tc := range []struct {
		value string
		want  float64
	}{{"0", 100}, {"100", 0}, {"25", 75}} {
		s := buildProviderUsageSnapshot(map[string]string{"x-codex-primary-used-percent": tc.value})
		if value, known := s.KnownRemainingPercent(); !known || value != tc.want {
			t.Fatalf("value=%v known=%v", value, known)
		}
	}
}

func TestIncompleteUsagePreservesLastSnapshot(t *testing.T) {
	p := &ProviderRuntime{}
	p.RecordUsageHeaders(http.Header{"X-Codex-Primary-Used-Percent": {"10"}, "X-Codex-Secondary-Used-Percent": {"70"}})
	before := p.UsageSnapshot()
	p.RecordUsageHeaders(http.Header{"X-Codex-Primary-Used-Percent": {"12"}})
	p.RecordUsageHeaders(http.Header{"X-Ratelimit-Limit-Requests": {"100"}})
	after := p.UsageSnapshot()
	if !after.UpdatedAt.Equal(before.UpdatedAt) || after.OverallRemainingPercent() != 30 {
		t.Fatal("partial snapshot replaced valid usage")
	}
}
