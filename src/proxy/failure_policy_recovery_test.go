package proxy

import (
	"context"
	"testing"
	"time"
)

func TestQuotaResetDetails(t *testing.T) {
	now := time.Unix(1800000000, 0)
	for _, value := range []interface{}{float64(172800), "172800"} {
		d := failureDetailsFromPayload(map[string]interface{}{"type": "usage_limit_reached", "resets_in_seconds": value}, now)
		if d.RetryAfter != 48*time.Hour || !d.QuotaResetAt.Equal(now.Add(48*time.Hour)) {
			t.Fatalf("reset details: %+v", d)
		}
	}
	d := failureDetailsFromPayload(map[string]interface{}{"type": "usage_limit_reached", "resets_at": now.Add(time.Hour).Unix(), "resets_in_seconds": 60, "retry_after": 120}, now)
	if d.RetryAfter != time.Hour || !d.QuotaResetAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("reset precedence: %+v", d)
	}
	d = failureDetailsFromPayload(map[string]interface{}{"type": "usage_limit_reached", "resets_at": now.Add(time.Hour).Unix(), "retry_after": 7200}, now)
	if d.RetryAfter != 2*time.Hour || !d.QuotaResetAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("較長的 Retry-After 不可被額度重設時間縮短: %+v", d)
	}
	d = failureDetailsFromPayload(map[string]interface{}{"type": "rate_limit_exceeded", "resets_in_seconds": 172800}, now)
	if !d.QuotaResetAt.IsZero() {
		t.Fatal("transient rate limit became quota cooldown")
	}
}

func TestFailureActionsAndQuotaClassification(t *testing.T) {
	for _, tc := range []struct {
		code         string
		status       int
		action       FailureAction
		quota, model bool
	}{
		{"context_length_exceeded", 400, FailureStop, false, false},
		{"invalid_encrypted_content", 400, FailureStop, false, false},
		{"model_not_found", 400, FailureContinueAndCooldown, false, true},
		{"usage_limit_reached", 400, FailureContinueAndCooldown, true, false},
		{"invalid_api_key", 401, FailureContinueAndCooldown, false, false},
		{"server_error", 500, FailureContinueAndCooldown, false, false},
	} {
		err := &ProviderStatusError{StatusCode: tc.status}
		errorType := "invalid_request_error"
		if tc.status >= 500 {
			errorType = "server_error"
		}
		EnrichFailure(err, `{"error":{"type":"`+errorType+`","code":"`+tc.code+`"}}`)
		p := ClassifyFailure(err)
		if p.Action() != tc.action || p.Quota != tc.quota || p.ModelOnly != tc.model {
			t.Fatalf("%s: %+v", tc.code, p)
		}
	}
	if ClassifyFailure(context.Canceled).Action() != FailureStop {
		t.Fatal("canceled request is retryable")
	}
}
