package proxy

import (
	"encoding/json"
	"testing"

	"LoadBalanceProvider/src/domain"
)

func TestCodexAccountAPIURL(t *testing.T) {
	_tests := []struct {
		name     string
		baseURL  string
		resource string
		want     string
	}{
		{
			name:     "ChatGPT host uses WHAM paths",
			baseURL:  "https://chatgpt.com",
			resource: "rate-limit-reset-credits",
			want:     "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits",
		},
		{
			name:     "ChatGPT backend path is not duplicated",
			baseURL:  "https://chatgpt.com/backend-api/",
			resource: "/usage",
			want:     "https://chatgpt.com/backend-api/wham/usage",
		},
		{
			name:     "Codex API compatible host uses Codex API paths",
			baseURL:  "https://codex.example.com",
			resource: "rate-limit-reset-credits/consume",
			want:     "https://codex.example.com/api/codex/rate-limit-reset-credits/consume",
		},
	}

	for _, _test := range _tests {
		t.Run(_test.name, func(t *testing.T) {
			_provider := &domain.LLMProviderConfig{BaseURL: _test.baseURL}
			if _got := codexAccountAPIURL(_provider, _test.resource); _got != _test.want {
				t.Fatalf("codexAccountAPIURL() = %q, want %q", _got, _test.want)
			}
		})
	}
}

func TestCodexAccountAPIErrorMessage(t *testing.T) {
	if _got := codexAccountAPIErrorMessage([]byte(`{"error":{"message":"reset unavailable"}}`)); _got != "reset unavailable" {
		t.Fatalf("error message = %q", _got)
	}
}

func TestEarliestExpiringCodexResetCreditID(t *testing.T) {
	_expiringSoon := "2026-09-21T00:05:00Z"
	_expiringLater := "2026-10-04T02:06:00Z"
	_invalidExpiry := "invalid"
	_credits := []codexRateLimitResetCreditPayload{
		{ID: "redeemed", Status: "redeemed", ExpiresAt: &_expiringSoon},
		{ID: "no-expiry", Status: "available"},
		{ID: "later", Status: "available", ExpiresAt: &_expiringLater},
		{ID: "invalid-expiry", Status: "available", ExpiresAt: &_invalidExpiry},
		{ID: " soon ", Status: "AVAILABLE", ExpiresAt: &_expiringSoon},
	}

	if _got := earliestExpiringCodexResetCreditID(_credits); _got != "soon" {
		t.Fatalf("earliest credit ID = %q, want %q", _got, "soon")
	}
}

func TestEarliestExpiringCodexResetCreditIDKeepsUnknownExpiryLast(t *testing.T) {
	_expiry := "2026-10-04T02:06:00Z"
	_credits := []codexRateLimitResetCreditPayload{
		{ID: "no-expiry", Status: "available"},
		{ID: "dated", Status: "available", ExpiresAt: &_expiry},
	}

	if _got := earliestExpiringCodexResetCreditID(_credits); _got != "dated" {
		t.Fatalf("earliest credit ID = %q, want %q", _got, "dated")
	}
}

func TestCodexRateLimitResetConsumeRequestIncludesSelectedCredit(t *testing.T) {
	_body, _err := json.Marshal(codexRateLimitResetConsumeRequest{
		RedeemRequestID: "request-1",
		CreditID:        "credit-1",
	})
	if _err != nil {
		t.Fatal(_err)
	}
	if _got, _want := string(_body), `{"redeem_request_id":"request-1","credit_id":"credit-1"}`; _got != _want {
		t.Fatalf("request body = %s, want %s", _got, _want)
	}
}

func TestCodexRateLimitResetConsumeRequestOmitsCreditWithoutDetails(t *testing.T) {
	_body, _err := json.Marshal(codexRateLimitResetConsumeRequest{RedeemRequestID: "request-1"})
	if _err != nil {
		t.Fatal(_err)
	}
	if _got, _want := string(_body), `{"redeem_request_id":"request-1"}`; _got != _want {
		t.Fatalf("request body = %s, want %s", _got, _want)
	}
}

func TestCodexResetCreditDetailsListsAvailableCreditsByExpiry(t *testing.T) {
	_count := int64(3)
	_soon := "2030-09-21T00:05:00Z"
	_later := "2030-10-04T02:06:00Z"
	_payload := codexRateLimitResetCreditsPayload{
		AvailableCount: &_count,
		Credits: []codexRateLimitResetCreditPayload{
			{ID: "unknown", Status: "available"},
			{ID: "later", Status: "available", ExpiresAt: &_later},
			{ID: "used", Status: "redeemed", ExpiresAt: &_soon},
			{ID: " soon ", Status: "AVAILABLE", ExpiresAt: &_soon},
			{ID: " ", Status: "available", ExpiresAt: &_soon},
		},
	}
	_result := codexResetCreditDetails(_payload)
	if _result.AvailableCount != 3 || !_result.HasCreditDetails || len(_result.Credits) != 3 {
		t.Fatalf("unexpected credit details: %+v", _result)
	}
	for _i, _id := range []string{"soon", "later", "unknown"} {
		if _result.Credits[_i].ID != _id {
			t.Fatalf("credit %d = %q, want %q", _i, _result.Credits[_i].ID, _id)
		}
	}
	if _result.NextExpiresAt == nil || *_result.NextExpiresAt != _soon || _result.Credits[1].ExpiresAt == nil || *_result.Credits[1].ExpiresAt != _later {
		t.Fatal("credit expiry was not preserved")
	}
	if _payload.Credits[0].ID != "unknown" || _payload.Credits[3].ID != " soon " {
		t.Fatal("modified upstream credit list")
	}
}

func TestCodexResetCreditDetailsCountWithoutList(t *testing.T) {
	_count := int64(2)
	_result := codexResetCreditDetails(codexRateLimitResetCreditsPayload{AvailableCount: &_count})
	if _result.AvailableCount != 2 || _result.HasCreditDetails || _result.NextExpiresAt != nil || _result.Credits == nil || len(_result.Credits) != 0 {
		t.Fatalf("unexpected count-only fallback: %+v", _result)
	}
	_raw, _err := json.Marshal(_result.Credits)
	if _err != nil || string(_raw) != "[]" {
		t.Fatalf("expected empty JSON array, got %s (%v)", _raw, _err)
	}
}
