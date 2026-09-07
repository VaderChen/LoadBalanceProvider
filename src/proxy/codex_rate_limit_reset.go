package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"LoadBalanceProvider/src/codexauth"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/security"
)

const (
	codexResetCreditReadTimeout    = 5 * time.Second
	codexResetCreditConsumeTimeout = 10 * time.Second
)

// CodexRateLimitResetCredits is the reset-credit availability exposed to the API layer.
type CodexRateLimitResetCredits struct {
	AvailableCount   int64
	NextExpiresAt    *string
	HasCreditDetails bool
	Credits          []CodexRateLimitResetCredit
}

type CodexRateLimitResetCredit struct {
	ID        string  `json:"id"`
	ExpiresAt *string `json:"expiresAt"`
}

// CodexRateLimitResetResult is the result of one idempotent reset-credit redemption.
type CodexRateLimitResetResult struct {
	Outcome      string `json:"outcome"`
	WindowsReset int64  `json:"windowsReset"`
}

type codexRateLimitResetCreditsPayload struct {
	AvailableCount *int64                             `json:"available_count"`
	Credits        []codexRateLimitResetCreditPayload `json:"credits"`
}

type codexRateLimitResetCreditPayload struct {
	ID        string  `json:"id"`
	Status    string  `json:"status"`
	ExpiresAt *string `json:"expires_at"`
}

type codexRateLimitResetUsagePayload struct {
	RateLimitResetCredits *codexRateLimitResetCreditsPayload `json:"rate_limit_reset_credits"`
}

type codexRateLimitResetConsumeRequest struct {
	RedeemRequestID string `json:"redeem_request_id"`
	CreditID        string `json:"credit_id,omitempty"`
}

type codexRateLimitResetConsumeResponse struct {
	Code         string `json:"code"`
	WindowsReset int64  `json:"windows_reset"`
}

// GetCodexRateLimitResetCredits reads the reset-credit count for one Codex OAuth Provider.
// The usage endpoint is a fallback because reset-credit detail rollout can be unavailable
// while the authoritative available count is already present in the usage response.
func (_c *Client) GetCodexRateLimitResetCredits(_ctx context.Context, _provider *domain.LLMProviderConfig) (CodexRateLimitResetCredits, error) {
	if !isOpenAICodexProviderConfig(_provider) {
		return CodexRateLimitResetCredits{}, fmt.Errorf("rate-limit reset credits require an OpenAI Codex provider")
	}

	_detailsURL := codexAccountAPIURL(_provider, "rate-limit-reset-credits")
	_raw, _detailsErr := _c.requestCodexAccountAPI(_ctx, _provider, http.MethodGet, _detailsURL, nil, codexResetCreditReadTimeout)
	if _detailsErr == nil {
		var _details codexRateLimitResetCreditsPayload
		if _err := json.Unmarshal(_raw, &_details); _err == nil && _details.AvailableCount != nil && *_details.AvailableCount >= 0 {
			return codexResetCreditDetails(_details), nil
		} else if _err != nil {
			_detailsErr = fmt.Errorf("decode Codex reset-credit response: %w", _err)
		} else {
			_detailsErr = fmt.Errorf("Codex reset-credit response does not contain a valid available count")
		}
	}

	_usageURL := codexAccountAPIURL(_provider, "usage")
	_raw, _usageErr := _c.requestCodexAccountAPI(_ctx, _provider, http.MethodGet, _usageURL, nil, codexResetCreditReadTimeout)
	if _usageErr == nil {
		var _usage codexRateLimitResetUsagePayload
		if _err := json.Unmarshal(_raw, &_usage); _err != nil {
			_usageErr = fmt.Errorf("decode Codex usage response: %w", _err)
		} else if _usage.RateLimitResetCredits == nil || _usage.RateLimitResetCredits.AvailableCount == nil || *_usage.RateLimitResetCredits.AvailableCount < 0 {
			_usageErr = fmt.Errorf("Codex usage response does not contain reset-credit availability")
		} else {
			return codexResetCreditDetails(*_usage.RateLimitResetCredits), nil
		}
	}

	return CodexRateLimitResetCredits{}, fmt.Errorf("read Codex reset credits: %v; usage fallback: %v", _detailsErr, _usageErr)
}

// ConsumeCodexRateLimitResetCredit redeems one available credit. Reusing the same
// idempotency key is safe when an earlier request has an uncertain transport outcome.
func (_c *Client) ConsumeCodexRateLimitResetCredit(_ctx context.Context, _provider *domain.LLMProviderConfig, _idempotencyKey, _creditID string) (CodexRateLimitResetResult, error) {
	if !isOpenAICodexProviderConfig(_provider) {
		return CodexRateLimitResetResult{}, fmt.Errorf("rate-limit reset requires an OpenAI Codex provider")
	}
	_idempotencyKey = strings.TrimSpace(_idempotencyKey)
	if _idempotencyKey == "" {
		return CodexRateLimitResetResult{}, fmt.Errorf("reset idempotency key is required")
	}

	// 指定券時交由上游驗證並處理冪等重試，不改選其他券。
	// 未指定券的舊呼叫端仍沿用最早到期優先的行為。
	_creditID = strings.TrimSpace(_creditID)
	if _creditID == "" {
		_detailsURL := codexAccountAPIURL(_provider, "rate-limit-reset-credits")
		if _raw, _detailsErr := _c.requestCodexAccountAPI(_ctx, _provider, http.MethodGet, _detailsURL, nil, codexResetCreditReadTimeout); _detailsErr == nil {
			var _details codexRateLimitResetCreditsPayload
			if json.Unmarshal(_raw, &_details) == nil {
				_creditID = earliestExpiringCodexResetCreditID(_details.Credits)
			}
		}
	}

	_body, _err := json.Marshal(codexRateLimitResetConsumeRequest{
		RedeemRequestID: _idempotencyKey,
		CreditID:        _creditID,
	})
	if _err != nil {
		return CodexRateLimitResetResult{}, _err
	}
	_targetURL := codexAccountAPIURL(_provider, "rate-limit-reset-credits/consume")
	_raw, _err := _c.requestCodexAccountAPI(_ctx, _provider, http.MethodPost, _targetURL, _body, codexResetCreditConsumeTimeout)
	if _err != nil {
		return CodexRateLimitResetResult{}, _err
	}

	var _response codexRateLimitResetConsumeResponse
	if _err := json.Unmarshal(_raw, &_response); _err != nil {
		return CodexRateLimitResetResult{}, fmt.Errorf("decode Codex reset response: %w", _err)
	}
	_response.Code = strings.TrimSpace(_response.Code)
	switch _response.Code {
	case "reset", "nothing_to_reset", "no_credit", "already_redeemed":
	default:
		return CodexRateLimitResetResult{}, fmt.Errorf("Codex reset response contains an unknown outcome %q", _response.Code)
	}
	return CodexRateLimitResetResult{Outcome: _response.Code, WindowsReset: _response.WindowsReset}, nil
}

func earliestExpiringCodexResetCreditID(_credits []codexRateLimitResetCreditPayload) string {
	_credit, _ok := earliestExpiringCodexResetCredit(_credits)
	if !_ok {
		return ""
	}
	return _credit.ID
}

func earliestExpiringCodexResetCredit(_credits []codexRateLimitResetCreditPayload) (codexRateLimitResetCreditPayload, bool) {
	_available := availableCodexResetCredits(_credits)
	if len(_available) == 0 {
		return codexRateLimitResetCreditPayload{}, false
	}
	return _available[0], true
}

func codexResetCreditDetails(_payload codexRateLimitResetCreditsPayload) CodexRateLimitResetCredits {
	_available := availableCodexResetCredits(_payload.Credits)
	_result := CodexRateLimitResetCredits{
		AvailableCount:   *_payload.AvailableCount,
		HasCreditDetails: len(_available) > 0,
		Credits:          make([]CodexRateLimitResetCredit, 0, len(_available)),
	}
	for _, _credit := range _available {
		_result.Credits = append(_result.Credits, CodexRateLimitResetCredit{ID: _credit.ID, ExpiresAt: _credit.ExpiresAt})
	}
	if len(_available) > 0 {
		_result.NextExpiresAt = _available[0].ExpiresAt
	}
	return _result
}

func availableCodexResetCredits(_credits []codexRateLimitResetCreditPayload) []codexRateLimitResetCreditPayload {
	_available := make([]codexRateLimitResetCreditPayload, 0, len(_credits))
	for _, _credit := range _credits {
		_credit.ID = strings.TrimSpace(_credit.ID)
		if _credit.ID == "" || !strings.EqualFold(strings.TrimSpace(_credit.Status), "available") {
			continue
		}
		_available = append(_available, _credit)
	}
	sort.SliceStable(_available, func(_i, _j int) bool {
		_iExpiresAt, _iOK := codexResetCreditExpiry(_available[_i].ExpiresAt)
		_jExpiresAt, _jOK := codexResetCreditExpiry(_available[_j].ExpiresAt)
		if _iOK != _jOK {
			return _iOK
		}
		if !_iOK {
			return false
		}
		return _iExpiresAt.Before(_jExpiresAt)
	})
	return _available
}

func codexResetCreditExpiry(_value *string) (time.Time, bool) {
	if _value == nil {
		return time.Time{}, false
	}
	_expiry, _err := time.Parse(time.RFC3339, strings.TrimSpace(*_value))
	return _expiry, _err == nil
}

func (_c *Client) requestCodexAccountAPI(_ctx context.Context, _provider *domain.LLMProviderConfig, _method string, _targetURL string, _body []byte, _timeout time.Duration) ([]byte, error) {
	_auth, _err := codexauth.Ensure(_provider.ID)
	if _err != nil {
		return nil, fmt.Errorf("openai codex oauth unavailable: %w", _err)
	}
	if strings.TrimSpace(_auth.AccessToken) == "" {
		return nil, fmt.Errorf("openai codex oauth access token is empty")
	}
	if _err := security.ValidateOutboundURL(_targetURL); _err != nil {
		return nil, _err
	}

	_requestCtx, _cancel := context.WithTimeout(_ctx, _timeout)
	defer _cancel()
	_req, _err := http.NewRequestWithContext(_requestCtx, _method, _targetURL, bytes.NewReader(_body))
	if _err != nil {
		return nil, _err
	}
	_req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(_auth.AccessToken))
	_req.Header.Set("Accept", "application/json")
	_req.Header.Set("User-Agent", defaultCodexUpstreamUserAgent)
	if _method == http.MethodPost {
		_req.Header.Set("Content-Type", "application/json")
	}
	if _accountID := strings.TrimSpace(_auth.AccountID); _accountID != "" {
		_req.Header.Set("ChatGPT-Account-Id", _accountID)
	}

	_resp, _err := security.GuardedHTTPClient(usageRefreshHTTPClient(_c)).Do(_req)
	if _err != nil {
		return nil, _err
	}
	defer _resp.Body.Close()
	_raw, _err := io.ReadAll(io.LimitReader(_resp.Body, 1024*1024))
	if _err != nil {
		return nil, _err
	}
	if _resp.StatusCode < http.StatusOK || _resp.StatusCode >= http.StatusMultipleChoices {
		_message := codexAccountAPIErrorMessage(_raw)
		if _message == "" {
			_message = strings.TrimSpace(string(_raw))
		}
		if _message == "" {
			_message = http.StatusText(_resp.StatusCode)
		}
		return nil, fmt.Errorf("Codex account API returned status %d: %s", _resp.StatusCode, _message)
	}
	return _raw, nil
}

func codexAccountAPIURL(_provider *domain.LLMProviderConfig, _resource string) string {
	_baseURL := ""
	if _provider != nil {
		_baseURL = strings.TrimRight(strings.TrimSpace(_provider.BaseURL), "/")
	}
	_lowerBaseURL := strings.ToLower(_baseURL)
	if _baseURL == "" || strings.Contains(_lowerBaseURL, "api.openai.com") {
		_baseURL = "https://chatgpt.com"
		_lowerBaseURL = strings.ToLower(_baseURL)
	}
	if (strings.HasPrefix(_lowerBaseURL, "https://chatgpt.com") || strings.HasPrefix(_lowerBaseURL, "https://chat.openai.com")) && !strings.Contains(_lowerBaseURL, "/backend-api") {
		_baseURL += "/backend-api"
		_lowerBaseURL += "/backend-api"
	}

	_resource = strings.Trim(strings.TrimSpace(_resource), "/")
	if strings.Contains(_lowerBaseURL, "/backend-api") {
		return _baseURL + "/wham/" + _resource
	}
	return _baseURL + "/api/codex/" + _resource
}

func codexAccountAPIErrorMessage(_raw []byte) string {
	var _payload struct {
		Detail  string `json:"detail"`
		Message string `json:"message"`
		Error   struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(_raw, &_payload) != nil {
		return ""
	}
	if _message := strings.TrimSpace(_payload.Error.Message); _message != "" {
		return _message
	}
	if _message := strings.TrimSpace(_payload.Detail); _message != "" {
		return _message
	}
	return strings.TrimSpace(_payload.Message)
}
