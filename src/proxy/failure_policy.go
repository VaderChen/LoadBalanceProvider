package proxy

import (
	"LoadBalanceProvider/src/providerdispatch"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type FailureDetails struct {
	Code         string
	Type         string
	RetryAfter   time.Duration
	QuotaResetAt time.Time
}

// HTTP 錯誤的本文保留在 deferred writer，讀完後再補入結構化分類資訊。
func EnrichFailure(err error, body string) {
	var status *ProviderStatusError
	if !errors.As(err, &status) {
		return
	}
	var payload map[string]interface{}
	if json.Unmarshal([]byte(body), &payload) != nil {
		return
	}
	if nested, ok := payload["error"].(map[string]interface{}); ok {
		payload = nested
	}
	status.Code = strings.ToLower(stringFromAny(payload["code"]))
	if status.Code == "" {
		status.Code = strings.ToLower(stringFromAny(payload["type"]))
	}
	if message := strings.TrimSpace(stringFromAny(payload["message"])); message != "" {
		status.Message = message
	}
	details := failureDetailsFromPayload(payload, time.Now())
	status.Type = details.Type
	status.QuotaResetAt = details.QuotaResetAt
	if details.RetryAfter > status.RetryAfter {
		status.RetryAfter = details.RetryAfter
	}
}

// SSE 本文可能不帶等待提示，仍須保留 HTTP 標頭要求的較長冷卻。
func retainStreamRetryAfter(err error, header http.Header) error {
	var stream *ProviderStreamError
	if errors.As(err, &stream) {
		if wait := retryAfterHeader(header); wait > stream.RetryAfter {
			stream.RetryAfter = wait
		}
	}
	return err
}

func failureDetailsFromEvent(event string) FailureDetails {
	var details FailureDetails
	for _, payload := range responseEventPayloads(event) {
		if response, ok := payload["response"].(map[string]interface{}); ok {
			payload = response
		}
		if nested, ok := payload["error"].(map[string]interface{}); ok {
			payload = nested
		}
		details = failureDetailsFromPayload(payload, time.Now())
	}
	return details
}

func failureDetailsFromPayload(payload map[string]interface{}, now time.Time) FailureDetails {
	d := FailureDetails{Code: strings.ToLower(stringFromAny(payload["code"])), Type: strings.ToLower(stringFromAny(payload["type"]))}
	if d.Code == "" {
		d.Code = strings.ToLower(stringFromAny(payload["type"]))
	}
	seconds := func(value interface{}) float64 {
		n, err := strconv.ParseFloat(strings.TrimSpace(stringFromAny(value)), 64)
		if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || n <= 0 {
			return 0
		}
		return n
	}
	if n := seconds(payload["retry_after"]); n > 0 && n <= 90*86400 {
		d.RetryAfter = time.Duration(n * float64(time.Second))
	}
	knownLimit := func(code string) bool {
		switch code {
		case "usage_limit_reached", "insufficient_quota", "quota_exceeded", "rate_limit_exceeded":
			return true
		}
		return false
	}
	if !knownLimit(d.Code) && !knownLimit(d.Type) {
		return d
	}
	// 配額可能按週／月恢復；拒絕異常遠的時間，避免永久隔離帳號。
	const maxQuotaWait = 90 * 24 * time.Hour
	if n := seconds(payload["resets_at"]); n > float64(now.Unix()) && n <= float64(now.Add(maxQuotaWait).Unix()) {
		d.QuotaResetAt = time.Unix(int64(n), 0)
	} else if n := seconds(payload["resets_in_seconds"]); n > 0 && n <= maxQuotaWait.Seconds() {
		d.QuotaResetAt = now.Add(time.Duration(n * float64(time.Second)))
	}
	if d.QuotaResetAt.After(now) && d.QuotaResetAt.Sub(now) > d.RetryAfter {
		d.RetryAfter = d.QuotaResetAt.Sub(now)
	}
	return d
}

type FailureAction string

const (
	FailureStop                FailureAction = "stop"
	FailureStopAndCooldown     FailureAction = "stop-and-cooldown"
	FailureContinue            FailureAction = "continue"
	FailureContinueAndCooldown FailureAction = "continue-and-cooldown"
)

func (p FailurePolicy) Action() FailureAction {
	if p.Request || p.Canceled {
		return FailureStop
	}
	if p.Capacity || p.RetryableServer || p.Auth {
		return FailureContinueAndCooldown
	}
	return FailureContinue
}

func retryAfterHeader(header http.Header) time.Duration {
	value := strings.TrimSpace(header.Get("Retry-After"))
	if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds > 0 && seconds <= 90*86400 {
		return time.Duration(seconds * float64(time.Second))
	}
	if deadline, err := http.ParseTime(value); err == nil {
		if wait := time.Until(deadline); wait > 0 && wait <= 90*24*time.Hour {
			return wait
		}
	}
	return 0
}

// Request 表示請求本身有誤，不重試也不懲罰帳號；ModelOnly 避免誤停其他模型。
type FailurePolicy struct {
	Canceled          bool
	Auth              bool
	Quota             bool
	Request           bool
	Capacity          bool
	TransientCapacity bool
	RetryableServer   bool
	ModelOnly         bool
	RetryAfter        time.Duration
	QuotaResetAt      time.Time
}

func ClassifyFailure(err error) FailurePolicy {
	if err == nil {
		return FailurePolicy{}
	}
	// 本機排隊逾時不是上游失敗，不懲罰帳號或換來源重試。
	if errors.Is(err, providerdispatch.ErrProtectionWaitExceeded) {
		return FailurePolicy{Request: true}
	}
	if errors.Is(err, context.Canceled) {
		return FailurePolicy{Canceled: true}
	}
	var stream *ProviderStreamError
	var status *ProviderStatusError
	var details FailureDetails
	code := 0
	if errors.As(err, &stream) {
		details = stream.FailureDetails
	}
	if errors.As(err, &status) {
		details = status.FailureDetails
		code = status.StatusCode
	}
	text := strings.ToLower(err.Error() + " " + details.Code + " " + details.Type)
	policy := FailurePolicy{RetryAfter: details.RetryAfter, QuotaResetAt: details.QuotaResetAt}
	for _, marker := range []string{"context_length_exceeded", "context_too_large", "maximum context length", "cyber_policy", "content_policy_violation", "invalid_encrypted_content", "previous_response_not_found"} {
		if strings.Contains(text, marker) {
			policy.Request = true
			return policy
		}
	}
	// 模型支援錯誤仍可換其他 Provider，但只隔離該模型。
	if code == 401 || code == 403 || details.Type == "authentication_error" || details.Code == "authentication_error" || details.Code == "invalid_api_key" || details.Code == "unauthorized" || details.Code == "permission_denied" {
		policy.Auth = true
		return policy
	}
	if strings.Contains(text, "model_not_found") || strings.Contains(text, "model not found") {
		policy.Capacity, policy.ModelOnly = true, true
		return policy
	}
	for _, marker := range []string{"usage_limit_reached", "insufficient_quota", "quota_exceeded", "rate_limit_exceeded"} {
		if strings.Contains(text, marker) {
			policy.Capacity = true
			policy.TransientCapacity = marker == "rate_limit_exceeded"
			policy.Quota = !policy.TransientCapacity
			return policy
		}
	}
	if code == 400 || code == 422 || strings.Contains(text, "invalid_request_error") {
		policy.Request = true
		return policy
	}
	policy.Capacity = IsRetryableCapacityError(err) || code == 429 || code == 503
	policy.TransientCapacity = IsRetryableCapacityError(err) || code == 429
	// 只接受上游 HTTP／串流錯誤，避免把本地傳輸錯誤或取消誤當成伺服器過載。
	policy.RetryableServer = code == 500 || code == 502 || code == 503 || code == 504 ||
		(status == nil && stream != nil && providerErrorTextIsRetryableUpstreamFailure(text))
	policy.ModelOnly = policy.Capacity && (strings.Contains(text, "overloaded") || strings.Contains(text, "at capacity") || strings.Contains(text, "model_capacity"))
	return policy
}
