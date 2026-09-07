package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/codexauth"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/providerusage"
	"LoadBalanceProvider/src/security"
)

const providerUsageRefreshInterval = 3 * time.Minute
const providerUsageStaleThreshold = 10 * time.Minute
const providerUsageRefreshConcurrency = 4

// -------------------------------------------------------------------------------------
func (_c *Client) StartProviderUsageRefresher(_ctx context.Context, _balancer *balancer.LoadBalancer) {
	if _ctx == nil {
		_ctx = context.Background()
	}
	if _c == nil || _balancer == nil {
		return
	}

	go func() {
		_c.refreshProviderUsage(_ctx, _balancer)
		_ticker := time.NewTicker(providerUsageRefreshInterval)
		defer _ticker.Stop()

		for {
			select {
			case <-_ctx.Done():
				return
			case <-_ticker.C:
				_c.refreshProviderUsage(_ctx, _balancer)
			}
		}
	}()
}

// -------------------------------------------------------------------------------------
func (_c *Client) refreshProviderUsage(_ctx context.Context, _balancer *balancer.LoadBalancer) {
	if !_c.usageRefreshRunning.CompareAndSwap(false, true) {
		return
	}
	defer _c.usageRefreshRunning.Store(false)

	_semaphore := make(chan struct{}, providerUsageRefreshConcurrency)
	var _waitGroup sync.WaitGroup
	for _, _provider := range _balancer.ProvidersSnapshot() {
		if !providerShouldRefreshUsage(_provider) {
			continue
		}
		select {
		case <-_ctx.Done():
			_waitGroup.Wait()
			return
		case _semaphore <- struct{}{}:
		}
		_waitGroup.Add(1)
		go func(_target *balancer.ProviderRuntime) {
			defer _waitGroup.Done()
			defer func() { <-_semaphore }()
			if _err := _c.RefreshProviderUsage(_ctx, _target); _err != nil {
				log.Printf("provider usage refresh failed: provider=%s error=%v", providerIDForLog(_target), _err)
			}
		}(_provider)
	}
	_waitGroup.Wait()
}

// -------------------------------------------------------------------------------------
func (_c *Client) RefreshProviderUsage(_ctx context.Context, _provider *balancer.ProviderRuntime) error {
	return _c.refreshProviderUsageForProvider(_ctx, _provider, false)
}

// -------------------------------------------------------------------------------------
func (_c *Client) refreshProviderUsageForce(_ctx context.Context, _provider *balancer.ProviderRuntime) error {
	return _c.refreshProviderUsageForProvider(_ctx, _provider, true)
}

// -------------------------------------------------------------------------------------
func (_c *Client) refreshProviderUsageForProvider(_ctx context.Context, _provider *balancer.ProviderRuntime, _force bool) error {
	if !providerShouldRefreshUsage(_provider) {
		return nil
	}
	if _ctx == nil {
		_ctx = context.Background()
	}
	_ctx, _cancel := context.WithTimeout(_ctx, 45*time.Second)
	defer _cancel()

	if isOpenAICodexProvider(_provider) && strings.TrimSpace(providerAPIKey(_provider)) == "" {
		if !_force && !_provider.ShouldProbeAccountUsage(time.Now(), providerUsageStaleThreshold) {
			return nil
		}
		return _c.refreshOpenAICodexOAuthUsage(_ctx, _provider)
	}
	if !_force && !_provider.ShouldProbeUsage(time.Now(), providerUsageStaleThreshold) {
		return nil
	}
	return _c.refreshGenericProviderUsageByMinimalRequest(_ctx, _provider)
}

// StartProviderUsageDailyAccounting captures quota boundaries at local 00:00 and 23:59.
// Live observations between those boundaries accumulate every positive quota drop. A
// same-day reset begins a new quota segment without erasing already consumed usage.
// -------------------------------------------------------------------------------------
func (_c *Client) StartProviderUsageDailyAccounting(_ctx context.Context, _balancer *balancer.LoadBalancer) {
	if _ctx == nil {
		_ctx = context.Background()
	}
	if _c == nil || _balancer == nil {
		return
	}

	go func() {
		_boundaryAt, _isStart := nextProviderUsageBoundary(time.Now())
		for {
			_wait := time.Until(_boundaryAt)
			if _wait < 0 {
				_wait = 0
			}
			_timer := time.NewTimer(_wait)
			select {
			case <-_ctx.Done():
				if !_timer.Stop() {
					select {
					case <-_timer.C:
					default:
					}
				}
				return
			case <-_timer.C:
			}

			_c.captureProviderUsageDayBoundary(_ctx, _balancer, _boundaryAt, _isStart)
			_boundaryAt, _isStart = followingProviderUsageBoundary(_boundaryAt, _isStart)
		}
	}()
}

// -------------------------------------------------------------------------------------
func (_c *Client) captureProviderUsageDayBoundary(_ctx context.Context, _balancer *balancer.LoadBalancer, _boundaryAt time.Time, _isStart bool) {
	_semaphore := make(chan struct{}, providerUsageRefreshConcurrency)
	var _waitGroup sync.WaitGroup
	for _, _provider := range _balancer.ProvidersSnapshot() {
		if !providerShouldRefreshUsage(_provider) {
			continue
		}
		select {
		case <-_ctx.Done():
			_waitGroup.Wait()
			return
		case _semaphore <- struct{}{}:
		}
		_waitGroup.Add(1)
		go func(_target *balancer.ProviderRuntime) {
			defer _waitGroup.Done()
			defer func() { <-_semaphore }()
			_probeStartedAt := time.Now()
			if _err := _c.refreshProviderUsageForce(_ctx, _target); _err != nil {
				log.Printf("provider daily usage boundary refresh failed: provider=%s start=%t error=%v", providerIDForLog(_target), _isStart, _err)
				return
			}

			_usage := _target.UsageSnapshot()
			if !_usage.HasUsageInfo() || _usage.UpdatedAt.Before(_probeStartedAt) {
				return
			}
			_remaining := _usage.OverallRemainingPercent()
			var _err error
			if _isStart {
				_err = providerusage.DefaultRecorder().RecordDayStart(providerIDForLog(_target), _remaining, _boundaryAt)
			} else {
				_err = providerusage.DefaultRecorder().RecordDayEnd(providerIDForLog(_target), _remaining, _boundaryAt)
			}
			if _err != nil {
				log.Printf("provider daily usage boundary record failed: provider=%s start=%t error=%v", providerIDForLog(_target), _isStart, _err)
			}
		}(_provider)
	}
	_waitGroup.Wait()
}

// -------------------------------------------------------------------------------------
func nextProviderUsageBoundary(_now time.Time) (time.Time, bool) {
	_local := _now.Local()
	_dayStart := time.Date(_local.Year(), _local.Month(), _local.Day(), 0, 0, 0, 0, _local.Location())
	_dayEnd := time.Date(_local.Year(), _local.Month(), _local.Day(), 23, 59, 0, 0, _local.Location())
	if _local.Before(_dayEnd) {
		return _dayEnd, false
	}
	return _dayStart.AddDate(0, 0, 1), true
}

// -------------------------------------------------------------------------------------
func followingProviderUsageBoundary(_boundaryAt time.Time, _wasStart bool) (time.Time, bool) {
	_local := _boundaryAt.Local()
	if _wasStart {
		return time.Date(_local.Year(), _local.Month(), _local.Day(), 23, 59, 0, 0, _local.Location()), false
	}
	_nextDay := _local.AddDate(0, 0, 1)
	return time.Date(_nextDay.Year(), _nextDay.Month(), _nextDay.Day(), 0, 0, 0, 0, _nextDay.Location()), true
}

// -------------------------------------------------------------------------------------
func (_c *Client) TestProviderMinimalChat(_ctx context.Context, _provider *balancer.ProviderRuntime) error {
	if !providerShouldRefreshUsage(_provider) {
		return fmt.Errorf("provider is not available for test")
	}
	if _ctx == nil {
		_ctx = context.Background()
	}
	_ctx, _cancel := context.WithTimeout(_ctx, 45*time.Second)
	defer _cancel()

	if isOpenAICodexProvider(_provider) && strings.TrimSpace(providerAPIKey(_provider)) == "" {
		return _c.testOpenAICodexMinimalChat(_ctx, _provider)
	}
	return _c.refreshGenericProviderUsageByMinimalRequest(_ctx, _provider)
}

// -------------------------------------------------------------------------------------
func providerShouldRefreshUsage(_provider *balancer.ProviderRuntime) bool {
	if _provider == nil || _provider.Config == nil {
		return false
	}
	if !_provider.Config.Enabled {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(_provider.Config.Role), "classifier") {
		return false
	}
	return true
}

// -------------------------------------------------------------------------------------
func (_c *Client) testOpenAICodexMinimalChat(_ctx context.Context, _provider *balancer.ProviderRuntime) error {
	_model := providerUsageProbeModelName(_provider.Config)
	if _model == "" {
		return fmt.Errorf("provider has no model for codex usage refresh")
	}

	_auth, _err := codexauth.Ensure(_provider.Config.ID)
	if _err != nil {
		_provider.MarkAuthError(_err.Error())
		return fmt.Errorf("openai codex oauth unavailable: %w", _err)
	}

	_payload := codexResponsesRequest{
		Model:        _model,
		Instructions: "Reply with only: ok",
		Input: []codexResponsesMessage{{
			Type:    "message",
			Role:    "user",
			Content: []codexResponsesContentPart{{Type: "input_text", Text: "ok"}},
		}},
		Stream: true,
		Store:  false,
	}
	_body, _err := json.Marshal(_payload)
	if _err != nil {
		return _err
	}

	_targetURL := codexResponsesURL(*_provider.Config, false)
	if _err := security.ValidateOutboundURL(_targetURL); _err != nil {
		return _err
	}
	_req, _err := http.NewRequestWithContext(_ctx, http.MethodPost, _targetURL, bytes.NewReader(_body))
	if _err != nil {
		return _err
	}
	_req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(_auth.AccessToken))
	_req.Header.Set("Content-Type", "application/json")
	_req.Header.Set("Accept", "text/event-stream")
	_req.Header.Set("OpenAI-Beta", "responses=experimental")
	_req.Header.Set("originator", defaultCodexUpstreamOriginator)
	_req.Header.Set("User-Agent", defaultCodexUpstreamUserAgent)
	if _accountID := strings.TrimSpace(_auth.AccountID); _accountID != "" {
		_req.Header.Set("chatgpt-account-id", _accountID)
	}

	_resp, _err := security.GuardedHTTPClient(usageRefreshHTTPClient(_c)).Do(_req)
	if _err != nil {
		return _err
	}
	defer _resp.Body.Close()

	_provider.RecordUsageHeaders(_resp.Header)
	_raw, _ := io.ReadAll(io.LimitReader(_resp.Body, 1024*1024))
	if usageProbeHasAuthError(_resp.StatusCode, _raw) {
		_message := usageProbeAuthErrorMessage(_resp.StatusCode, _raw)
		_provider.MarkAuthError(_message)
		return fmt.Errorf("%s", _message)
	}
	if _resp.StatusCode < http.StatusOK || _resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("codex usage refresh returned status %d: %s", _resp.StatusCode, strings.TrimSpace(string(_raw)))
	}

	_provider.ClearAuthError()
	return nil
}

// -------------------------------------------------------------------------------------
func (_c *Client) refreshGenericProviderUsageByMinimalRequest(_ctx context.Context, _provider *balancer.ProviderRuntime) error {
	_model := providerUsageProbeModelName(_provider.Config)
	if _model == "" {
		return fmt.Errorf("provider has no model for usage probe")
	}

	_now := time.Now()
	_provider.MarkUsageProbeAttempt(_now)

	_body, _targetURL, _stream, _err := minimalUsageProbeRequest(_provider.Config, _model)
	if _err != nil {
		return _err
	}
	if _err := security.ValidateOutboundURL(_targetURL); _err != nil {
		return _err
	}

	_req, _err := http.NewRequestWithContext(_ctx, http.MethodPost, _targetURL, bytes.NewReader(_body))
	if _err != nil {
		return _err
	}
	_req.Header.Set("Content-Type", "application/json")
	if _stream {
		_req.Header.Set("Accept", "text/event-stream")
	} else {
		_req.Header.Set("Accept", "application/json")
	}
	if _apiKey := providerAPIKey(_provider); _apiKey != "" {
		_req.Header.Set("Authorization", "Bearer "+_apiKey)
	}

	_resp, _err := security.GuardedHTTPClient(usageRefreshHTTPClient(_c)).Do(_req)
	if _err != nil {
		return _err
	}
	defer _resp.Body.Close()

	_provider.RecordUsageHeaders(_resp.Header)
	_raw, _ := io.ReadAll(io.LimitReader(_resp.Body, 1024*1024))
	if usageProbeHasAuthError(_resp.StatusCode, _raw) {
		_message := usageProbeAuthErrorMessage(_resp.StatusCode, _raw)
		_provider.MarkAuthError(_message)
		return fmt.Errorf("%s", _message)
	}
	if _resp.StatusCode < http.StatusOK || _resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("usage probe returned status %d: %s", _resp.StatusCode, strings.TrimSpace(string(_raw)))
	}
	_provider.ClearAuthError()
	return nil
}

// -------------------------------------------------------------------------------------
func usageProbeHasAuthError(_statusCode int, _body []byte) bool {
	if _statusCode == http.StatusUnauthorized || _statusCode == http.StatusForbidden {
		return true
	}
	_text := strings.ToLower(strings.TrimSpace(string(_body)))
	if _text == "" {
		return false
	}
	_authErrorPatterns := []string{
		"auth error",
		"authentication error",
		"unauthorized",
		"invalid token",
		"expired token",
		"token expired",
		"invalid access token",
		"missing bearer",
		"login required",
		"please login",
	}
	for _, _pattern := range _authErrorPatterns {
		if strings.Contains(_text, _pattern) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func usageProbeAuthErrorMessage(_statusCode int, _body []byte) string {
	_message := strings.TrimSpace(string(_body))
	if len(_message) > 500 {
		_message = _message[:500]
	}
	if _message == "" {
		_message = http.StatusText(_statusCode)
	}
	return fmt.Sprintf("provider auth error detected during usage probe: status %d: %s", _statusCode, _message)
}

// -------------------------------------------------------------------------------------
func minimalUsageProbeRequest(_provider *domain.LLMProviderConfig, _model string) ([]byte, string, bool, error) {
	if _provider == nil {
		return nil, "", false, fmt.Errorf("provider is nil")
	}
	_path := strings.ToLower(strings.TrimSpace(_provider.ChatCompletionsPath))
	if strings.Contains(_path, "responses") {
		_payload := map[string]interface{}{
			"model":             _model,
			"input":             "ok",
			"instructions":      "Reply with only: ok",
			"max_output_tokens": 1,
			"stream":            false,
			"store":             false,
		}
		_body, _err := json.Marshal(_payload)
		return _body, responsesURL(_provider), false, _err
	}

	_payload := map[string]interface{}{
		"model": _model,
		"messages": []map[string]string{{
			"role":    "user",
			"content": "ok",
		}},
		"max_tokens": 1,
		"stream":     false,
	}
	_body, _err := json.Marshal(_payload)
	return _body, _provider.ChatURL(), false, _err
}

// -------------------------------------------------------------------------------------
func usageRefreshHTTPClient(_client *Client) *http.Client {
	if _client != nil && _client.HTTPClient != nil {
		return _client.HTTPClient
	}
	return &http.Client{Timeout: 45 * time.Second}
}

// -------------------------------------------------------------------------------------
func firstProviderModelName(_provider *domain.LLMProviderConfig) string {
	if _provider == nil {
		return ""
	}
	for _, _model := range _provider.Models {
		if _name := strings.TrimSpace(_model.Name); _name != "" && !strings.EqualFold(_name, "auto") {
			return _name
		}
	}
	return ""
}

// -------------------------------------------------------------------------------------
func providerUsageProbeModelName(_provider *domain.LLMProviderConfig) string {
	_model := firstProviderModelName(_provider)
	if isOpenAICodexProviderConfig(_provider) {
		return codexUpstreamModelName(_model)
	}
	return _model
}

// -------------------------------------------------------------------------------------
func providerIDForLog(_provider *balancer.ProviderRuntime) string {
	if _provider == nil || _provider.Config == nil {
		return ""
	}
	return _provider.Config.ID
}
