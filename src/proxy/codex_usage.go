package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"time"

	"LoadBalanceProvider/src/balancer"
)

type codexAccountUsageWindow struct {
	UsedPercent *float64 `json:"used_percent"`
	Seconds     *int64   `json:"limit_window_seconds"`
	ResetAfter  *int64   `json:"reset_after_seconds"`
	ResetAt     *int64   `json:"reset_at"`
}

func (c *Client) refreshOpenAICodexOAuthUsage(ctx context.Context, provider *balancer.ProviderRuntime) error {
	started := time.Now()
	provider.MarkUsageProbeAttempt(started)
	raw, err := c.requestCodexAccountAPI(ctx, provider.Config, http.MethodGet,
		codexAccountAPIURL(provider.Config, "usage"), nil, 45*time.Second)
	if err == nil {
		var headers http.Header
		headers, err = codexAccountUsageHeaders(raw)
		if err == nil && !provider.RecordCodexAccountUsage(headers, providerUsageStaleThreshold) {
			err = fmt.Errorf("帳號用量 API 未產生有效觀測")
		}
	}
	if err != nil {
		provider.AccountUsageUnavailable(started)
		return err
	}
	provider.ClearAuthError()
	log.Printf("provider usage refreshed: provider=%s source=account_api remaining=%.3f", providerIDForLog(provider), provider.UsageSnapshot().OverallRemainingPercent())
	return nil
}

func codexAccountUsageHeaders(raw []byte) (http.Header, error) {
	var payload struct {
		RateLimit *struct {
			Primary   json.RawMessage `json:"primary_window"`
			Secondary json.RawMessage `json:"secondary_window"`
		} `json:"rate_limit"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("解析帳號用量 API: %w", err)
	}
	if payload.RateLimit == nil || len(payload.RateLimit.Primary) == 0 || len(payload.RateLimit.Secondary) == 0 {
		return nil, fmt.Errorf("帳號用量 API 缺少完整額度窗口")
	}
	headers := make(http.Header)
	for name, rawWindow := range map[string]json.RawMessage{"primary": payload.RateLimit.Primary, "secondary": payload.RateLimit.Secondary} {
		var window *codexAccountUsageWindow
		if err := json.Unmarshal(rawWindow, &window); err != nil {
			return nil, fmt.Errorf("解析 %s 額度窗口: %w", name, err)
		}
		if window == nil {
			continue
		}
		if window.UsedPercent == nil || math.IsNaN(*window.UsedPercent) || math.IsInf(*window.UsedPercent, 0) || *window.UsedPercent < 0 || *window.UsedPercent > 100 {
			return nil, fmt.Errorf("%s 額度窗口缺少有效用量百分比", name)
		}
		prefix := "x-codex-" + name + "-"
		headers.Set(prefix+"used-percent", strconv.FormatFloat(*window.UsedPercent, 'f', -1, 64))
		if window.Seconds != nil && *window.Seconds > 0 {
			headers.Set(prefix+"window-minutes", strconv.FormatFloat(float64(*window.Seconds)/60, 'f', -1, 64))
		}
		if window.ResetAfter != nil && *window.ResetAfter >= 0 {
			headers.Set(prefix+"reset-after-seconds", strconv.FormatInt(*window.ResetAfter, 10))
		}
		if window.ResetAt != nil && *window.ResetAt > 0 {
			headers.Set(prefix+"reset-at", strconv.FormatInt(*window.ResetAt, 10))
		}
	}
	if len(headers) == 0 {
		return nil, fmt.Errorf("帳號用量 API 沒有可用的額度窗口")
	}
	return headers, nil
}
