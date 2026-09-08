package balancer

import (
	"crypto/sha256"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"LoadBalanceProvider/src/providerusage"
)

const AccountUsageRefreshInterval = 3 * time.Minute

func (p *ProviderRuntime) BeginAccountUsageProbe(now time.Time, force bool) bool {
	s := p.runtimeState()
	s._usageLock.Lock()
	defer s._usageLock.Unlock()
	if s.usageProbeRunning || (!force && now.Before(s.usageProbeNext)) {
		return false
	}
	s.usageProbeRunning = true
	atomic.StoreInt64(&s.LastUsageProbeAt, now.UnixNano())
	return true
}

func (p *ProviderRuntime) EndAccountUsageProbe(err error) {
	s := p.runtimeState()
	s._usageLock.Lock()
	defer s._usageLock.Unlock()
	s.usageProbeRunning = false
	now := time.Now()
	if err != nil {
		s.usageProbeError = "帳號用量查詢失敗"
		s.usageProbeNext = now.Add(time.Minute)
		return
	}
	s.usageProbeError = ""
	s.usageProbeSuccess = now
	s.usageProbeNext = now.Add(AccountUsageRefreshInterval)
}

func (p *ProviderRuntime) UsageProbeStatus() map[string]interface{} {
	s := p.runtimeState()
	s._usageLock.Lock()
	defer s._usageLock.Unlock()
	attempt := time.Time{}
	if value := atomic.LoadInt64(&s.LastUsageProbeAt); value > 0 {
		attempt = time.Unix(0, value)
	}
	return map[string]interface{}{
		"running": s.usageProbeRunning, "last_attempt": attempt, "last_success": s.usageProbeSuccess,
		"next_due": s.usageProbeNext, "error": s.usageProbeError,
		"freshness_seconds": int(p.UsageFreshness().Seconds()),
	}
}

// 統計只比較同帳號設定、同資料來源與同一額度窗口；不保存金鑰原文。
func (p *ProviderRuntime) UsageObservationContext(snapshot ProviderUsageSnapshot) providerusage.ObservationContext {
	window := ""
	remaining, _ := snapshot.KnownRemainingPercent()
	dimensions := snapshot.knownRemainingDimensions()
	reset := int64(0)
	for _, name := range []string{"primary", "secondary"} {
		prefix := "x-codex-" + name + "-"
		if value, known := dimensions[name]; known && math.Abs(value-remaining) < 0.0001 {
			window = name + ":" + snapshot.Headers[prefix+"window-minutes"]
			reset, _ = strconv.ParseInt(snapshot.Headers[prefix+"reset-at"], 10, 64)
			break
		}
	}
	if window == "" {
		window = "requests:" + snapshot.LimitRequests
		if value, known := dimensions["tokens"]; known && math.Abs(value-remaining) < 0.0001 {
			window = "tokens:" + snapshot.LimitTokens
		}
	}
	identity := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s", p.Config.ID, p.Config.Kind, p.Config.BaseURL, p.Config.APIKey, p.Config.APIKeyEnv, snapshot.AccountIdentity)
	account := fmt.Sprintf("%x", sha256.Sum256([]byte(identity)))
	return providerusage.ObservationContext{Series: account + ":" + snapshot.Source + ":" + window, ResetAt: reset, Source: snapshot.Source, Account: account}
}

// 僅供完整且已驗證的帳號用量回應使用，允許移除上游已取消的額度窗口。
func (p *ProviderRuntime) RecordCodexAccountUsage(headers http.Header, maxAge time.Duration, identity ...string) bool {
	if p == nil || maxAge <= 0 {
		return false
	}
	snapshot := buildProviderUsageSnapshot(providerUsageHeaders(headers))
	if len(identity) > 0 {
		snapshot.AccountIdentity = identity[0]
	}
	return p.recordUsageSnapshot(snapshot, maxAge)
}

// 查詢失敗時釋放主來源優先權；較晚完成的成功查詢不受影響。
func (p *ProviderRuntime) AccountUsageUnavailable(started time.Time) {
	if p == nil {
		return
	}
	p.runtimeState()._usageLock.Lock()
	if p.runtimeState().accountUsageAt.After(started) {
		p.runtimeState()._usageLock.Unlock()
		return
	}
	p.runtimeState().accountUsageUntil = time.Time{}
	fallback := cloneProviderUsageSnapshot(p.runtimeState().usageHeaderFallback)
	newer := fallback.UpdatedAt.After(p.runtimeState().Usage.UpdatedAt)
	p.runtimeState()._usageLock.Unlock()
	if newer {
		p.recordUsageSnapshot(fallback, 0)
	}
}
