package balancer

import (
	"strings"
	"sync/atomic"
	"time"
)

func (p *ProviderRuntime) ModelUnavailableUntil(model string) time.Time {
	p.runtimeState().modelCooldownLock.Lock()
	defer p.runtimeState().modelCooldownLock.Unlock()
	return p.runtimeState().modelCooldowns[strings.ToLower(strings.TrimSpace(model))]
}

func (p *ProviderRuntime) CooldownUntil(model string) time.Time {
	until := p.ModelUnavailableUntil(model)
	if account := time.Unix(0, atomic.LoadInt64(&p.runtimeState().CapacityUnavailableUntil)); account.After(until) {
		until = account
	}
	if quota := time.Unix(0, atomic.LoadInt64(&p.runtimeState().QuotaUnavailableUntil)); quota.After(until) {
		until = quota
	}
	return until
}

func (p *ProviderRuntime) QuotaCooldownUntil() time.Time {
	return time.Unix(0, atomic.LoadInt64(&p.runtimeState().QuotaUnavailableUntil))
}

func (p *ProviderRuntime) RestoreQuotaCooldown(until time.Time) {
	for {
		old := atomic.LoadInt64(&p.runtimeState().QuotaUnavailableUntil)
		if old >= until.UnixNano() || atomic.CompareAndSwapInt64(&p.runtimeState().QuotaUnavailableUntil, old, until.UnixNano()) {
			return
		}
	}
}

func (p *ProviderRuntime) ClearQuotaCooldown() {
	atomic.StoreInt64(&p.runtimeState().QuotaUnavailableUntil, 0)
}

func (p *ProviderRuntime) MarkQuotaUnavailable(latency, duration time.Duration) {
	if duration <= 0 {
		duration = 30 * time.Second
	}
	p.RestoreQuotaCooldown(time.Now().Add(duration))
	atomic.AddInt64(&p.runtimeState().Failures, 1)
	p.recordLatency(latency)
}

func (p *ProviderRuntime) MarkModelUnavailable(model string, latency, duration time.Duration) {
	if duration <= 0 {
		duration = 30 * time.Second
	}
	now := time.Now()
	p.runtimeState().modelCooldownLock.Lock()
	if p.runtimeState().modelCooldowns == nil {
		p.runtimeState().modelCooldowns = make(map[string]time.Time)
	}
	// 只保留仍有效的項目，避免動態模型名稱讓快取無限成長。
	for key, until := range p.runtimeState().modelCooldowns {
		if !until.After(now) {
			delete(p.runtimeState().modelCooldowns, key)
		}
	}
	key := strings.ToLower(strings.TrimSpace(model))
	if until, exists := p.runtimeState().modelCooldowns[key]; now.Add(duration).After(until) {
		if exists || len(p.runtimeState().modelCooldowns) < 256 {
			p.runtimeState().modelCooldowns[key] = now.Add(duration)
		}
	}
	p.runtimeState().modelCooldownLock.Unlock()
	atomic.AddInt64(&p.runtimeState().Failures, 1)
	p.recordLatency(latency)
}
