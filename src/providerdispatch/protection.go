package providerdispatch

import (
	"errors"
	"strings"
	"time"

	"LoadBalanceProvider/src/domain"
)

var ErrProtectionWaitExceeded = errors.New("全站流量保護等待額度已用盡，未送往上游")

// 僅保留計數需要的設定，不複製帳號密鑰；重載不清除在途連線計數。
func ConfigureProviders(providers []domain.LLMProviderConfig) {
	snapshot := make([]domain.LLMProviderConfig, 0, len(providers))
	for _, p := range providers {
		entry := domain.LLMProviderConfig{ID: p.ID, Enabled: p.Enabled, Role: p.Role, BaseURL: p.BaseURL}
		if p.Downtime != nil {
			downtime := *p.Downtime
			entry.Downtime = &downtime
		}
		snapshot = append(snapshot, entry)
	}
	providerDispatch.Lock()
	defer providerDispatch.Unlock()
	providerDispatch.providers = snapshot
}

func globalConcurrencyLimitLocked(now time.Time) int64 {
	accounts := int64(0)
	seen := make(map[string]bool)
	for _, p := range providerDispatch.providers {
		if !p.Enabled || p.InScheduledDowntime(now) || strings.EqualFold(p.Role, "classifier") || strings.TrimSpace(p.BaseURL) == "" {
			continue
		}
		key := keyFor(&p)
		if !seen[key] {
			accounts++
			seen[key] = true
		}
	}
	// 沒有啟用來源時仍保留直連診斷的有限額度。
	return max(accounts, 1) * 2
}

func Configure(settings domain.AdvancedSettingsConfig) {
	providerDispatch.Lock()
	defer providerDispatch.Unlock()
	providerDispatch.settings = settings
}

func keyFor(p *domain.LLMProviderConfig) string {
	if p.ID != "" {
		return p.ID
	}
	return p.BaseURL
}

// 記錄來源冷卻，包含排程器重新載入的配額冷卻。
func Cooldown(p *domain.LLMProviderConfig, until time.Time) {
	if p == nil || !until.After(time.Now()) {
		return
	}
	providerDispatch.Lock()
	defer providerDispatch.Unlock()
	key := keyFor(p)
	s := providerDispatch.states[key]
	if s == nil {
		s = &providerDispatchState{}
		providerDispatch.states[key] = s
	}
	if until.After(s.cooldownUntil) {
		s.cooldownUntil = until
	}
	s.recovering, s.probeStarted = true, false
}

// 只有冷卻後已放行探測的成功可恢復；冷卻前的在途成功不提前解封。
func Success(p *domain.LLMProviderConfig, started time.Time) {
	if p == nil {
		return
	}
	providerDispatch.Lock()
	defer providerDispatch.Unlock()
	s := providerDispatch.states[keyFor(p)]
	if s != nil && !started.Before(s.cooldownUntil) && (s.probeStarted || !providerDispatch.settings.CooldownSingleProbeEnabled) {
		s.recovering, s.probeStarted = false, false
	}
}
