package balancer

import (
	"sync"
	"sync/atomic"

	"LoadBalanceProvider/src/domain"
)

func (p *ProviderRuntime) runtimeState() *ProviderRuntime {
	if p.shared != nil {
		return p.shared
	}
	return p
}

func sameProviderIdentity(a, b *domain.LLMProviderConfig) bool {
	return a != nil && b != nil && a.ID == b.ID && a.BaseURL == b.BaseURL &&
		a.Kind == b.Kind && a.Type == b.Type && a.APIKey == b.APIKey && a.APIKeyEnv == b.APIKeyEnv
}

func (p *ProviderRuntime) tryStartRequest() bool {
	if !p.Config.AvailableNow() {
		return false
	}
	s := p.runtimeState()
	for {
		active := atomic.LoadInt64(&s.Active)
		if p.Config.MaxConcurrent > 0 && active >= p.Config.MaxConcurrent {
			return false
		}
		if atomic.CompareAndSwapInt64(&s.Active, active, active+1) {
			return true
		}
	}
}

// 可同時 defer 及提早釋放，避免 panic 或後續出口漏還名額。
func (p *ProviderRuntime) RequestRelease() func() {
	var once sync.Once
	return func() { once.Do(p.FinishRequest) }
}
