package api

import (
	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/domain"
	"fmt"
	"strings"
)

type providerRetryBudget struct {
	round, maxRounds, maxSources int
	used                         []string
}

func (b *providerRetryBudget) nextRound() bool {
	if len(b.used) == 0 || b.round >= b.maxRounds {
		return false
	}
	b.round++
	b.used = nil
	return true
}

// 一輪內每個來源只嘗試一次；總嘗試次數仍由外層 RetryCount 限制。
func (h *HTTPAPI) selectRetryProvider(req *domain.ChatCompletionRequest, b *providerRetryBudget) (*balancer.ProviderRuntime, *domain.LLMModelConfig, domain.RequestProfile, balancer.SelectionMeta, error) {
	if strings.TrimSpace(req.ProviderID) != "" || strings.TrimSpace(req.Provider) != "" {
		return h.Balancer.Select(req)
	}
	if b.maxSources > 0 && len(b.used) >= b.maxSources {
		if !b.nextRound() {
			return nil, nil, domain.RequestProfile{}, balancer.SelectionMeta{}, fmt.Errorf("已達每輪來源數與重試輪數上限")
		}
	}
	p, m, profile, meta, err := h.Balancer.SelectExcluding(req, b.used)
	if err != nil && b.nextRound() {
		return h.Balancer.SelectExcluding(req, b.used)
	}
	return p, m, profile, meta, err
}
