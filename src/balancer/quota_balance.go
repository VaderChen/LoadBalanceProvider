package balancer

import (
	"fmt"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"LoadBalanceProvider/src/domain"
)

const AccountUsageFreshness = 6 * time.Minute

func requestPinsProvider(req *domain.ChatCompletionRequest) bool {
	for _, value := range []string{req.ProviderID, req.Provider} {
		if value = strings.TrimSpace(value); value != "" && !strings.EqualFold(value, "auto") {
			return true
		}
	}
	return false
}

// 僅平衡策略已選定的同模型、同用途與同政策等級來源，不修改既有回合配對。
// 呼叫端持有 admissionLock；相同負載時使用最近獲配順序，避免連續新回合集中。
func (b *LoadBalancer) balanceNewTurn(candidates []ProviderSelection, selected ProviderSelection, meta SelectionMeta) (ProviderSelection, SelectionMeta) {
	peers := make([]ProviderSelection, 0, len(candidates))
	seen := make(map[string]bool)
	for _, candidate := range candidates {
		left, right := candidate.Provider.Config, selected.Provider.Config
		if seen[left.ID] || !sameProviderQuotaFamily(left, right) ||
			!strings.EqualFold(candidate.Model.Name, selected.Model.Name) ||
			!strings.EqualFold(left.Purpose, right.Purpose) || left.Weight != right.Weight ||
			candidate.Model.QualityTier != selected.Model.QualityTier || candidate.Model.CostTier != selected.Model.CostTier {
			continue
		}
		seen[left.ID] = true
		peers = append(peers, candidate)
	}
	if len(peers) < 2 {
		return selected, meta
	}
	// 不同額度窗口不能直接比較；缺資料的來源使用同儕平均，不虛構為 0% 或 100%。
	now := time.Now()
	usage := make(map[string]float64)
	var total float64
	schema := ""
	comparable := true
	for _, peer := range peers {
		snapshot := peer.Provider.UsageSnapshot()
		remaining, known := snapshot.KnownRemainingPercent()
		if !known || !peer.Provider.UsageFresh(snapshot, now) {
			continue
		}
		key := quotaWindowSchema(snapshot)
		if schema != "" && key != schema {
			comparable = false
		}
		schema = key
		usage[peer.Provider.Config.ID] = remaining
		total += remaining
	}
	mean := 0.0
	if comparable && len(usage) >= 2 {
		mean = total / float64(len(usage))
	} else {
		usage = nil
	}
	b._bindingLock.RLock()
	defer b._bindingLock.RUnlock()
	best := math.Inf(1)
	for _, peer := range peers {
		state := peer.Provider.runtimeState()
		capacity := float64(max(int64(1), peer.Provider.Config.MaxConcurrent))
		load := float64(atomic.LoadInt64(&state.Active)) / capacity
		bindings := float64(b._bindings[peer.Provider.Config.ID]) / capacity
		remaining := mean
		if value, known := usage[peer.Provider.Config.ID]; known {
			remaining = value
		}
		cost := mean - remaining + load*35 + load*load*70 + bindings*15
		last := selected.Provider.runtimeState().lastAdmission
		tiePreferred := state.lastAdmission < last || (state.lastAdmission == last && peer.Provider.Config.ID == meta.SelectedProviderID)
		if cost < best-0.0001 || (math.Abs(cost-best) <= 0.0001 && tiePreferred) {
			selected, best = peer, cost
		}
	}
	meta.SelectedProviderID = selected.Provider.Config.ID
	meta.SelectedProvider = selected.Provider.Config.Name
	meta.SelectedModel = selected.Model.Name
	quota := "unknown"
	if value, known := usage[selected.Provider.Config.ID]; known {
		quota = fmt.Sprintf("%.2f", value)
	}
	meta.Reason += fmt.Sprintf("; new-turn balance: peers=%d fresh_quotas=%d remaining=%s active=%d bindings=%d cost=%.2f",
		len(peers), len(usage), quota, selected.Provider.ActiveCount(), b._bindings[selected.Provider.Config.ID], best)
	return selected, meta
}

func (p *ProviderRuntime) UsageFreshness() time.Duration {
	if p == nil || p.Config == nil {
		return 12 * time.Minute
	}
	key := p.Config.APIKey
	if key == "" && p.Config.APIKeyEnv != "" {
		key = os.Getenv(p.Config.APIKeyEnv)
	}
	if strings.TrimSpace(key) == "" && (strings.EqualFold(p.Config.Kind, "openai-codex") || strings.EqualFold(p.Config.Type, "openai-codex")) {
		return AccountUsageFreshness
	}
	return 12 * time.Minute
}

func (p *ProviderRuntime) UsageFresh(snapshot ProviderUsageSnapshot, now time.Time) bool {
	return snapshot.HasUsageInfo() && !snapshot.UpdatedAt.After(now) && now.Sub(snapshot.UpdatedAt) <= p.UsageFreshness()
}

func quotaWindowSchema(snapshot ProviderUsageSnapshot) string {
	dimensions := snapshot.knownRemainingDimensions()
	_, primary := dimensions["primary"]
	_, secondary := dimensions["secondary"]
	return fmt.Sprintf("requests=%s;tokens=%s;primary=%t:%s;secondary=%t:%s",
		snapshot.LimitRequests, snapshot.LimitTokens, primary, snapshot.Headers["x-codex-primary-window-minutes"],
		secondary, snapshot.Headers["x-codex-secondary-window-minutes"])
}
