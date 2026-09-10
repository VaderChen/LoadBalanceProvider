package api

import (
	"context"
	"time"
)

func canContinueRecoveryProbe(entry *reconnectBudget, writer *deferredResponseWriter, used bool, remaining time.Duration) bool {
	return entry != nil && entry.active && entry.provider != "" && entry.rebindFrom == "" &&
		entry.attempts >= entry.limit && !entry.delivered && !writer.ContentWritten() && !used && !entry.recoveryUsed && remaining > 0
}

// 只有已通過配對保存的容量改綁才重設工作次數，探測與等待紀錄不變。
func resetReboundProviderAttempts(entry *reconnectBudget, provider string) bool {
	if entry == nil || entry.rebindFrom == "" || entry.rebindFrom == provider {
		return false
	}
	entry.attempts = 0
	return true
}

// 沿用跨重連追蹤器發放下一窗口的一次探測，不另外補發額度。
// 呼叫端仍持有回合閘門與相同請求防重入保護，但不能持有上游名額。
func (h *HTTPAPI) continueRecoveryProbe(ctx context.Context, key string, entry *reconnectBudget, writer *deferredResponseWriter, heartbeat []byte, budget time.Duration, trace string) (*reconnectBudget, time.Duration, error) {
	started := time.Now()
	entry.recoveryUsed = true
	h.reconnectBudgets.release(key, entry, false)
	next, _, err := h.acquireReconnectBudget(ctx, key, entry.limit, writer, heartbeat, budget, trace)
	return next, time.Since(started), err
}
