package api

import (
	"context"
	"errors"
	"time"

	"LoadBalanceProvider/src/balancer"
)

func cooldownWaitReason(ctx context.Context, err error, budget time.Duration, ready bool) string {
	if ctx.Err() != nil {
		return "canceled"
	}
	if ready {
		return "ready"
	}
	var unavailable *balancer.NoAvailableProviderError
	if !errors.As(err, &unavailable) || !unavailable.TemporaryOverload || unavailable.RetryAfter <= 0 {
		return "not_temporarily_available"
	}
	if budget <= 0 {
		return "wait_budget_exhausted"
	}
	if unavailable.RetryAfter > budget {
		return "cooldown_wait_budget_exhausted"
	}
	return "downstream_write_failed"
}

// 等待消耗獨立的累計預算，不因換 Provider 重設，也不計作一次實際上游請求。
func waitForProviderCooldown(ctx context.Context, writer *deferredResponseWriter, heartbeat []byte, selectionErr error, budget time.Duration) (time.Duration, bool) {
	elapsed, ready, _ := waitForProviderCooldownResult(ctx, writer, heartbeat, selectionErr, budget)
	return elapsed, ready
}

func waitForProviderCooldownResult(ctx context.Context, writer *deferredResponseWriter, heartbeat []byte, selectionErr error, budget time.Duration) (time.Duration, bool, error) {
	if ctx.Err() != nil {
		return 0, false, ctx.Err()
	}
	var unavailable *balancer.NoAvailableProviderError
	if !errors.As(selectionErr, &unavailable) || !unavailable.TemporaryOverload || unavailable.RetryAfter <= 0 || budget <= 0 {
		return 0, false, nil
	}
	// 只等本次可用額度；不能把較長的上游冷卻縮短成可派送時間。
	wait := min(unavailable.RetryAfter, budget)
	elapsed, err := waitWithStreamHeartbeat(ctx, writer, heartbeat, wait)
	return elapsed, err == nil && elapsed >= unavailable.RetryAfter, err
}

// 只維持下游連線，不保留 Provider 名額；取消或寫入失敗立即停止等待。
func waitWithStreamHeartbeat(ctx context.Context, writer *deferredResponseWriter, heartbeat []byte, wait time.Duration) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if wait <= 0 {
		return 0, nil
	}
	started := time.Now()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	if err := writer.WriteStreamHeartbeat(heartbeat); err != nil {
		return time.Since(started), err
	}
	ticker := time.NewTicker(providerRetryKeepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return time.Since(started), ctx.Err()
		case <-timer.C:
			return time.Since(started), ctx.Err()
		case <-ticker.C:
			if err := writer.WriteStreamHeartbeat(heartbeat); err != nil {
				return time.Since(started), err
			}
		}
	}
}
