package api

import (
	"context"
	"log"
	"time"
)

// 完成事件不是重試指令；在交付內容前，由代理等待追蹤器重新允許探測。
// 等待有固定期限，不增加額度、不延長冷卻，也不持有追蹤器或 Provider 的鎖。
func (h *HTTPAPI) acquireReconnectBudget(ctx context.Context, key string, limit int, writer *deferredResponseWriter, heartbeat []byte, budget time.Duration, trace string) (*reconnectBudget, *reconnectRejection, error) {
	deadline := time.Now().Add(budget)
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		entry, rejection := h.reconnectBudgets.acquire(key, limit)
		if rejection == nil || !rejection.canWaitForRecovery() || writer.ContentWritten() {
			return entry, rejection, nil
		}
		remaining := time.Until(deadline)
		if budget <= 0 || remaining <= 0 {
			return nil, rejection, nil
		}
		wait := min(time.Until(rejection.retryAt), remaining)
		log.Printf("provider replay cooldown wait: trace=%s reason=%s wait=%s", trace, rejection.code, wait.Round(time.Millisecond))
		if _, err := waitWithStreamHeartbeat(ctx, writer, heartbeat, wait); err != nil {
			return nil, nil, err
		}
	}
}
