package proxy

import (
	"sync/atomic"
	"time"
)

// ClaimTurnRoute 在送出前原子綁定，並行請求不能將同一回合改綁其他來源。
func (c *Client) ClaimTurnRoute(key string, target ResponseRouteTarget) bool {
	target.CreatedAt = time.Now()
	actual, loaded := c.ResponseRoutes.LoadOrStore(key, target)
	if !loaded {
		atomic.AddInt64(&c.responseRouteCount, 1)
	}
	bound, ok := actual.(ResponseRouteTarget)
	if !ok || bound.ProviderID != target.ProviderID || bound.Model != target.Model || bound.Owner != target.Owner {
		return false
	}
	c.RecordPromptCacheRoute(key, target.ProviderID, target.Model, target.Owner)
	return true
}
