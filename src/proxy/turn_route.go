package proxy

import (
	"sync/atomic"
	"time"
)

// ClaimTurnRoute 在送出前原子綁定，並行請求不能將同一回合改綁其他來源。
func (c *Client) ClaimTurnRoute(key string, target ResponseRouteTarget) bool {
	target.CreatedAt = time.Now()
	c.responseRouteMutationLock.Lock()
	actual, loaded := c.ResponseRoutes.LoadOrStore(key, target)
	if !loaded {
		atomic.AddInt64(&c.responseRouteCount, 1)
	}
	bound, ok := actual.(ResponseRouteTarget)
	if !ok || bound.ProviderID != target.ProviderID || bound.Model != target.Model || bound.Owner != target.Owner {
		c.responseRouteMutationLock.Unlock()
		return false
	}
	c.ResponseRoutes.Store(key, target)
	c.responseRouteMutationLock.Unlock()
	if atomic.LoadInt64(&c.responseRouteCount) > int64(c.responseRouteMaxEntriesValue()) {
		c.pruneResponseRoutes(time.Now(), true)
	}
	return true
}

// 原子替換而非先刪再建，避免並行請求趁空窗綁到第三個來源；總數不變。
func (c *Client) RebindTurnRoute(key, expectedProvider, owner string, target ResponseRouteTarget) bool {
	c.responseRouteMutationLock.Lock()
	defer c.responseRouteMutationLock.Unlock()
	actual, ok := c.ResponseRoutes.Load(key)
	bound, valid := actual.(ResponseRouteTarget)
	if !ok || !valid || bound.ProviderID != expectedProvider || bound.Owner != owner || target.Owner != owner {
		return false
	}
	target.CreatedAt = time.Now()
	c.ResponseRoutes.Store(key, target)
	return true
}
