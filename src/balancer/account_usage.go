package balancer

import (
	"net/http"
	"time"
)

// 僅供完整且已驗證的帳號用量回應使用，允許移除上游已取消的額度窗口。
func (p *ProviderRuntime) RecordCodexAccountUsage(headers http.Header, maxAge time.Duration) bool {
	if p == nil || maxAge <= 0 {
		return false
	}
	return p.recordUsageSnapshot(buildProviderUsageSnapshot(providerUsageHeaders(headers)), maxAge)
}

// 查詢失敗時釋放主來源優先權；較晚完成的成功查詢不受影響。
func (p *ProviderRuntime) AccountUsageUnavailable(started time.Time) {
	if p == nil {
		return
	}
	p._usageLock.Lock()
	if p.accountUsageAt.After(started) {
		p._usageLock.Unlock()
		return
	}
	p.accountUsageUntil = time.Time{}
	fallback := cloneProviderUsageSnapshot(p.usageHeaderFallback)
	newer := fallback.UpdatedAt.After(p.Usage.UpdatedAt)
	p._usageLock.Unlock()
	if newer {
		p.recordUsageSnapshot(fallback, 0)
	}
}
