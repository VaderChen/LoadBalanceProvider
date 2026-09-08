package api

import (
	"testing"
	"time"
)

func TestRepeatedReconnectRecoverySmoke(t *testing.T) {
	var store reconnectBudgetStore
	for cycle := 0; cycle < 100; cycle++ {
		entry, rejected := store.acquire("same-request", 3)
		if rejected != nil || entry.attempts != 0 || entry.probeAttempts != 0 {
			t.Fatalf("前次成功留下重連額度: cycle=%d rejection=%+v", cycle, rejected)
		}
		entry.provider, entry.model = "original", "model"
		entry.probeAttempts = 3
		store.release("same-request", entry, false)
		deadline := entry.probeWindowAt.Add(reconnectRetryCooldown)
		for retry := 0; retry < 5; retry++ {
			if _, rejected = store.acquire("same-request", 3); rejected == nil || rejected.code != "request_probe_throttled" || rejected.retryAfter <= 0 {
				t.Fatalf("容量退款繞過實際探測限制: %+v", rejected)
			}
			if entry.probeWindowAt.Add(reconnectRetryCooldown) != deadline {
				t.Fatal("被拒絕的重連延長了恢復期限")
			}
		}
		// 直接推進追蹤窗口，驗證到期狀態，不必在測試中等待真實的 30 秒。
		entry.probeWindowAt = time.Now().Add(-reconnectRetryCooldown - time.Second)
		probe, rejected := store.acquire("same-request", 3)
		if rejected != nil || probe.provider != "original" || probe.attempts != 0 || probe.probeAttempts != 0 {
			t.Fatalf("冷卻後未恢復或改變綁定: %+v", rejected)
		}
		probe.attempts, probe.probeAttempts = 1, 1
		store.release("same-request", probe, true)
		if len(store.entries) != 0 {
			t.Fatal("成功後沒有回收重連紀錄")
		}
	}
}
