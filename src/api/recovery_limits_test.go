package api

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestTurnRecoveryLimitSurvivesWindowAndProviderReset(t *testing.T) {
	var limits recoveryLimits
	limits.recordTurn("turn", "a", true, false, false)
	expires := limits.turns["turn"].expires
	for i := 0; i < turnRecoveryAttempts; i++ {
		if err := limits.checkTurn("turn", false, true); err != nil {
			t.Fatal(err)
		}
		limits.recordTurn("turn", "b", false, i < turnRecoverySwitches, true)
	}
	if limits.checkTurn("turn", false, true) == nil || limits.checkTurn("turn", true, false) == nil {
		t.Fatal("回合限制未生效")
	}
	if limits.checkTurn("turn", false, false) != nil || limits.checkTurn("new-turn", true, true) != nil {
		t.Fatal("限制阻擋正常新請求或其他回合")
	}
	if limits.turns["turn"].expires != expires || len(limits.failedProviders("turn")) != 1 {
		t.Fatal("已失敗來源消失或期限被延長")
	}
	e := &reconnectBudget{active: true, provider: "a", limit: 3, attempts: 3, recoveryUsed: true}
	if canContinueRecoveryProbe(e, newDeferredResponseWriter(httptest.NewRecorder(), true), false, time.Second) {
		t.Fatal("重連重新取得同連線恢復機會")
	}
	limits.turns["turn"].expires = time.Now().Add(-time.Second)
	if limits.checkTurn("turn", true, true) != nil {
		t.Fatal("到期未解除限制")
	}
}

func TestModelRecoverySingleProbeAndIsolation(t *testing.T) {
	var limits recoveryLimits
	for i := 0; i < 2; i++ {
		finish, err := limits.acquireModel("upstream/model", "a")
		if err != nil {
			t.Fatal(err)
		}
		finish(false, true)
	}
	if limits.models["upstream/model"].blocked {
		t.Fatal("單帳號故障不應觸發模型封鎖")
	}
	finish, err := limits.acquireModel("upstream/model", "b")
	if err != nil {
		t.Fatal(err)
	}
	finish(false, true)
	if _, err := limits.acquireModel("upstream/model", "c"); err == nil {
		t.Fatal("冷卻期間放行")
	}
	other, err := limits.acquireModel("other-upstream/model", "a")
	if err != nil {
		t.Fatal("影響不同上游")
	}
	other(true, false)
	limits.models["upstream/model"].until = time.Now().Add(-time.Second)
	probe, err := limits.acquireModel("upstream/model", "c")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := limits.acquireModel("upstream/model", "d"); err == nil {
		t.Fatal("同時放行多個探測")
	}
	probe(false, false)
	probe(false, true)
	if limits.models["upstream/model"].active {
		t.Fatal("取消未歸還探測名額")
	}
	probe, err = limits.acquireModel("upstream/model", "c")
	if err != nil {
		t.Fatal(err)
	}
	probe(true, false)
	if limits.models["upstream/model"].blocked {
		t.Fatal("探測成功未恢復")
	}
}
