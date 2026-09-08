package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"LoadBalanceProvider/src/proxy"
	bolt "go.etcd.io/bbolt"
)

type failedFlushWriter struct {
	*httptest.ResponseRecorder
	failure error
}

func (w failedFlushWriter) FlushError() error { return w.failure }

func TestDownstreamFlushFailureIsStickyAndNotRetried(t *testing.T) {
	failure := errors.New("客戶端連線中斷")
	w := newDeferredResponseWriter(failedFlushWriter{httptest.NewRecorder(), failure}, true)
	_, err := w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n"))
	if !errors.Is(err, failure) || !errors.Is(w.Commit(), failure) {
		t.Fatal("Flush 失敗被當成成功交付")
	}
	if providerFailureCanRetryBeforeFirstToken(err, w) {
		t.Fatal("下游失敗不得重播上游")
	}
	if _, err = w.Write([]byte("later")); !errors.Is(err, failure) {
		t.Fatal("後續 Write 遺失原始錯誤")
	}
}

func TestRefundKeepsProbeLimitAndAdmissionWindowRecovers(t *testing.T) {
	var store reconnectBudgetStore
	e, rejection := store.acquire("capacity", 3)
	if rejection != nil {
		t.Fatal(rejection)
	}
	e.probeAttempts, e.rebindFrom, e.usedProviders = 3, "a", []string{"a", "b"}
	store.release("capacity", e, false)
	if _, rejection = store.acquire("capacity", 3); rejection == nil || rejection.code != "request_probe_throttled" {
		t.Fatal("退款遺失實際嘗試次數")
	}
	e.probeWindowAt = time.Now().Add(-reconnectRetryCooldown - time.Second)
	e, rejection = store.acquire("capacity", 3)
	if rejection != nil || e.attempts != 0 || e.probeAttempts != 0 || len(e.usedProviders) != 0 || e.rebindFrom != "a" {
		t.Fatal("容量探測未依期限恢復或遺失改綁資訊")
	}
	store.release("capacity", e, true)
	e, _ = store.acquire("queued", 3)
	e.admissionWaited = 30 * time.Second
	store.release("queued", e, false)
	e.probeWindowAt = time.Now().Add(-reconnectRetryCooldown - time.Second)
	e, rejection = store.acquire("queued", 3)
	if rejection != nil || e.admissionWaited != 0 {
		t.Fatal("未曾送出的請求被排隊額度永久封鎖")
	}
	store.release("queued", e, true)
}

func TestRouteDatabaseMigratesAndSeparatesRetention(t *testing.T) {
	h := capacityTestHandler(t)
	owner, route := "owner", "turn-binding:old"
	legacy := map[string]savedTurnBinding{
		turnBindingDiskKey(route, owner): {Provider: "a", Model: "smoke", Fingerprint: h.bindingFingerprint("a"), Until: time.Now().Add(24 * time.Hour)},
	}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(h.turnBindingPath(), raw, 0600); err != nil {
		t.Fatal(err)
	}
	target, ok, err := h.lookupDurableTurnRoute(route, owner)
	if err != nil || !ok || target.ProviderID != "a" {
		t.Fatalf("舊配對未匯入: %v %v", target, err)
	}
	target = proxy.ResponseRouteTarget{ProviderID: "a", Model: "smoke", Owner: owner}
	if err = h.saveTurnRoutes(map[string]proxy.ResponseRouteTarget{"turn-binding:new": target, "resp_new": target}); err != nil {
		t.Fatal(err)
	}
	if err = h.withTurnDB(func(db *bolt.DB) error {
		return db.View(func(tx *bolt.Tx) error {
			turn, turnOK, err := readRouteEntry(tx, routeBucket("turn-binding:new"), turnBindingDiskKey("turn-binding:new", owner))
			if err != nil {
				return err
			}
			response, responseOK, err := readRouteEntry(tx, routeBucket("resp_new"), turnBindingDiskKey("resp_new", owner))
			if err != nil {
				return err
			}
			if !turnOK || !responseOK || time.Until(turn.Until) < 29*24*time.Hour || time.Until(response.Until) > 7*24*time.Hour {
				return errors.New("回合與回應保存期限未分開")
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	h.Client = proxy.NewClient()
	if _, ok, err = h.lookupDurableTurnRoute("resp_new", owner); err != nil || !ok {
		t.Fatal("重新啟動後配對遺失")
	}
	conflict := proxy.ResponseRouteTarget{ProviderID: "b", Model: "smoke", Owner: owner}
	if err = h.saveTurnRoutes(map[string]proxy.ResponseRouteTarget{"turn-binding:new": conflict}); err == nil {
		t.Fatal("衝突配對被覆寫")
	}
	if _, err = os.Stat(h.turnBindingPath()); err != nil {
		t.Fatal("舊版備份未保留")
	}
}

var _ http.ResponseWriter = failedFlushWriter{}
