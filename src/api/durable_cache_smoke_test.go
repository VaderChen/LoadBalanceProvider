package api

import (
	"net/http/httptest"
	"testing"
	"time"

	"LoadBalanceProvider/src/proxy"
	bolt "go.etcd.io/bbolt"
)

func TestCachedTurnAccountIdentitySmoke(t *testing.T) {
	h := durableBindingTestHandler(t)
	r := httptest.NewRequest("POST", "/v1/responses", nil)
	owner := proxy.ResponseRouteOwner(r)
	route := "turn-binding:cached-identity"
	if err := h.bindTurnBeforeDispatch(route, r, "a", "model"); err != nil {
		t.Fatal(err)
	}
	config := h.Balancer.ConfigSnapshot()
	config.Providers[0].APIKey = "replacement-account"
	h.Balancer.ReloadConfig(&config)
	if _, ok, err := h.lookupDurableTurnRoute(route, owner); err == nil || ok {
		t.Fatal("記憶體命中繞過了原帳號驗證")
	}
}

func TestCachedResponseExpiryAndSnapshotSmoke(t *testing.T) {
	h := durableBindingTestHandler(t)
	route, owner := "resp_cached_expiry", "owner"
	target := proxy.ResponseRouteTarget{ProviderID: "a", Model: "model", Owner: owner}
	h.Client.RecordResponseSnapshotForOwner(route, "a", "model", owner, []interface{}{"history"}, map[string]interface{}{"id": route})
	if err := h.saveTurnRoutes(map[string]proxy.ResponseRouteTarget{route: target}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := h.lookupDurableTurnRoute(route, owner)
	if err != nil || !ok || got.Input == nil || got.Response["id"] != route {
		t.Fatalf("有效快照不應因持久化驗證遺失: %+v %v", got, err)
	}
	if err := h.withTurnDB(func(db *bolt.DB) error {
		return db.Update(func(tx *bolt.Tx) error {
			return putRouteEntry(tx, routeBucket(route), turnBindingDiskKey(route, owner), savedTurnBinding{
				Provider: "a", Model: "model", Fingerprint: h.bindingFingerprint("a"), Until: time.Now().Add(-time.Second),
			})
		})
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := h.lookupDurableTurnRoute(route, owner); err != nil || ok {
		t.Fatalf("已到期的持久化配對被記憶體復活: %v", err)
	}
	if err := h.withTurnDB(func(db *bolt.DB) error {
		return db.Update(func(tx *bolt.Tx) error {
			if err := tx.Bucket([]byte("metadata")).Delete([]byte("pruned")); err != nil {
				return err
			}
			return pruneTurnDB(tx)
		})
	}); err != nil {
		t.Fatal(err)
	}
	// 同來源的快取更新也不能抹除「已持久化」標記。
	h.Client.RecordPromptCacheRoute(route, "a", "model", owner)
	if _, ok, err := h.lookupDurableTurnRoute(route, owner); err != nil || ok {
		t.Fatalf("資料庫清理後過期配對被當成舊版快照: %v", err)
	}
}
