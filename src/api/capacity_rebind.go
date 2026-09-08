package api

import (
	"fmt"
	"net/http"
	"time"

	"LoadBalanceProvider/src/proxy"
	bolt "go.etcd.io/bbolt"
)

// 呼叫端持有回合閘門，且舊上游已關閉。改綁在同一交易中更新兩個標記。
func (h *HTTPAPI) rebindTurnBeforeDispatch(key string, r *http.Request, expected, provider, model string) error {
	if key == "" {
		return nil
	}
	owner := proxy.ResponseRouteOwner(r)
	turnBindingFileLock.Lock()
	defer turnBindingFileLock.Unlock()
	fingerprint := h.bindingFingerprint(provider)
	if fingerprint == "" {
		return fmt.Errorf("無法確認容量備援帳號")
	}
	target := proxy.ResponseRouteTarget{ProviderID: provider, Model: model, Owner: owner, Persisted: true}
	var previous proxy.ResponseRouteTarget
	rebound := false
	err := h.withTurnDB(func(db *bolt.DB) error {
		return db.Update(func(tx *bolt.Tx) error {
			if err := pruneTurnDB(tx); err != nil {
				return err
			}
			for _, route := range []string{key, "recovered:" + key} {
				bucket, diskKey := routeBucket(route), turnBindingDiskKey(route, owner)
				old, ok, err := readRouteEntry(tx, bucket, diskKey)
				if err != nil {
					return err
				}
				if ok && old.Provider != expected {
					return turnError("容量備援來源已變更，未跨帳號送出")
				}
				if route == key {
					previous = proxy.ResponseRouteTarget{ProviderID: expected, Model: old.Model, Owner: owner, Persisted: ok}
				}
				if err = putRouteEntry(tx, bucket, diskKey, savedTurnBinding{provider, model, fingerprint, time.Now().Add(30 * 24 * time.Hour)}); err != nil {
					return err
				}
			}
			if prior, ok := h.Client.LookupResponseRouteForOwner(key, owner); ok {
				previous = prior
			}
			if !h.Client.RebindTurnRoute(key, expected, owner, target) {
				return turnError("容量備援綁定衝突，未跨帳號送出")
			}
			rebound = true
			return nil
		})
	})
	if err != nil {
		if rebound && !h.Client.RebindTurnRoute(key, provider, owner, previous) {
			return fmt.Errorf("容量備援儲存失敗且記憶體綁定無法復原: %w", err)
		}
		return err
	}
	h.Client.RecordPromptCacheRoute("recovered:"+key, provider, model, owner)
	h.Client.MarkResponseRoutePersisted("recovered:"+key, target)
	return nil
}
