package api

import (
	"fmt"
	"net/http"
	"time"

	"LoadBalanceProvider/src/proxy"
)

// 呼叫端持有該回合閘門，且前一個上游串流已關閉，才可替換來源。
func (h *HTTPAPI) rebindTurnBeforeDispatch(key string, r *http.Request, expected, provider, model string) error {
	if key == "" {
		return nil
	}
	owner := proxy.ResponseRouteOwner(r)
	turnBindingFileLock.Lock()
	defer turnBindingFileLock.Unlock()
	entries, err := h.readTurnBindings()
	if err != nil {
		return err
	}
	fingerprint := h.bindingFingerprint(provider)
	if fingerprint == "" {
		return fmt.Errorf("無法確認容量備援帳號")
	}
	previous := make(map[string]savedTurnBinding, len(entries))
	for k, v := range entries {
		previous[k] = v
	}
	for _, route := range []string{key, "recovered:" + key} {
		diskKey := turnBindingDiskKey(route, owner)
		if old, ok := entries[diskKey]; ok && old.Provider != expected {
			return turnError("容量備援來源已變更，未跨帳號送出")
		}
		entries[diskKey] = savedTurnBinding{Provider: provider, Model: model, Fingerprint: fingerprint, Until: time.Now().Add(30 * 24 * time.Hour)}
	}
	if err := h.writeTurnBindings(entries); err != nil {
		return err
	}
	target := proxy.ResponseRouteTarget{ProviderID: provider, Model: model, Owner: owner}
	if !h.Client.RebindTurnRoute(key, expected, owner, target) {
		if err := h.writeTurnBindings(previous); err != nil {
			return fmt.Errorf("容量備援綁定衝突且無法復原紀錄: %w", err)
		}
		return turnError("容量備援綁定衝突，未跨帳號送出")
	}
	// recovered 標記可能已由先前查詢載入；不可留著舊 Provider。
	h.Client.RecordPromptCacheRoute("recovered:"+key, provider, model, owner)
	return nil
}
