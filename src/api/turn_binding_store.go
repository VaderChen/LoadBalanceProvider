package api

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"LoadBalanceProvider/src/proxy"
)

// 僅持久化來源與帳號指紋；不保存訊息、工具參數、token 或原始對話識別。
type savedTurnBinding struct {
	Provider    string    `json:"provider"`
	Model       string    `json:"model"`
	Fingerprint string    `json:"fingerprint"`
	Until       time.Time `json:"until"`
}

var turnBindingFileLock sync.Mutex

func turnBindingDiskKey(route, owner string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(owner+"\x00"+route)))
}

func (h *HTTPAPI) turnBindingPath() string {
	return filepath.Join(filepath.Dir(h.advancedSettingsConfigPath()), "turn_provider_bindings.json")
}

func (h *HTTPAPI) readTurnBindings() (map[string]savedTurnBinding, error) {
	entries := make(map[string]savedTurnBinding)
	f, err := os.Open(h.turnBindingPath())
	if errors.Is(err, os.ErrNotExist) {
		return entries, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 4*1024*1024+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 4*1024*1024 {
		return nil, fmt.Errorf("回合綁定檔超過大小限制")
	}
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	if entries == nil {
		entries = make(map[string]savedTurnBinding)
	}
	for key, entry := range entries {
		if len(key) != 64 || len(entry.Fingerprint) != 64 || !entry.Until.After(time.Now()) {
			delete(entries, key)
		}
	}
	return entries, nil
}

func (h *HTTPAPI) bindingFingerprint(provider string) string {
	if h.Balancer == nil {
		return ""
	}
	for _, p := range h.Balancer.ProvidersSnapshot() {
		if p != nil && p.Config != nil && p.Config.ID == provider {
			return quotaFingerprint(p)
		}
	}
	return ""
}

func (h *HTTPAPI) lookupDurableTurnRoute(route, owner string) (proxy.ResponseRouteTarget, bool, error) {
	if target, ok := h.Client.LookupResponseRouteForOwner(route, owner); ok {
		return target, true, nil
	}
	turnBindingFileLock.Lock()
	defer turnBindingFileLock.Unlock()
	entries, err := h.readTurnBindings()
	if err != nil {
		return proxy.ResponseRouteTarget{}, false, turnError("無法讀取持久化回合綁定，請檢查綁定檔")
	}
	entry, ok := entries[turnBindingDiskKey(route, owner)]
	if !ok {
		return proxy.ResponseRouteTarget{}, false, nil
	}
	if h.bindingFingerprint(entry.Provider) != entry.Fingerprint {
		return proxy.ResponseRouteTarget{}, false, turnError("原 Provider 的帳號已變更，不能接續既有回合")
	}
	target := proxy.ResponseRouteTarget{ProviderID: entry.Provider, Model: entry.Model, Owner: owner, CreatedAt: time.Now()}
	h.Client.RecordPromptCacheRoute(route, target.ProviderID, target.Model, owner)
	return target, true, nil
}

func (h *HTTPAPI) saveTurnRoutes(routes map[string]proxy.ResponseRouteTarget) error {
	if len(routes) == 0 {
		return nil
	}
	turnBindingFileLock.Lock()
	defer turnBindingFileLock.Unlock()
	entries, err := h.readTurnBindings()
	if err != nil {
		return err
	}
	for route, target := range routes {
		fingerprint := h.bindingFingerprint(target.ProviderID)
		if fingerprint == "" {
			return fmt.Errorf("無法確認原 Provider 帳號身分")
		}
		key := turnBindingDiskKey(route, target.Owner)
		if old, ok := entries[key]; ok && (old.Provider != target.ProviderID || old.Model != target.Model || old.Fingerprint != fingerprint) {
			return fmt.Errorf("持久化來源綁定衝突")
		}
		entries[key] = savedTurnBinding{Provider: target.ProviderID, Model: target.Model, Fingerprint: fingerprint, Until: time.Now().Add(30 * 24 * time.Hour)}
	}
	return h.writeTurnBindings(entries)
}

// 呼叫端持有 turnBindingFileLock。
func (h *HTTPAPI) writeTurnBindings(entries map[string]savedTurnBinding) error {
	// 不淘汰仍有效的綁定來容納新回合，以免既有工具流程失去來源。
	if len(entries) > 10000 {
		return fmt.Errorf("持久化回合綁定已達容量上限")
	}
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	if len(data) > 4*1024*1024 {
		return fmt.Errorf("回合綁定檔超過大小限制")
	}
	path := h.turnBindingPath()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".turn-bindings-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(f.Name(), path)
}
