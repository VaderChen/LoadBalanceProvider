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
	bolt "go.etcd.io/bbolt"
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
	cached, cachedOK := h.Client.LookupResponseRouteForOwner(route, owner)
	turnBindingFileLock.Lock()
	defer turnBindingFileLock.Unlock()
	var entry savedTurnBinding
	var ok bool
	err := h.withTurnDB(func(db *bolt.DB) error {
		return db.View(func(tx *bolt.Tx) error {
			var err error
			entry, ok, err = readRouteEntry(tx, routeBucket(route), turnBindingDiskKey(route, owner))
			return err
		})
	})
	if err != nil {
		return proxy.ResponseRouteTarget{}, false, turnError("無法讀取持久化回合綁定，請檢查綁定檔")
	}
	if !ok {
		// 未曾持久化的舊版快照仍可接續；已保存但到期的配對不可被快取復活。
		if entry.Until.IsZero() && cachedOK && !cached.Persisted {
			return cached, true, nil
		}
		return proxy.ResponseRouteTarget{}, false, nil
	}
	if h.bindingFingerprint(entry.Provider) != entry.Fingerprint {
		return proxy.ResponseRouteTarget{}, false, turnError("原 Provider 的帳號已變更，不能接續既有回合")
	}
	// 持久化來源是依據，但有效的記憶體快照仍保留完整歷史供安全接續。
	if cachedOK && cached.ProviderID == entry.Provider && cached.Model == entry.Model {
		cached.Persisted = true
		h.Client.MarkResponseRoutePersisted(route, cached)
		return cached, true, nil
	}
	target := proxy.ResponseRouteTarget{ProviderID: entry.Provider, Model: entry.Model, Owner: owner, CreatedAt: time.Now(), Persisted: true}
	h.Client.RecordPromptCacheRoute(route, target.ProviderID, target.Model, owner)
	h.Client.MarkResponseRoutePersisted(route, target)
	return target, true, nil
}

func (h *HTTPAPI) saveTurnRoutes(routes map[string]proxy.ResponseRouteTarget) error {
	if len(routes) == 0 {
		return nil
	}
	turnBindingFileLock.Lock()
	defer turnBindingFileLock.Unlock()
	err := h.withTurnDB(func(db *bolt.DB) error {
		return db.Update(func(tx *bolt.Tx) error {
			if err := pruneTurnDB(tx); err != nil {
				return err
			}
			for route, target := range routes {
				fingerprint := h.bindingFingerprint(target.ProviderID)
				if fingerprint == "" {
					return fmt.Errorf("無法確認原 Provider 帳號身分")
				}
				bucket, key := routeBucket(route), turnBindingDiskKey(route, target.Owner)
				old, ok, err := readRouteEntry(tx, bucket, key)
				if err != nil {
					return err
				}
				if ok && (old.Provider != target.ProviderID || old.Model != target.Model || old.Fingerprint != fingerprint) {
					return fmt.Errorf("持久化來源綁定衝突")
				}
				ttl := 30 * 24 * time.Hour
				if string(bucket) == "responses" {
					ttl = 7 * 24 * time.Hour
				}
				if ok && time.Until(old.Until) > ttl-time.Hour {
					continue
				}
				if err = putRouteEntry(tx, bucket, key, savedTurnBinding{target.ProviderID, target.Model, fingerprint, time.Now().Add(ttl)}); err != nil {
					return err
				}
			}
			return nil
		})
	})
	if err == nil {
		for route, target := range routes {
			h.Client.MarkResponseRoutePersisted(route, target)
		}
	}
	return err
}
