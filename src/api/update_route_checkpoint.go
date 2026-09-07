package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"LoadBalanceProvider/src/proxy"
)

type updateRouteRecord struct {
	Route       string    `json:"route"`
	Provider    string    `json:"provider"`
	Model       string    `json:"model"`
	Owner       string    `json:"owner"`
	Fingerprint string    `json:"fingerprint"`
	CreatedAt   time.Time `json:"created_at"`
}

func (h *HTTPAPI) updateRouteCheckpointPath() string {
	return filepath.Join(filepath.Dir(h.advancedSettingsConfigPath()), "update_provider_routes.json")
}

// 更新快照不含 Response 的 Input/Response，只保存路由與驗證所需資料。
func (h *HTTPAPI) saveUpdateRouteCheckpoint() error {
	if h.Client == nil || h.Balancer == nil {
		return fmt.Errorf("路由服務尚未初始化")
	}
	h.restoreUpdateRouteCheckpoint()
	turnBindingFileLock.Lock()
	defer turnBindingFileLock.Unlock()
	records := make([]updateRouteRecord, 0)
	fingerprints := make(map[string]string)
	var snapshotErr error
	h.Client.ResponseRoutes.Range(func(key, value interface{}) bool {
		route, ok := key.(string)
		target, valid := value.(proxy.ResponseRouteTarget)
		if !ok || !valid {
			return true
		}
		fingerprint, cached := fingerprints[target.ProviderID]
		if !cached {
			fingerprint = h.bindingFingerprint(target.ProviderID)
			fingerprints[target.ProviderID] = fingerprint
		}
		if fingerprint == "" {
			snapshotErr = fmt.Errorf("無法確認 Provider %q 的帳號", target.ProviderID)
			return false
		}
		records = append(records, updateRouteRecord{route, target.ProviderID, target.Model, target.Owner, fingerprint, target.CreatedAt})
		if len(records) > 10000 {
			snapshotErr = fmt.Errorf("更新綁定快照超過筆數限制")
			return false
		}
		return true
	})
	if snapshotErr != nil {
		return snapshotErr
	}
	data, err := json.Marshal(records)
	if err != nil {
		return err
	}
	if len(data) > 4*1024*1024 {
		return fmt.Errorf("更新綁定快照超過大小限制")
	}
	path := h.updateRouteCheckpointPath()
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".update-routes-*")
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
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	log.Printf("system update route checkpoint saved: routes=%d", len(records))
	return nil
}

func (h *HTTPAPI) restoreUpdateRouteCheckpoint() {
	if h == nil || h.Client == nil || h.Balancer == nil {
		return
	}
	h.updateRouteRestore.Do(func() {
		turnBindingFileLock.Lock()
		defer turnBindingFileLock.Unlock()
		f, err := os.Open(h.updateRouteCheckpointPath())
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		if err != nil {
			log.Printf("system update route checkpoint load failed: %v", err)
			return
		}
		defer f.Close()
		data, err := io.ReadAll(io.LimitReader(f, 4*1024*1024+1))
		var records []updateRouteRecord
		if err != nil || len(data) > 4*1024*1024 || json.Unmarshal(data, &records) != nil || len(records) > 10000 {
			log.Printf("system update route checkpoint invalid; not restored")
			return
		}
		fingerprints := make(map[string]string)
		restored, skipped := 0, 0
		for _, record := range records {
			fingerprint, cached := fingerprints[record.Provider]
			if !cached {
				fingerprint = h.bindingFingerprint(record.Provider)
				fingerprints[record.Provider] = fingerprint
			}
			if record.Route == "" || fingerprint == "" || fingerprint != record.Fingerprint || record.CreatedAt.IsZero() || time.Since(record.CreatedAt) > 30*24*time.Hour {
				skipped++
				continue
			}
			if _, exists := h.Client.ResponseRoutes.Load(record.Route); exists {
				skipped++
				continue
			}
			// 恢復滑動有效期，讓更新後第一個接續請求能查回原來源。
			h.Client.RecordPromptCacheRoute(record.Route, record.Provider, record.Model, record.Owner)
			restored++
		}
		log.Printf("system update route checkpoint restored: routes=%d skipped=%d", restored, skipped)
	})
}
