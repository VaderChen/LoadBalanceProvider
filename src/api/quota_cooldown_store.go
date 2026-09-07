package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/codexauth"
)

type quotaCooldownEntry struct {
	Fingerprint string    `json:"fingerprint"`
	Until       time.Time `json:"until"`
}

type quotaCooldownState struct {
	sync.Mutex
	loaded  bool
	entries map[string]quotaCooldownEntry
}

func quotaFingerprint(p *balancer.ProviderRuntime) string {
	if p == nil || p.Config == nil {
		return ""
	}
	c := p.Config
	identity := []string{c.ID, c.Kind, c.Type, c.BaseURL, c.APIKey, c.APIKeyEnv, os.Getenv(c.APIKeyEnv)}
	if strings.EqualFold(inferProviderKind(*c), "openai-codex") {
		record, err := codexauth.NewStore("").Get(c.ID)
		if err != nil || record.AccountSub == "" {
			return ""
		}
		identity = append(identity, record.AccountSub)
	}
	data, _ := json.Marshal(identity)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (h *HTTPAPI) quotaCooldownPath() string {
	return filepath.Join(filepath.Dir(h.advancedSettingsConfigPath()), "provider_quota_cooldowns.json")
}

func (h *HTTPAPI) loadQuotaCooldownsLocked(enabled bool) {
	s := &h.quotaCooldowns
	if s.loaded {
		return
	}
	s.loaded = true
	s.entries = make(map[string]quotaCooldownEntry)
	if !enabled {
		return
	}
	f, err := os.Open(h.quotaCooldownPath())
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		log.Printf("quota cooldown load failed: %v", err)
		return
	}
	defer f.Close()
	var saved map[string]quotaCooldownEntry
	if err = json.NewDecoder(io.LimitReader(f, 1024*1024)).Decode(&saved); err != nil {
		log.Printf("quota cooldown file invalid; ignoring saved state")
		return
	}
	now := time.Now()
	for id, entry := range saved {
		if len(id) <= 256 && len(entry.Fingerprint) == 64 && entry.Until.After(now) && entry.Until.Before(now.Add(90*24*time.Hour)) {
			s.entries[id] = entry
		}
	}
}

func (h *HTTPAPI) restoreQuotaCooldowns() {
	if h == nil || h.Balancer == nil {
		return
	}
	enabled := h.currentAdvancedSettings().PersistQuotaCooldown
	s := &h.quotaCooldowns
	s.Lock()
	defer s.Unlock()
	h.loadQuotaCooldownsLocked(enabled)
	if !enabled {
		return
	}
	for _, p := range h.Balancer.ProvidersSnapshot() {
		if p == nil || p.Config == nil {
			continue
		}
		if entry, ok := s.entries[p.Config.ID]; ok {
			fingerprint := quotaFingerprint(p)
			if !entry.Until.After(time.Now()) {
				delete(s.entries, p.Config.ID)
			} else if fingerprint == "" {
				continue
			} else if entry.Fingerprint == fingerprint {
				p.RestoreQuotaCooldown(entry.Until)
			} else {
				p.ClearQuotaCooldown()
				delete(s.entries, p.Config.ID)
			}
		}
	}
}

func (h *HTTPAPI) persistQuotaCooldown(p *balancer.ProviderRuntime) {
	if p == nil || p.Config == nil {
		return
	}
	enabled := h.currentAdvancedSettings().PersistQuotaCooldown
	s := &h.quotaCooldowns
	s.Lock()
	defer s.Unlock()
	h.loadQuotaCooldownsLocked(enabled)
	until := p.QuotaCooldownUntil()
	fingerprint := quotaFingerprint(p)
	// 短暫冷卻只留記憶體；永久儲存必須有可驗證的來源識別。
	if enabled && fingerprint != "" && time.Until(until) >= 5*time.Minute {
		s.entries[p.Config.ID] = quotaCooldownEntry{Fingerprint: fingerprint, Until: until}
	}
	if err := h.saveQuotaCooldownsLocked(enabled); err != nil {
		log.Printf("quota cooldown save failed: %v", err)
	}
}

func (h *HTTPAPI) clearQuotaCooldown(id string) {
	enabled := h.currentAdvancedSettings().PersistQuotaCooldown
	s := &h.quotaCooldowns
	s.Lock()
	defer s.Unlock()
	h.loadQuotaCooldownsLocked(enabled)
	delete(s.entries, id)
	if p, ok := h.findProviderRuntime(id); ok {
		p.ClearQuotaCooldown()
	}
	if err := h.saveQuotaCooldownsLocked(enabled); err != nil {
		log.Printf("quota cooldown clear failed: %v", err)
	}
}

func (h *HTTPAPI) saveQuotaCooldownsLocked(enabled bool) error {
	path := h.quotaCooldownPath()
	if !enabled {
		err := os.Remove(path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	s := &h.quotaCooldowns
	for id, entry := range s.entries {
		if !entry.Until.After(time.Now()) {
			delete(s.entries, id)
		}
	}
	data, err := json.Marshal(s.entries)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".quota-cooldown-*")
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
