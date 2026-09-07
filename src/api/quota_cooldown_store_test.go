package api

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/domain"
)

func TestQuotaCooldownPersistenceIdentityAndClear(t *testing.T) {
	path := filepath.Join(t.TempDir(), "advanced_settings.json")
	newHandler := func(key string) *HTTPAPI {
		return &HTTPAPI{AdvancedSettingsConfigPath: path, Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{
			{ID: "p", Kind: "openai", BaseURL: "https://example.com", APIKey: key, Enabled: true},
		}})}
	}
	h := newHandler("test-secret-not-for-storage")
	p := h.Balancer.ProvidersSnapshot()[0]
	p.MarkQuotaUnavailable(time.Millisecond, time.Hour)
	h.persistQuotaCooldown(p)
	data, err := os.ReadFile(h.quotaCooldownPath())
	if err != nil || strings.Contains(string(data), "test-secret-not-for-storage") {
		t.Fatalf("invalid persisted data: %v", err)
	}
	restored := newHandler("test-secret-not-for-storage")
	restored.restoreQuotaCooldowns()
	if !restored.Balancer.ProvidersSnapshot()[0].CapacityUnavailable(time.Now()) {
		t.Fatal("quota cooldown not restored")
	}
	changed := newHandler("different-secret")
	changed.restoreQuotaCooldowns()
	if changed.Balancer.ProvidersSnapshot()[0].CapacityUnavailable(time.Now()) {
		t.Fatal("quota applied to different identity")
	}
	restored.clearQuotaCooldown("p")
	afterReset := newHandler("test-secret-not-for-storage")
	afterReset.restoreQuotaCooldowns()
	if afterReset.Balancer.ProvidersSnapshot()[0].CapacityUnavailable(time.Now()) {
		t.Fatal("cleared quota resurrected")
	}
}
