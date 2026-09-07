package api

import (
	"LoadBalanceProvider/src/proxy"
	"os"
	"strings"
	"testing"
)

func TestUpdateCheckpointRestoresProviderBinding(t *testing.T) {
	h := durableBindingTestHandler(t)
	h.Client.RecordPromptCacheRoute("prompt-cache:conversation", "a", "model", "owner")
	if err := h.saveUpdateRouteCheckpoint(); err != nil {
		t.Fatal(err)
	}
	restarted := &HTTPAPI{Client: proxy.NewClient(), Balancer: h.Balancer, AdvancedSettingsConfigPath: h.AdvancedSettingsConfigPath}
	restarted.restoreUpdateRouteCheckpoint()
	target, ok := restarted.Client.LookupResponseRouteForOwner("prompt-cache:conversation", "owner")
	if !ok || target.ProviderID != "a" {
		t.Fatal("provider binding was not restored")
	}
	if restarted.Client.PromptCacheRouteCounts()["a"] != 1 {
		t.Fatal("binding count was not restored")
	}
	data, err := os.ReadFile(h.updateRouteCheckpointPath())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "test-key") {
		t.Fatal("credential leaked into checkpoint")
	}
}
