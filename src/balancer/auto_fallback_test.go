package balancer

import (
	"testing"
	"time"

	"LoadBalanceProvider/src/domain"
)

func TestAutoFallbackPreservesRequiredCapabilities(t *testing.T) {
	profile := domain.RequestProfile{HardRequirements: []string{"tools", "long_context", "vision"}}
	fallback := autoFallbackProfile(&domain.ChatCompletionRequest{Model: "AUTO"}, profile)
	if len(fallback.HardRequirements) != 2 || fallback.HardRequirements[0] != "tools" || fallback.HardRequirements[1] != "vision" || len(profile.HardRequirements) != 3 {
		t.Fatalf("unexpected fallback or mutated original: %+v / %+v", fallback, profile)
	}
	explicit := autoFallbackProfile(&domain.ChatCompletionRequest{RequiredCapabilities: []string{"long_context"}}, profile)
	if len(explicit.HardRequirements) != 3 {
		t.Fatal("explicit capability was removed")
	}
}

func TestAutoFallbackStillHonorsCooldownAndTokenLimits(t *testing.T) {
	b := NewLoadBalancer(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{testSessionProvider("a")}})
	model := &b.Providers[0].Config.Models[0]
	model.Capabilities = []string{"chat"}
	model.MaxInputTokens, model.MaxOutputTokens = 64000, 4000
	req := &domain.ChatCompletionRequest{Model: "AUTO"}
	profile := domain.RequestProfile{HardRequirements: []string{"long_context"}, EstimatedInputTokens: 40000, RequestedOutputTokens: 1000}
	if candidates, _ := b.collectCandidates(req, profile, "AUTO", nil); len(candidates) != 0 {
		t.Fatal("expected initial capability mismatch")
	}
	profile = autoFallbackProfile(req, profile)
	if candidates, _ := b.collectCandidates(req, profile, "AUTO", nil); len(candidates) == 0 {
		t.Fatal("expected token-compatible fallback")
	}
	profile.EstimatedInputTokens = 100000
	if candidates, _ := b.collectCandidates(req, profile, "AUTO", nil); len(candidates) != 0 {
		t.Fatal("fallback exceeded token limit")
	}
	profile.EstimatedInputTokens = 40000
	b.Providers[0].MarkTemporaryUnavailable(0, time.Minute)
	if candidates, _ := b.collectCandidates(req, profile, "AUTO", nil); len(candidates) != 0 {
		t.Fatal("fallback bypassed cooldown")
	}
}
