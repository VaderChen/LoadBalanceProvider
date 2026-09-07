package balancer

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"LoadBalanceProvider/src/domain"
)

func TestLayeredCooldownSmoke(t *testing.T) {
	p := testSessionProvider("account")
	p.Models = append(p.Models, domain.LLMModelConfig{Name: "other", MaxInputTokens: 1048576, MaxOutputTokens: 8192, Capabilities: []string{"chat"}})
	b := NewLoadBalancer(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{p}})
	runtime := b.ProvidersSnapshot()[0]
	runtime.MarkModelUnavailable("gpt-test", time.Millisecond, time.Minute)
	until := runtime.ModelUnavailableUntil("gpt-test")
	runtime.MarkModelUnavailable("gpt-test", time.Millisecond, time.Hour)
	if !runtime.ModelUnavailableUntil("gpt-test").After(until) {
		t.Fatal("longer model cooldown was ignored")
	}
	_, _, _, _, err := b.Select(&domain.ChatCompletionRequest{Model: "gpt-test"})
	var unavailable *NoAvailableProviderError
	if !errors.As(err, &unavailable) || !unavailable.TemporaryOverload {
		t.Fatalf("missing model cooldown: %v", err)
	}
	if _, _, _, _, err := b.Select(&domain.ChatCompletionRequest{Model: "other"}); err != nil {
		t.Fatalf("other model was blocked: %v", err)
	}
	runtime.MarkTemporaryUnavailable(time.Millisecond, time.Minute)
	accountUntil := atomic.LoadInt64(&runtime.CapacityUnavailableUntil)
	runtime.MarkTemporaryUnavailable(time.Millisecond, time.Hour)
	if atomic.LoadInt64(&runtime.CapacityUnavailableUntil) <= accountUntil {
		t.Fatal("longer account cooldown was ignored")
	}
	accountUntil = atomic.LoadInt64(&runtime.CapacityUnavailableUntil)
	runtime.MarkSuccessWithMetrics(time.Millisecond, 1, 0, 0, 0)
	if atomic.LoadInt64(&runtime.CapacityUnavailableUntil) != accountUntil {
		t.Fatal("in-flight success cleared account cooldown")
	}
}
