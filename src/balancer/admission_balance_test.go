package balancer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"LoadBalanceProvider/src/domain"
)

func admissionTestBalancer() *LoadBalancer {
	config := &domain.ProxyConfig{SelectionStrategy: "random"}
	for i := 0; i < 4; i++ {
		config.Providers = append(config.Providers, domain.LLMProviderConfig{
			ID: fmt.Sprintf("p%d", i), Name: fmt.Sprintf("p%d", i), Kind: "openai",
			Enabled: true, BaseURL: "https://example.invalid", Weight: 1, Priority: i + 1, MaxConcurrent: 1,
			Models: []domain.LLMModelConfig{{Name: "balanced-model", MaxInputTokens: 100000, MaxOutputTokens: 8192, Capabilities: []string{"chat"}}},
		})
	}
	return NewLoadBalancer(config)
}

func TestNewTurnBalanceIncludesDifferentDisplayPriorities(t *testing.T) {
	b := admissionTestBalancer()
	counts := map[string]int{}
	for i := 0; i < 16; i++ {
		p, _, _, _, err := b.Acquire(&domain.ChatCompletionRequest{Model: "balanced-model"})
		if err != nil {
			t.Fatal(err)
		}
		counts[p.Config.ID]++
		p.RequestRelease()()
	}
	for _, p := range b.ProvidersSnapshot() {
		if counts[p.Config.ID] != 4 {
			t.Fatalf("新回合分配不均衡: %v", counts)
		}
	}
}

func TestQuotaBalanceDoesNotMovePinnedTurn(t *testing.T) {
	b := admissionTestBalancer()
	for i, p := range b.Providers {
		remaining := 20.0
		if i == 1 {
			remaining = 90
		}
		p.Usage = ProviderUsageSnapshot{UpdatedAt: time.Now(), LimitRequests: "100", RemainingRequests: fmt.Sprint(remaining), RequestRemainingPercent: remaining}
	}
	p, _, _, _, err := b.Acquire(&domain.ChatCompletionRequest{Model: "balanced-model"})
	if err != nil || p.Config.ID != "p1" {
		t.Fatalf("未優先分配剩餘額度較多的來源: %v %v", p, err)
	}
	p.RequestRelease()()
	p, _, _, _, err = b.Acquire(&domain.ChatCompletionRequest{Model: "balanced-model", ProviderID: "p0"})
	if err != nil || p.Config.ID != "p0" {
		t.Fatalf("用量均衡改變了既有配對: %v %v", p, err)
	}
	p.RequestRelease()()
}

func TestAtomicAdmissionSurvivesConfigReload(t *testing.T) {
	b := admissionTestBalancer()
	req := &domain.ChatCompletionRequest{Model: "balanced-model", ProviderID: "p0"}
	p, _, _, _, err := b.Acquire(req)
	if err != nil {
		t.Fatal(err)
	}
	release := p.RequestRelease()
	defer release()
	config := b.ConfigSnapshot()
	b.ReloadConfig(&config)
	if _, _, _, _, err = b.Acquire(req); err == nil {
		t.Fatal("重載遺失活躍名額")
	}
	p.MarkSuccess(time.Second)
	if b.ProviderStatus()[0]["successes"] != int64(1) {
		t.Fatal("舊請求完成後狀態未合併")
	}
	release()
	release()
	if b.ProviderStatus()[0]["active"] != int64(0) {
		t.Fatal("名額沒有正確歸還")
	}
	var accepted atomic.Int64
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			target, _, _, _, err := b.Acquire(req)
			if err == nil {
				accepted.Add(1)
				_ = target
			}
		}()
	}
	close(gate)
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("併發名額超額: %d", accepted.Load())
	}
	b.ProvidersSnapshot()[0].RequestRelease()()
}
