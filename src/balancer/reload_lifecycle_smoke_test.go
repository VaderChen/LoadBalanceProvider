package balancer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"LoadBalanceProvider/src/domain"
)

func TestConcurrentReloadAdmissionLifecycleSmoke(t *testing.T) {
	b := admissionTestBalancer()
	var wg sync.WaitGroup
	var completed atomic.Int64
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				p, _, _, _, err := b.Acquire(&domain.ChatCompletionRequest{Model: "balanced-model"})
				if err != nil {
					continue
				}
				release := p.RequestRelease()
				if p.ActiveCount() > p.Config.MaxConcurrent {
					t.Error("重載期間超額接收請求")
				}
				p.MarkSuccess(time.Millisecond)
				completed.Add(1)
				release()
				release()
			}
		}()
	}
	for i := 0; i < 100; i++ {
		config := b.ConfigSnapshot()
		b.ReloadConfig(&config)
		_ = b.ProviderStatus()
	}
	wg.Wait()
	var recorded int64
	for _, status := range b.ProviderStatus() {
		if status["active"] != int64(0) {
			t.Fatalf("請求完成後名額未收回: %+v", status)
		}
		recorded += status["successes"].(int64)
	}
	if recorded == 0 || recorded != completed.Load() {
		t.Fatalf("重載遺失完成紀錄: got=%d want=%d", recorded, completed.Load())
	}
}
