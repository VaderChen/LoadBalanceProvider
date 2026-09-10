package providerdispatch

import (
	"context"
	"errors"
	"testing"
	"time"

	"LoadBalanceProvider/src/domain"
)

func TestDispatchHoldsRapidRequestsAndReleasesOnce(t *testing.T) {
	p := &domain.LLMProviderConfig{ID: t.Name(), MaxConcurrent: 2}
	release, err := Acquire(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := Acquire(ctx, p); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("密集請求未 HOLD: %v", err)
	}
	providerDispatch.Lock()
	state := providerDispatch.states[p.ID]
	state.last = time.Now().Add(-11 * time.Second)
	providerDispatch.Unlock()
	second, err := Acquire(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	second()
	second()
	release()
	release()
	providerDispatch.Lock()
	defer providerDispatch.Unlock()
	if state.active != 0 {
		t.Fatalf("名額釋放錯誤: %d", state.active)
	}
}

func TestDispatchPreservesMaximumConcurrency(t *testing.T) {
	p := &domain.LLMProviderConfig{ID: t.Name(), MaxConcurrent: 1}
	release, err := Acquire(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	providerDispatch.Lock()
	providerDispatch.states[p.ID].last = time.Now().Add(-11 * time.Second)
	providerDispatch.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := Acquire(ctx, p); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("超過最大併發: %v", err)
	}
}
