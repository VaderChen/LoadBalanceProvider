package providerdispatch

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/security"
)

type providerDispatchState struct {
	last   time.Time
	active int64
}

// 所有 Client 共用實際發送閘門，設定重載不會清除發送間隔。
var providerDispatch = struct {
	sync.Mutex
	states map[string]*providerDispatchState
}{states: make(map[string]*providerDispatchState)}

func Acquire(ctx context.Context, p *domain.LLMProviderConfig) (func(), error) {
	key := p.ID
	if key == "" {
		key = p.BaseURL
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := p.CheckScheduledDowntime(); err != nil {
			return nil, err
		}
		now := time.Now()
		providerDispatch.Lock()
		for id, state := range providerDispatch.states {
			if state.active == 0 && now.Sub(state.last) >= 10*time.Second {
				delete(providerDispatch.states, id)
			}
		}
		state := providerDispatch.states[key]
		if state == nil {
			state = &providerDispatchState{}
			providerDispatch.states[key] = state
		}
		wait := state.last.Add(10 * time.Second).Sub(now)
		if p.MaxConcurrent > 0 && state.active >= p.MaxConcurrent {
			wait = max(wait, 100*time.Millisecond)
		}
		if wait <= 0 {
			state.last, state.active = now, state.active+1
			providerDispatch.Unlock()
			var once sync.Once
			return func() { once.Do(func() { providerDispatch.Lock(); state.active--; providerDispatch.Unlock() }) }, nil
		}
		providerDispatch.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

type dispatchResponseBody struct {
	io.ReadCloser
	release func()
}

func (b *dispatchResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.release()
	return err
}

func Do(client *http.Client, req *http.Request, p *domain.LLMProviderConfig) (*http.Response, error) {
	release, err := Acquire(req.Context(), p)
	if err != nil {
		return nil, err
	}
	resp, err := security.GuardedHTTPClient(client).Do(req)
	if err != nil {
		release()
		return resp, err
	}
	resp.Body = &dispatchResponseBody{ReadCloser: resp.Body, release: release}
	return resp, nil
}
