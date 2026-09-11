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
	last          time.Time
	active        int64
	cooldownUntil time.Time
	recovering    bool
	probeStarted  bool
}

// 所有 Client 共用實際發送閘門，設定重載不會清除發送間隔。
var providerDispatch = struct {
	sync.Mutex
	states    map[string]*providerDispatchState
	settings  domain.AdvancedSettingsConfig
	providers []domain.LLMProviderConfig
	last      time.Time
	active    int64
}{states: make(map[string]*providerDispatchState)}

func Acquire(ctx context.Context, p *domain.LLMProviderConfig) (func(), error) {
	started := time.Now()
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
			if state.active == 0 && !state.recovering && now.Sub(state.last) >= 10*time.Second {
				delete(providerDispatch.states, id)
			}
		}
		state := providerDispatch.states[key]
		if state == nil {
			state = &providerDispatchState{}
			providerDispatch.states[key] = state
		}
		wait := state.last.Add(10 * time.Second).Sub(now)
		settings := providerDispatch.settings
		if settings.GlobalDispatchRateEnabled {
			wait = max(wait, providerDispatch.last.Add(5*time.Second).Sub(now))
		}
		if settings.GlobalConcurrencyEnabled && providerDispatch.active >= globalConcurrencyLimitLocked(now) {
			wait = max(wait, 100*time.Millisecond)
		}
		if settings.CooldownSingleProbeEnabled && state.recovering {
			wait = max(wait, state.cooldownUntil.Sub(now))
			if state.active > 0 {
				wait = max(wait, 100*time.Millisecond)
			}
		}
		if p.MaxConcurrent > 0 && state.active >= p.MaxConcurrent {
			wait = max(wait, 100*time.Millisecond)
		}
		if wait <= 0 {
			state.last, state.active = now, state.active+1
			providerDispatch.last, providerDispatch.active = now, providerDispatch.active+1
			if settings.CooldownSingleProbeEnabled && state.recovering {
				state.probeStarted = true
			}
			providerDispatch.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() { providerDispatch.Lock(); state.active--; providerDispatch.active--; providerDispatch.Unlock() })
			}, nil
		}
		providerDispatch.Unlock()
		if settings.GlobalDispatchRateEnabled || settings.GlobalConcurrencyEnabled || settings.CooldownSingleProbeEnabled {
			remaining := time.Duration(settings.ProviderRetryWaitSeconds)*time.Second - time.Since(started)
			if remaining <= 0 {
				return nil, ErrProtectionWaitExceeded
			}
			wait = min(wait, remaining)
		}
		// 定期重讀開關；關閉保護時不必等完原本的長冷卻。
		timer := time.NewTimer(min(wait, time.Second))
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
