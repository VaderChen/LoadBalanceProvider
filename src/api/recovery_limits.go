package api

import (
	"fmt"
	"sync"
	"time"

	"LoadBalanceProvider/src/balancer"
)

const turnRecoveryAttempts = 6
const turnRecoverySwitches = 2

type turnRecoveryKey struct{}
type turnRecoveryState struct {
	expires            time.Time
	attempts, switches int
	failed             map[string]bool
}
type modelRecoveryState struct {
	failures        map[string]time.Time
	until           time.Time
	blocked, active bool
	expires         time.Time
}
type recoveryLimits struct {
	mu     sync.Mutex
	turns  map[string]*turnRecoveryState
	models map[string]*modelRecoveryState
}

// 到期固定以第一次失敗為基準；被拒絕的請求不延長限制。
func (s *recoveryLimits) turnLocked(key string) *turnRecoveryState {
	now := time.Now()
	for k, v := range s.turns {
		if !v.expires.After(now) {
			delete(s.turns, k)
		}
	}
	return s.turns[key]
}
func (s *recoveryLimits) failedProviders(key string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []string
	if e := s.turnLocked(key); e != nil {
		for p := range e.failed {
			result = append(result, p)
		}
	}
	return result
}
func (s *recoveryLimits) checkTurn(key string, switching, recovery bool) error {
	if key == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.turnLocked(key)
	if e == nil {
		if len(s.turns) >= reconnectEntryLimit {
			return fmt.Errorf("回合恢復追蹤容量已滿，請稍後再試")
		}
		return nil
	}
	if recovery && e.attempts >= turnRecoveryAttempts || switching && e.switches >= turnRecoverySwitches {
		seconds := max(1, int((time.Until(e.expires)+time.Second-1)/time.Second))
		return fmt.Errorf("本回合恢復預算已用盡（最多 %d 次恢復、%d 次換帳號），約 %d 秒後解除限制", turnRecoveryAttempts, turnRecoverySwitches, seconds)
	}
	return nil
}
func (s *recoveryLimits) recordTurn(key, provider string, failed, switching, recovery bool) {
	if key == "" || !(failed || switching || recovery) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.turnLocked(key)
	if e == nil {
		if s.turns == nil {
			s.turns = make(map[string]*turnRecoveryState)
		}
		if len(s.turns) >= reconnectEntryLimit {
			return
		}
		e = &turnRecoveryState{expires: time.Now().Add(reconnectRetention), failed: make(map[string]bool)}
		s.turns[key] = e
	}
	if failed {
		e.failed[provider] = true
	}
	if switching {
		e.switches++
	}
	if recovery {
		e.attempts++
	}
}

// 相同上游模型有兩個不同來源在短窗口失敗，開啟共用冷卻與單一恢復探測。
func (s *recoveryLimits) acquireModel(key, provider string) (func(bool, bool), error) {
	s.mu.Lock()
	now := time.Now()
	for k, v := range s.models {
		if !v.active && !v.expires.After(now) {
			delete(s.models, k)
		}
	}
	if s.models == nil {
		s.models = make(map[string]*modelRecoveryState)
	}
	e := s.models[key]
	if e == nil {
		if len(s.models) >= reconnectEntryLimit {
			s.mu.Unlock()
			return nil, &balancer.NoAvailableProviderError{TemporaryOverload: true, RetryAfter: reconnectRetryCooldown}
		}
		e = &modelRecoveryState{failures: make(map[string]time.Time), expires: now.Add(reconnectRetention)}
		s.models[key] = e
	}
	probe := e.blocked
	if probe && (e.active || now.Before(e.until)) {
		wait := time.Until(e.until)
		if wait <= 0 {
			wait = time.Second
		}
		s.mu.Unlock()
		return nil, &balancer.NoAvailableProviderError{TemporaryOverload: true, RetryAfter: wait}
	}
	if probe {
		e.active = true
	}
	e.expires = now.Add(reconnectRetention)
	s.mu.Unlock()
	var once sync.Once
	return func(success, failed bool) {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.models[key] != e {
				return
			}
			now := time.Now()
			if probe {
				e.active = false
				if success {
					e.blocked = false
					e.failures = make(map[string]time.Time)
					return
				}
				if failed {
					e.until = now.Add(reconnectRetryCooldown)
				}
			}
			if !failed {
				return
			}
			for p, at := range e.failures {
				if now.Sub(at) >= reconnectRetryCooldown {
					delete(e.failures, p)
				}
			}
			e.failures[provider] = now
			if !e.blocked && len(e.failures) >= 2 {
				e.blocked = true
				e.until = now.Add(reconnectRetryCooldown)
			}
		})
	}, nil
}
