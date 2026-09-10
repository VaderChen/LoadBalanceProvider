package balancer

import (
	"testing"
	"time"
)

func TestOverloadBackoffSharesWindow(t *testing.T) {
	p := &ProviderRuntime{}
	first := p.NextOverloadBackoff(0)
	if first < 30*time.Second || first >= 31*time.Second {
		t.Fatalf("first delay = %s", first)
	}
	until := p.overloadUntil
	p.NextOverloadBackoff(0)
	p.MarkSuccess(time.Millisecond)
	if p.consecutiveOverloads != 1 || !p.overloadUntil.Equal(until) {
		t.Fatal("same-window failure or in-flight success changed backoff")
	}
	p.NextOverloadBackoff(time.Minute)
	if p.consecutiveOverloads != 1 || !p.overloadUntil.After(until) {
		t.Fatal("Retry-After must extend the window without escalating")
	}
}

func TestOverloadBackoffEscalatesOnlyAfterWindow(t *testing.T) {
	p := &ProviderRuntime{}
	for _, base := range []time.Duration{30, 60, 90, 90, 90} {
		p.overloadUntil = time.Now().Add(-time.Second)
		delay := p.NextOverloadBackoff(0)
		if delay < base*time.Second || delay >= (base+1)*time.Second {
			t.Fatalf("delay = %s, expected base %s", delay, base*time.Second)
		}
	}
	p.overloadUntil = time.Now().Add(-time.Second)
	p.MarkSuccess(time.Millisecond)
	if p.consecutiveOverloads != 0 || !p.overloadUntil.IsZero() {
		t.Fatal("success after cooldown did not reset backoff")
	}
}
