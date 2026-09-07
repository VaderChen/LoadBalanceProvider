package proxy

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestTurnRouteRebindHasOneWinner(t *testing.T) {
	c := NewClient()
	if !c.ClaimTurnRoute("turn", ResponseRouteTarget{ProviderID: "a", Model: "m", Owner: "owner"}) {
		t.Fatal("initial claim failed")
	}
	if c.RebindTurnRoute("turn", "a", "other", ResponseRouteTarget{ProviderID: "b", Model: "m", Owner: "other"}) {
		t.Fatal("owner boundary bypassed")
	}
	var wins atomic.Int32
	var wg sync.WaitGroup
	for _, provider := range []string{"b", "c"} {
		wg.Add(1)
		go func(provider string) {
			defer wg.Done()
			if c.RebindTurnRoute("turn", "a", "owner", ResponseRouteTarget{ProviderID: provider, Model: "m", Owner: "owner"}) {
				wins.Add(1)
			}
		}(provider)
	}
	wg.Wait()
	if wins.Load() != 1 || atomic.LoadInt64(&c.responseRouteCount) != 1 {
		t.Fatal("rebind lost atomicity or changed route count")
	}
}
