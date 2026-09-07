package api

import (
	"net/http/httptest"
	"testing"
	"time"

	"LoadBalanceProvider/src/proxy"
)

func TestReconnectIdentityIsolation(t *testing.T) {
	key := func(owner, turn, body string) string {
		r := httptest.NewRequest("POST", "/v1/responses", nil)
		r = r.WithContext(proxy.WithResponseRouteOwner(r.Context(), owner))
		r.Header.Set("X-Proxy-Turn-ID", turn)
		r = withReconnectIdentity(r, []byte(body))
		v, _ := r.Context().Value(reconnectIdentityKey{}).(string)
		return v
	}
	body := `{"prompt_cache_key":"task","input":"hello","seed":9007199254740993}`
	want := key("key:a", "", body)
	if want == "" || want != key("key:a", "", `{"seed":9007199254740993, "input":"hello", "prompt_cache_key":"task"}`) {
		t.Fatal("equivalent JSON must share identity")
	}
	for _, got := range []string{
		key("key:b", "", body), key("key:a", "new-turn", body),
		key("key:a", "", `{"prompt_cache_key":"task","input":"hello","seed":9007199254740992}`),
		key("key:a", "", `{"prompt_cache_key":"task","input":[{"type":"function_call_output","call_id":"call1","output":""}]}`),
	} {
		if got == want {
			t.Fatal("distinct request shared identity")
		}
	}
	if key("anonymous", "", body) != "" || key("key:a", "", `{"input":"hello"}`) != "" {
		t.Fatal("unidentified requests must not share a content budget")
	}
}

func TestReconnectBudgetAcrossAttempts(t *testing.T) {
	var store reconnectBudgetStore
	e, rejection := store.acquire("a", 3)
	if rejection != nil {
		t.Fatal(rejection)
	}
	if _, rejection = store.acquire("a", 3); rejection == nil || rejection.code != "request_in_progress" {
		t.Fatal("concurrent replay accepted")
	}
	e.attempts, e.provider, e.model, e.waited = 2, "p", "m", time.Second
	store.release("a", e, false)
	retry, rejection := store.acquire("a", 5)
	if rejection != nil || retry != e || retry.limit != 3 || retry.attempts != 2 || retry.waited != time.Second || retry.provider != "p" {
		t.Fatal("reconnect reset budget")
	}
	retry.attempts++
	store.release("a", retry, false)
	if _, rejection = store.acquire("a", 3); rejection == nil || rejection.code != "request_retry_exhausted" {
		t.Fatal("exhausted replay accepted")
	}
	deadline := e.retryAt
	if _, rejection = store.acquire("a", 3); rejection == nil || e.retryAt != deadline || rejection.retryAfter <= 0 {
		t.Fatal("blocked reconnect extended cooldown or omitted remaining wait")
	}
	e.retryAt = time.Now().Add(-time.Second)
	probe, rejection := store.acquire("a", 3)
	if rejection != nil || probe.limit-probe.attempts != 1 || probe.provider != "p" || probe.model != "m" || probe.waited != 0 {
		t.Fatal("cooldown did not admit exactly one same-provider probe")
	}
	probe.attempts++
	store.release("a", probe, false)
	if _, rejection = store.acquire("a", 3); rejection == nil {
		t.Fatal("failed recovery probe did not reapply cooldown")
	}
	e.expires = time.Now().Add(-time.Second)
	fresh, rejection := store.acquire("a", 3)
	if rejection != nil || fresh.attempts != 0 {
		t.Fatal("expired budget not released")
	}
	fresh.attempts, fresh.delivered = 1, true
	store.release("a", fresh, false)
	if _, rejection = store.acquire("a", 3); rejection == nil || rejection.code != "request_replay_unsafe" {
		t.Fatal("partial output replay accepted")
	}
}

func TestReconnectSuccessAndCapacity(t *testing.T) {
	var store reconnectBudgetStore
	e, _ := store.acquire("a", 3)
	e.attempts = 1
	store.release("a", e, true)
	if len(store.entries) != 0 {
		t.Fatal("success retained stale budget")
	}
	for i := 0; i < reconnectEntryLimit; i++ {
		store.entries[string(rune(i))] = &reconnectBudget{active: true}
	}
	if _, reject := store.acquire("overflow", 3); reject == nil || reject.code != "retry_tracker_full" {
		t.Fatal("capacity limit bypassed")
	}
}

func TestReconnectRetainsSeparateWaitBudgets(t *testing.T) {
	var store reconnectBudgetStore
	e, _ := store.acquire("queued", 3)
	e.admissionWaited = 20 * time.Second
	store.release("queued", e, false)
	r, rejection := store.acquire("queued", 3)
	if rejection != nil || r.admissionWaited != 20*time.Second || r.waited != 0 {
		t.Fatal("pre-dispatch reconnect reset queue budget or consumed retry budget")
	}
	r.attempts, r.waited = 1, 5*time.Second
	store.release("queued", r, false)
	r, rejection = store.acquire("queued", 3)
	if rejection != nil || r.admissionWaited != 20*time.Second || r.waited != 5*time.Second {
		t.Fatal("reconnect did not preserve both independent budgets")
	}
}
