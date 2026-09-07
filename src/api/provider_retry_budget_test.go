package api

import "testing"

func TestRetryRoundsRequireAnAttempt(t *testing.T) {
	b := providerRetryBudget{maxRounds: 2, maxSources: 1}
	if b.nextRound() {
		t.Fatal("waiting used a retry round")
	}
	for i := 1; i <= 2; i++ {
		b.used = []string{"p"}
		if !b.nextRound() || b.round != i || len(b.used) != 0 {
			t.Fatal("invalid retry round")
		}
	}
	b.used = []string{"p"}
	if b.nextRound() {
		t.Fatal("retry budget exceeded")
	}
}
