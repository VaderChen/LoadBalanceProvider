package api

import (
	"errors"
	"testing"

	"LoadBalanceProvider/src/domain"
)

func TestProviderWaitQueueBoundAndRelease(t *testing.T) {
	var q providerWaitQueue
	req := domain.ChatCompletionRequest{ProviderID: "source-a"}
	var releases []func()
	for i := 0; i < providerMaxWaiting; i++ {
		release, err := q.acquire(req)
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
		defer release()
	}
	if _, err := q.acquire(req); !errors.Is(err, errProviderWaitQueueFull) {
		t.Fatalf("full queue: %v", err)
	}
	other, err := q.acquire(domain.ChatCompletionRequest{ProviderID: "source-b"})
	if err != nil {
		t.Fatal(err)
	}
	other()
	releases[0]()
	releases[0]()
	release, err := q.acquire(req)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := q.acquire(req); !errors.Is(err, errProviderWaitQueueFull) {
		t.Fatalf("double release must not add a slot: %v", err)
	}
}
