package providerdispatch

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"LoadBalanceProvider/src/domain"
)

type timingTransport func(*http.Request) (*http.Response, error)

func (f timingTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDispatchTimingSmoke(t *testing.T) {
	var sent []time.Time
	client := &http.Client{Transport: timingTransport(func(*http.Request) (*http.Response, error) {
		sent = append(sent, time.Now())
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})}
	p := &domain.LLMProviderConfig{ID: t.Name(), MaxConcurrent: 2}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", "https://8.8.8.8", nil)
	first, err := Do(client, req, p)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Body.Close()
	second, err := Do(client, req, p)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Body.Close()
	if len(sent) != 2 || sent[1].Sub(sent[0]) < 10*time.Second {
		t.Fatalf("實際發送間隔不足: %v", sent)
	}
	providerDispatch.Lock()
	active := providerDispatch.states[p.ID].active
	providerDispatch.Unlock()
	if active != 2 {
		t.Fatalf("未保留兩筆同時連線: %d", active)
	}
	t.Logf("實際發送間隔 %s，兩筆回應同時開啟", sent[1].Sub(sent[0]))
}
