package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type timeoutSmokeTransport func(*http.Request) (*http.Response, error)

func TestProviderErrorBodyTimeoutSmoke(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	req, _ := http.NewRequest(http.MethodPost, server.URL, nil)
	resp, err := doProviderHTTPRequest(server.Client(), req, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("stalled error body did not time out")
	}
}

func TestCodexChatCommentDoesNotTerminateSmoke(t *testing.T) {
	r := httptest.NewRecorder()
	_, err := streamCodexResponsesAsChat(r, strings.NewReader(": ping\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"after-comment\"}\n\ndata: [DONE]\n\n"), "model", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.Body.String(), "[DONE]") || !strings.Contains(r.Body.String(), "after-comment") {
		t.Fatal("missing completion")
	}
}

func TestProviderRealHTTPHeaderTimeoutSmoke(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	req, _ := http.NewRequest(http.MethodPost, server.URL, nil)
	_, err := doProviderHTTPRequest(server.Client(), req, 40*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected local HTTP deadline: %v", err)
	}
}

func (f timeoutSmokeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestProviderHeaderTimeoutSmoke(t *testing.T) {
	client := &http.Client{Transport: timeoutSmokeTransport(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	req, _ := http.NewRequest(http.MethodPost, "https://example.com", nil)
	_, err := doProviderHTTPRequest(client, req, 30*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline, got %v", err)
	}
}

func TestProviderHeaderTimeoutDoesNotLimitBodySmoke(t *testing.T) {
	var ctx context.Context
	client := &http.Client{Transport: timeoutSmokeTransport(func(r *http.Request) (*http.Response, error) {
		ctx = r.Context()
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})}
	req, _ := http.NewRequest(http.MethodPost, "https://example.com", nil)
	resp, err := doProviderHTTPRequest(client, req, 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	if ctx.Err() != nil {
		t.Fatalf("body canceled after headers: %v", ctx.Err())
	}
	resp.Body.Close()
	if ctx.Err() == nil {
		t.Fatal("body close did not release context")
	}
}

func TestProviderHeartbeatResetsIdleSmoke(t *testing.T) {
	r, w := io.Pipe()
	defer w.Close()
	idle := newStreamIdleTimeoutReader(r, time.Minute)
	defer idle.Close()
	for _, event := range []string{"keepalive", "ping", "response.ping", "heartbeat", "keep-alive", "comment"} {
		before := time.Now().Add(-time.Second).UnixNano()
		idle.lastActivity.Store(before)
		idle.MarkStreamActivity(event)
		if idle.lastActivity.Load() <= before || idle.LastEventType() != event {
			t.Fatalf("upstream heartbeat did not reset idle timer: %s", event)
		}
	}
}
