package proxy

import (
	"bytes"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type bufferedDeliveryWriter struct {
	*httptest.ResponseRecorder
	buffer bytes.Buffer
}

type countedStreamBody struct {
	io.Reader
	closed atomic.Int32
}

func (b *countedStreamBody) Close() error { b.closed.Add(1); return nil }

func TestProviderStreamOwnsCleanup(t *testing.T) {
	before := activeProviderStreams.Load()
	body := &countedStreamBody{Reader: strings.NewReader("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n")}
	_, err := streamCopyWithProviderIdleTimeout(httptest.NewRecorder(), body, time.Now(), true, nil, nil, nil, ResponsesFailureTerminal, responsesStreamFailureTerminal)
	if err != nil || body.closed.Load() != 1 || activeProviderStreams.Load() != before {
		t.Fatalf("stream cleanup failed: closes=%d active=%d error=%v", body.closed.Load(), activeProviderStreams.Load(), err)
	}
	body = &countedStreamBody{Reader: strings.NewReader("")}
	r := newStreamIdleTimeoutReader(body, time.Minute)
	_ = r.Close()
	_ = r.Close()
	if body.closed.Load() != 1 {
		t.Fatal("body closed more than once")
	}
}

func (w *bufferedDeliveryWriter) Write(b []byte) (int, error) { return w.buffer.Write(b) }
func (w *bufferedDeliveryWriter) ContentWritten() bool        { return false }

func TestBufferedToolFailureRemainsRetryable(t *testing.T) {
	w := &bufferedDeliveryWriter{ResponseRecorder: httptest.NewRecorder()}
	body := "data: {\"type\":\"response.custom_tool_call_input.delta\",\"delta\":\"command\"}\n\ndata: {\"type\":\"error\",\"message\":\"Our servers are currently overloaded\"}\n\n"
	metrics, err := streamCopyWithResponseRecorder(w, strings.NewReader(body), time.Now(), true, nil, nil, ResponsesFailureTerminal, responsesStreamFailureTerminal)
	var failure *ProviderStreamError
	if !errors.As(err, &failure) || failure.ResponseForwarded || !IsRetryableCapacityError(err) || metrics.ClientContentItems != 0 {
		t.Fatalf("buffered content treated as delivered: metrics=%+v error=%v", metrics, err)
	}
	if strings.Contains(w.buffer.String(), "response.failed") {
		t.Fatal("retryable error appended a terminal to buffered content")
	}
}
