package api

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBufferedCodexResponseCommitsOnlyOnSuccess(t *testing.T) {
	target := httptest.NewRecorder()
	w := newDeferredResponseWriter(target, true)
	w.DeferStreamUntilSuccess()
	delta := "data: {\"type\":\"response.custom_tool_call_input.delta\",\"delta\":\"command\"}\n\n"
	if _, err := w.Write([]byte(delta)); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteStreamHeartbeat([]byte(": ping\n\n")); err != nil {
		t.Fatal(err)
	}
	if w.ContentWritten() || strings.Contains(target.Body.String(), "command") {
		t.Fatal("partial tool escaped buffer")
	}
	terminal := "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n"
	if _, err := w.Write([]byte(terminal)); err != nil {
		t.Fatal(err)
	}
	if w.ContentWritten() {
		t.Fatal("write committed before validation")
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	if !w.ContentWritten() || !strings.Contains(target.Body.String(), "command") {
		t.Fatal("validated response not delivered")
	}
}

func TestBufferedIncompleteAndOversizedResponsesNeverLeak(t *testing.T) {
	for _, body := range []string{
		"data: [DONE]\n\n",
		"data: {\"type\":\"response.custom_tool_call_input.delta\",\"delta\":\"unfinished\"}\n\n",
		"data: {\"type\":\"response.incomplete\"}\n\n",
	} {
		target := httptest.NewRecorder()
		w := newDeferredResponseWriter(target, true)
		w.DeferStreamUntilSuccess()
		if _, err := w.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
		if w.Commit() == nil || w.ContentWritten() || target.Body.Len() != 0 {
			t.Fatal("incomplete stream leaked")
		}
		if !w.ResetForGracefulTerminal() {
			t.Fatal("unable to discard failed buffer")
		}
	}
	w := newDeferredResponseWriter(httptest.NewRecorder(), true)
	w.DeferStreamUntilSuccess()
	_, err := w.Write([]byte(strings.Repeat("x", deferredResponseBufferLimit+1)))
	if !errors.Is(err, errResponseBufferLimit) || w.ContentWritten() || providerFailureCanRetryBeforeFirstToken(err, w) {
		t.Fatal("buffer overflow was forwarded or retryable")
	}
}
