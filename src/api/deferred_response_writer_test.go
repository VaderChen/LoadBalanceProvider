package api

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"LoadBalanceProvider/src/proxy"
)

func TestToolDeliveryAcrossWrites(t *testing.T) {
	for _, event := range []string{
		"data: {\"type\":\"response.function_call_arguments.done\",\"arguments\":\"{}\"}\n\n",
		"event: response.custom_tool_call_input.done\r\ndata: {\"input\":\"command\"}\r\n\r\n",
		"data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"custom_tool_call\"}}\n\n",
	} {
		w := newDeferredResponseWriter(httptest.NewRecorder(), true)
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"text\"}\n\n"))
		for i := range event {
			if w.ToolCallDelivered() {
				t.Fatalf("tool marked delivered before event boundary at %d", i)
			}
			if _, err := w.Write([]byte(event[i : i+1])); err != nil {
				t.Fatal(err)
			}
		}
		if !w.ToolCallDelivered() {
			t.Fatalf("fragmented tool completion missed: %s", event)
		}
	}
}

type limitedToolWriter struct {
	*httptest.ResponseRecorder
	remaining int
}

func (w *limitedToolWriter) Write(data []byte) (int, error) {
	n := min(w.remaining, len(data))
	w.remaining -= n
	_, _ = w.ResponseRecorder.Write(data[:n])
	if n != len(data) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

func TestToolDeliveryRequiresWrittenCompletion(t *testing.T) {
	event := []byte("data: {\"type\":\"response.function_call_arguments.done\",\"arguments\":\"{}\"}\n\n")
	for _, accepted := range []int{0, len(event) - 1, len(event)} {
		for _, buffered := range []bool{false, true} {
			target := &limitedToolWriter{httptest.NewRecorder(), accepted}
			w := newDeferredResponseWriter(target, true)
			if !buffered {
				w.contentWritten = true
			}
			_, _ = w.Write(event)
			if w.ToolCallDelivered() != (accepted == len(event)) {
				t.Fatalf("incorrect delivery state: accepted=%d buffered=%v", accepted, buffered)
			}
		}
	}
}

func TestDeferredInitializationRemainsBufferedAfterHeartbeatSmoke(t *testing.T) {
	r := httptest.NewRecorder()
	w := newDeferredResponseWriter(r, true)
	w.WriteStreamHeartbeat([]byte(": ping\n\n"))
	for _, kind := range []string{"response.created", "response.in_progress", "codex.rate_limits", "codex.response.metadata"} {
		w.Write([]byte("data: {\"type\":\"" + kind + "\"}\n\n"))
	}
	w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"x"))
	if w.ContentWritten() || r.Body.String() != ": ping\n\n" {
		t.Fatalf("incomplete content leaked: %s", r.Body.String())
	}
	w.Write([]byte("\"}\n\n"))
	if !w.ContentWritten() || !strings.Contains(r.Body.String(), "response.output_text.delta") {
		t.Fatal("complete content remained buffered")
	}
}

// -------------------------------------------------------------------------------------
func TestDeferredResponseWriterBuffersInitializationUntilContent(t *testing.T) {
	_recorder := httptest.NewRecorder()
	_writer := newDeferredResponseWriter(_recorder, true)
	_writer.Header().Set("Content-Type", "text/event-stream")
	_writer.WriteHeader(http.StatusOK)
	_, _ = _writer.Write([]byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n"))
	if _writer.Committed() || _recorder.Body.Len() != 0 {
		t.Fatal("initialization must remain buffered")
	}
	_, _ = _writer.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n"))
	if !strings.Contains(_recorder.Body.String(), "response.created") {
		t.Fatalf("committed response lost first event: %s", _recorder.Body.String())
	}
}

// -------------------------------------------------------------------------------------
func TestDeferredResponseWriterKeepsFailureDiscardableBeforeContent(t *testing.T) {
	_recorder := httptest.NewRecorder()
	_writer := newDeferredResponseWriter(_recorder, true)
	_writer.Header().Set("Content-Type", "text/event-stream")
	_writer.WriteHeader(http.StatusOK)
	_, _ = _writer.Write([]byte("event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"Selected model is at capacity\"}}}\n\n"))
	if _writer.Committed() || _recorder.Body.Len() != 0 {
		t.Fatal("pre-token failure must remain discardable")
	}
	_err := &proxy.ProviderStreamError{Message: "Selected model is at capacity", RetryableCapacity: true, ResponseForwarded: true}
	if !providerFailureCanRetryBeforeFirstToken(_err, _writer) {
		t.Fatal("capacity failure before first token should be retryable")
	}
}

// -------------------------------------------------------------------------------------
func TestDeferredResponseWriterDoesNotRetryAfterContent(t *testing.T) {
	_recorder := httptest.NewRecorder()
	_writer := newDeferredResponseWriter(_recorder, true)
	_, _ = _writer.Write([]byte("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking\"}}]}\n\n"))
	if !_writer.Committed() {
		t.Fatal("reasoning token should commit response")
	}
	if providerFailureCanRetryBeforeFirstToken(errors.New("connection reset"), _writer) {
		t.Fatal("committed response must never retry")
	}
}

// -------------------------------------------------------------------------------------
func TestDeferredResponseWriterDoesNotTreatHeadersAsForwardedContent(t *testing.T) {
	_recorder := httptest.NewRecorder()
	_writer := newDeferredResponseWriter(_recorder, true)
	_writer.Header().Set("Content-Type", "text/event-stream")
	_writer.WriteHeader(http.StatusOK)
	if _writer.HasBufferedResponse() {
		t.Fatal("headers without an SSE event must remain replaceable")
	}
	if !providerFailureCanRetryBeforeFirstToken(errors.New("unexpected EOF"), _writer) {
		t.Fatal("connection failure after headers but before content should retry")
	}
}

// -------------------------------------------------------------------------------------
func TestDeferredResponseWriterHeartbeatCommitsAndFlushes(t *testing.T) {
	_recorder := httptest.NewRecorder()
	_writer := newDeferredResponseWriter(_recorder, true)
	_writer.Header().Set("Content-Type", "text/event-stream")
	_writer.WriteHeader(http.StatusOK)

	if _err := _writer.WriteStreamHeartbeat([]byte(": keep-alive\n\n")); _err != nil {
		t.Fatalf("write heartbeat: %v", _err)
	}
	if !_writer.Committed() {
		t.Fatal("heartbeat must commit the downstream SSE response")
	}
	if _recorder.Body.String() != ": keep-alive\n\n" {
		t.Fatalf("heartbeat body = %q", _recorder.Body.String())
	}
	if !_recorder.Flushed {
		t.Fatal("heartbeat must flush the downstream response")
	}
}

// -------------------------------------------------------------------------------------
// 保活心跳會 commit（header 必須先送出去），但它不帶回應內容 ——
// 送過心跳之後仍然要能換帳號重試，否則「保持連線」與「還能重試」變成互斥。
func TestHeartbeatDoesNotForfeitRetry(t *testing.T) {
	_recorder := httptest.NewRecorder()
	_writer := newDeferredResponseWriter(_recorder, true)

	if _err := _writer.WriteStreamHeartbeat([]byte("event: response.ping\ndata: {}\n\n")); _err != nil {
		t.Fatal(_err)
	}
	if !_writer.Committed() {
		t.Fatalf("a heartbeat must commit so the client starts reading")
	}
	if _writer.ContentWritten() {
		t.Fatalf("a heartbeat carries no response content; retrying must stay possible")
	}
	if !_writer.ResetForGracefulTerminal() {
		t.Fatalf("a heartbeat-only stream must still be closeable with a terminal event")
	}

	// 真正的內容送出後就沒有退路了。
	if _, _err := _writer.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")); _err != nil {
		t.Fatal(_err)
	}
	if !_writer.ContentWritten() {
		t.Fatalf("real content must mark the response as delivered")
	}
	if _writer.ResetForGracefulTerminal() {
		t.Fatalf("delivered content cannot be reset")
	}
}

// -------------------------------------------------------------------------------------
// 緩衝中但尚未送出的內容不算「已送達」—— 它仍然可以被丟棄改用優雅結尾。
func TestBufferedContentIsNotDelivered(t *testing.T) {
	_recorder := httptest.NewRecorder()
	_writer := newDeferredResponseWriter(_recorder, true)

	_writer.WriteHeader(http.StatusInternalServerError)
	if _, _err := _writer.Write([]byte(`{"error":"boom"}`)); _err != nil {
		t.Fatal(_err)
	}
	if _writer.Committed() || _writer.ContentWritten() {
		t.Fatalf("an error body must stay buffered: committed=%v content=%v", _writer.Committed(), _writer.ContentWritten())
	}
	if !_writer.ResetForGracefulTerminal() {
		t.Fatalf("buffered content must be discardable")
	}
	if _recorder.Body.Len() != 0 {
		t.Fatalf("nothing should have reached the client yet: %q", _recorder.Body.String())
	}
}

// -------------------------------------------------------------------------------------
// 重試會建立新的 writer，但共用同一個底層 ResponseWriter；
// 心跳送過 header 之後，後續 writer 不能再送一次。
func TestAdoptCommittedPreventsDuplicateHeader(t *testing.T) {
	_recorder := httptest.NewRecorder()
	_first := newDeferredResponseWriter(_recorder, true)
	if _err := _first.WriteStreamHeartbeat([]byte("event: response.ping\ndata: {}\n\n")); _err != nil {
		t.Fatal(_err)
	}

	_second := newDeferredResponseWriter(_recorder, true)
	_second.AdoptCommitted()
	if !_second.Committed() {
		t.Fatalf("the retry writer must inherit the committed state")
	}
	if _second.ContentWritten() {
		t.Fatalf("inheriting headers must not imply content was delivered")
	}
	if _, _err := _second.Write([]byte("data: hello\n\n")); _err != nil {
		t.Fatal(_err)
	}
	if !strings.Contains(_recorder.Body.String(), "response.ping") || !strings.Contains(_recorder.Body.String(), "hello") {
		t.Fatalf("both the heartbeat and the real content should reach the client: %q", _recorder.Body.String())
	}
}
