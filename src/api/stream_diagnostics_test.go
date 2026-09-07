package api

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUpstreamHeartbeatPreservesRetryBoundary(t *testing.T) {
	for _, kind := range []string{"ping", "response.ping", "heartbeat", "keepalive", "keep-alive"} {
		for _, named := range []bool{false, true} {
			frame := "data: {\"type\":\"" + kind + "\"}\n\n"
			if named {
				frame = "event: " + kind + "\ndata: {}\n\n"
			}
			target := httptest.NewRecorder()
			writer := newDeferredResponseWriter(target, true)
			if _, err := writer.Write([]byte("data: {\"type\":\"response.created\"}\n\n" + frame)); err != nil {
				t.Fatal(err)
			}
			if err := writer.WriteStreamHeartbeat([]byte(": keep-alive\n\n")); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Write([]byte("data: {\"type\":\"error\",\"message\":\"upstream failed\"}\n\n")); err != nil {
				t.Fatal(err)
			}
			if writer.ContentWritten() || writer.FirstCommitEvent() != "" || target.Body.String() != ": keep-alive\n\n" {
				t.Fatalf("%s named=%t: heartbeat or failure escaped content buffer", kind, named)
			}
			// 下一次嘗試沿用下游連線，真正的工具內容仍必須觸發不可重播邊界。
			retry := newDeferredResponseWriter(target, true)
			retry.AdoptCommitted()
			delta := "data: {\"type\":\"response.function_call_arguments.delta\",\"delta\":\"command\"}\n\n"
			if _, err := retry.Write([]byte(delta)); err != nil {
				t.Fatal(err)
			}
			if !retry.ContentWritten() || retry.FirstCommitEvent() != "response.function_call_arguments.delta" || !strings.Contains(target.Body.String(), "command") {
				t.Fatal("tool content did not commit after heartbeat-only attempt")
			}
			if strings.Contains(target.Body.String(), "upstream failed") {
				t.Fatal("failed attempt leaked into retry")
			}
		}
	}
}

func TestNamedSSEBootstrapAndMultilineDelta(t *testing.T) {
	initial := "event: response.in_progress\ndata: {}\n\n"
	if streamBufferHasForwardableEvent([]byte(initial)) {
		t.Fatal("named bootstrap forfeited retry")
	}
	delta := "event: response.output_text.delta\ndata: {\ndata: \"delta\":\"hello\"}\n\n"
	if got := streamBufferForwardableEvent([]byte(initial + delta)); got != "response.output_text.delta" {
		t.Fatalf("got %q", got)
	}
	if streamBufferHasForwardableEvent([]byte("event: error\ndata: {\"message\":\"failed\"}\n\n")) {
		t.Fatal("named failure became content")
	}
}

func TestStreamPlaceholderBoundary(t *testing.T) {
	initial := "data: {\"type\":\"response.reasoning_summary_part.added\",\"part\":{\"type\":\"summary_text\",\"text\":\"\"}}\n\n"
	if streamBufferHasForwardableEvent([]byte(initial)) {
		t.Fatal("empty part must remain buffered")
	}
	delta := "data: {\"type\":\"response.custom_tool_call_input.delta\",\"delta\":\"command\"}\n\n"
	if got := streamBufferForwardableEvent([]byte(initial + delta)); got != "response.custom_tool_call_input.delta" {
		t.Fatalf("got %q", got)
	}
	if streamDiagnosticEventType("private text") != "other-event" {
		t.Fatal("unknown event leaked")
	}
	if streamDiagnosticEventType("response.new_protocol_event") != "response.new_protocol_event" {
		t.Fatal("unknown protocol event name was lost")
	}
	if streamDiagnosticEventType("response.event\nprivate text") != "other-event" {
		t.Fatal("invalid diagnostic label accepted")
	}
}
