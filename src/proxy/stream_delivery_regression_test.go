package proxy

import (
	"net/http"
	"testing"
	"time"
)

func TestStreamTerminalRequiresSSEMarker(t *testing.T) {
	for _, event := range []string{
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"use [DONE] in code\"}\n\n",
		"data: {\"type\":\"response.function_call_arguments.delta\",\"delta\":\"[DONE]\"}\n\n",
	} {
		if streamEventIsTerminalMarker(event) {
			t.Fatalf("正文被當成結束事件: %s", event)
		}
	}
	if !streamEventIsTerminalMarker("data: [DONE]\r\n\r\n") {
		t.Fatal("未辨識結束標記")
	}
	if !streamEventIsTerminalMarker("event: response.completed\ndata: {\"type\":\ndata: \"response.completed\"}\n\n") {
		t.Fatal("未辨識多行 SSE 資料")
	}
}

func TestStreamFailureHonorsLongerHeaderRetryAfter(t *testing.T) {
	failure := &ProviderStreamError{FailureDetails: FailureDetails{RetryAfter: 2 * time.Second}}
	retainStreamRetryAfter(failure, http.Header{"Retry-After": {"60"}})
	if failure.RetryAfter != time.Minute {
		t.Fatal("未保留上游 HTTP 等待提示")
	}
	retainStreamRetryAfter(failure, http.Header{"Retry-After": {"1"}})
	if failure.RetryAfter != time.Minute {
		t.Fatal("縮短了原本的冷卻時間")
	}
}
