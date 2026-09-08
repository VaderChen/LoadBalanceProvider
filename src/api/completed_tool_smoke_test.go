package api

import (
	"fmt"
	"net/http/httptest"
	"testing"
)

func TestCompletedResponseToolDeliverySmoke(t *testing.T) {
	for _, tc := range []struct {
		name, item string
		delivered  bool
	}{
		{"文字", `{"type":"message","content":[]}`, false},
		{"推理", `{"type":"reasoning","summary":[]}`, false},
		{"未完成工具", `{"type":"function_call","status":"in_progress","arguments":"{"}`, false},
		{"完整函式", `{"type":"function_call","status":"completed","call_id":"call_1","name":"exec","arguments":"{}"}`, true},
		{"完整自訂工具", `{"type":"custom_tool_call","call_id":"call_1","name":"apply_patch","input":"patch"}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := []byte(fmt.Sprintf("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[%s]}}\n\n", tc.item))
			for _, accepted := range []int{len(event) - 1, len(event)} {
				w := newDeferredResponseWriter(&limitedToolWriter{httptest.NewRecorder(), accepted}, true)
				_, _ = w.Write(event)
				want := tc.delivered && accepted == len(event)
				if w.ToolCallDelivered() != want {
					t.Fatalf("完整回應的工具交付判定錯誤: accepted=%d got=%t want=%t", accepted, w.ToolCallDelivered(), want)
				}
			}
		})
	}
}
