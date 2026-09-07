package proxy

import "testing"

func TestCustomToolInputDeltaMetrics(t *testing.T) {
	parts := streamResponseParts(map[string]interface{}{"type": "response.custom_tool_call_input.delta", "delta": "command"})
	if parts.Tool != "command" {
		t.Fatalf("tool delta not counted: %#v", parts)
	}
}
