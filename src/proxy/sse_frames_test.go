package proxy

import "testing"

func TestNamedMultilineSSEPayload(t *testing.T) {
	event := "event: response.output_text.delta\r\ndata: {\r\ndata: \"delta\":\"hello\"}\r\n\r\n"
	payloads := responseEventPayloads(event)
	if len(payloads) != 1 || payloads[0]["type"] != "response.output_text.delta" || payloads[0]["delta"] != "hello" {
		t.Fatalf("unexpected payloads: %#v", payloads)
	}
	if !streamEventMetrics(event).ContentSeen {
		t.Fatal("named multiline content was not counted")
	}
	if len(ParseSSEDataFrames(": heartbeat\n\n")) != 0 {
		t.Fatal("comment became a data event")
	}
}
