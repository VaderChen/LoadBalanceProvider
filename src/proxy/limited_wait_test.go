package proxy

import (
	"io"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRateLimitResetDetails(t *testing.T) {
	now := time.Unix(1800000000, 0)
	for _, payload := range []map[string]interface{}{
		{"code": "rate_limit_exceeded", "resets_at": now.Add(time.Hour).Unix()},
		{"type": "rate_limit_exceeded", "resets_in_seconds": "3600"},
		{"code": "other", "type": "rate_limit_exceeded", "resets_in_seconds": 3600},
	} {
		details := failureDetailsFromPayload(payload, now)
		if details.RetryAfter != time.Hour {
			t.Fatalf("reset not retained: %+v", details)
		}
		payload["retry_after"] = 7200
		if details = failureDetailsFromPayload(payload, now); details.RetryAfter != 2*time.Hour {
			t.Fatalf("longer retry-after lost: %+v", details)
		}
	}
}

func TestDownstreamHeartbeatDoesNotResetUpstreamActivity(t *testing.T) {
	r, w := io.Pipe()
	defer w.Close()
	idle := newStreamIdleTimeoutReader(r, time.Minute)
	defer idle.Close()
	before := time.Now().Add(-time.Second).UnixNano()
	idle.lastActivity.Store(before)
	downstream := httptest.NewRecorder()
	for _, heartbeat := range [][]byte{ChatStreamHeartbeat(), ResponsesStreamHeartbeat()} {
		if _, err := downstream.Write(heartbeat); err != nil {
			t.Fatal(err)
		}
	}
	if idle.lastActivity.Load() != before {
		t.Fatal("downstream heartbeat changed upstream activity")
	}
	markStreamActivity(idle, "event: keepalive\ndata: {\"type\":\"keepalive\"}\n\n")
	if idle.lastActivity.Load() <= before {
		t.Fatal("upstream heartbeat did not reset activity")
	}
}
