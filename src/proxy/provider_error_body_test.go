package proxy

import (
	"io"
	"strings"
	"testing"
)

func TestProviderErrorBodyCapturePreservesForwarding(t *testing.T) {
	raw := strings.Repeat("x", 100000)
	var capture providerErrorBodyCapture
	forwarded, err := io.ReadAll(io.TeeReader(strings.NewReader(raw), &capture))
	if err != nil || string(forwarded) != raw || len(capture.body) != 64*1024 {
		t.Fatalf("forwarding or capture failed: %v, captured=%d", err, len(capture.body))
	}
}

func TestEnrichFailureRetainsNonstandardErrorMessage(t *testing.T) {
	for _, body := range []string{`{"detail":"unsupported size"}`, `upstream rejected image`} {
		status := &ProviderStatusError{StatusCode: 400, Message: body}
		EnrichFailure(status, body)
		if status.Message != body {
			t.Fatalf("lost fallback message: %q", status.Message)
		}
	}
	status := &ProviderStatusError{StatusCode: 400}
	EnrichFailure(status, `{"error":{"code":"invalid_size","message":"size is unsupported"}}`)
	if status.Message != "size is unsupported" || status.Code != "invalid_size" {
		t.Fatalf("missing structured error: %+v", status)
	}
}
