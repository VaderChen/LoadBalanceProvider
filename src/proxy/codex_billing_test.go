package proxy

import "testing"

func TestSafeCodexInvoiceURL(t *testing.T) {
	for _, test := range []struct {
		url  string
		want bool
	}{
		{"https://invoice.stripe.com/i/example?s=ap", true},
		{"http://invoice.stripe.com/i/example", false},
		{"https://invoice.stripe.com.evil.example/i/example", false},
		{"https://invoice.stripe.com@evil.example/i/example", false},
		{"https://user:password@invoice.stripe.com/i/example", false},
		{"https://invoice.stripe.com:8443/i/example", false},
		{"https://invoice.stripe.com/other", false},
		{"javascript:alert(1)", false},
		{"/i/example", false},
		{"", false},
	} {
		t.Run(test.url, func(t *testing.T) {
			got := safeCodexInvoiceURL(test.url)
			if (got != "") != test.want || (test.want && got != test.url) {
				t.Fatalf("unexpected invoice URL validation result")
			}
		})
	}
}
