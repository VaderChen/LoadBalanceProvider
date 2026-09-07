package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"LoadBalanceProvider/src/auth"
)

func TestBillingRouteRequiresManagementSession(t *testing.T) {
	for _, route := range []string{"/api/provider-configs/provider/billing", "/v1/provider-configs/provider/billing"} {
		for _, keyType := range []string{auth.APIKeyTypeChat, auth.APIKeyTypeMCP} {
			if canAccessRoute(auth.APIKeyView{KeyType: keyType}, false, http.MethodGet, route) {
				t.Fatal("API or MCP key must not access billing")
			}
		}
		if canAccessRoute(auth.APIKeyView{Temporary: true}, false, http.MethodGet, route) {
			t.Fatal("management session must come from a cookie")
		}
		if !canAccessRoute(auth.APIKeyView{Temporary: true}, true, http.MethodGet, route) {
			t.Fatal("management cookie should access billing")
		}
	}
}

func TestBillingHandlerWithoutManagementContextDoesNotFetch(t *testing.T) {
	handler := &HTTPAPI{}
	response := httptest.NewRecorder()
	handler.handleProviderBilling(response, httptest.NewRequest(http.MethodGet, "/api/provider-configs/provider/billing", nil), "provider")
	if response.Code != http.StatusForbidden || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("unexpected billing response: status=%d cache=%q", response.Code, response.Header().Get("Cache-Control"))
	}
}
