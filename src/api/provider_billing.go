package api

import (
	"net/http"
	"strings"

	"LoadBalanceProvider/src/auth"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/proxy"
)

func (_h *HTTPAPI) handleProviderBilling(_w http.ResponseWriter, _r *http.Request, _id string) {
	_w.Header().Set("Cache-Control", "no-store")
	// 帳單僅供管理登入查詢，不透過 MCP 的內部授權捷徑公開。
	_view, _ok := _r.Context().Value(requestAPIKeyContextKey{}).(auth.APIKeyView)
	if !_ok || !_view.Temporary || isMCPInternalRequest(_r) {
		_h.writeJSON(_w, http.StatusForbidden, domain.ErrorResponse("forbidden", "帳單查詢僅供管理頁面登入使用"))
		return
	}
	_provider, _ok := _h.findProviderConfig(strings.TrimSpace(_id))
	if !_ok {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "找不到 Provider"))
		return
	}
	if !strings.EqualFold(inferProviderKind(_provider), "openai-codex") {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("unsupported_provider", "帳單查詢僅支援 ChatGPT／Codex OAuth 來源"))
		return
	}
	_provider.Kind = "openai-codex"
	_cursor := _r.URL.Query().Get("cursor")
	if len(_cursor) > 4096 {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "帳單分頁游標過長"))
		return
	}
	_client := _h.Client
	if _client == nil {
		_client = proxy.NewClient()
	}
	_history, _err := _client.GetCodexBillingHistory(_r.Context(), &_provider, _cursor)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadGateway, domain.ErrorResponse("billing_error", _err.Error()))
		return
	}
	_h.writeJSON(_w, http.StatusOK, _history)
}
