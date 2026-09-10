package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/proxy"
)

// 僅供管理介面診斷，不經正式對話的排程、冷卻、重試與回應改寫。
func (h *HTTPAPI) handleDiagnosticChat(w http.ResponseWriter, r *http.Request, body []byte) {
	w.Header().Set("X-Diagnostic-Origin", "proxy")
	w.Header().Set("Cache-Control", "no-store")
	var chat domain.ChatCompletionRequest
	if json.Unmarshal(body, &chat) != nil || strings.TrimSpace(chat.Model) == "" || len(chat.Messages) == 0 {
		h.writeJSON(w, http.StatusBadRequest, domain.ErrorResponse("diagnostic_validation_error", "請選擇來源、模型並輸入訊息"))
		return
	}
	provider, ok := h.findProviderConfig(chat.ProviderID)
	if !ok {
		h.writeJSON(w, http.StatusBadRequest, domain.ErrorResponse("diagnostic_validation_error", "來源不存在"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	resp, err := h.Client.OpenDiagnosticChat(ctx, provider, chat)
	if err != nil {
		// 連線錯誤可能含 URL 或認證細節，不冒充上游回覆，也不傳出敏感資料。
		h.writeJSON(w, http.StatusBadGateway, domain.ErrorResponse("diagnostic_transport_error", "代理未取得上游 HTTP 回應：認證、連線或逾時錯誤"))
		return
	}
	defer resp.Body.Close()
	w.Header().Set("X-Diagnostic-Origin", "upstream")
	for _, name := range []string{"Content-Type", "Retry-After", "X-Request-ID", "Request-ID", "OpenAI-Request-ID"} {
		if value := resp.Header.Get(name); value != "" {
			w.Header().Set(name, value)
		}
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(resp.StatusCode)
	if proxy.DownstreamError(proxy.FlushResponseWriter(w)) != nil {
		return
	}
	buffer := make([]byte, 32*1024)
	for {
		n, readErr := resp.Body.Read(buffer)
		if n > 0 {
			if _, err = w.Write(buffer[:n]); err != nil {
				return
			}
			if proxy.DownstreamError(proxy.FlushResponseWriter(w)) != nil {
				return
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				// 已送出標頭後不能再插入 JSON 或完成事件，讓客戶端辨識未完成串流。
				cancel()
			}
			return
		}
	}
}
