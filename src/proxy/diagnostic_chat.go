package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"

	"LoadBalanceProvider/src/codexauth"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/security"
)

// OpenDiagnosticChat 僅做必要的請求格式與認證轉換，保留上游原始回應，不做重試。
func (c *Client) OpenDiagnosticChat(ctx context.Context, provider domain.LLMProviderConfig, chat domain.ChatCompletionRequest) (*http.Response, error) {
	if err := provider.CheckScheduledDowntime(); err != nil {
		return nil, err
	}
	token := strings.TrimSpace(provider.APIKey)
	if token == "" {
		token = strings.TrimSpace(os.Getenv(provider.APIKeyEnv))
	}
	accountID := ""
	useAPIKey := token != ""
	chat.ProviderID, chat.Provider = "", ""
	chat.Stream = true
	var payload interface{} = chat
	target := provider.ChatURL()
	codex := isOpenAICodexProviderConfig(&provider)
	if codex {
		if !useAPIKey {
			auth, err := codexauth.EnsureContext(ctx, provider.ID)
			if err != nil {
				return nil, err
			}
			token, accountID = auth.AccessToken, auth.AccountID
		}
		request := buildCodexResponsesRequestWithDefaults(&chat, codexUpstreamModelName(chat.Model), &provider, false)
		// 保留協定欄位，但不為診斷對話加入預設指令。
		payload = struct {
			codexResponsesRequest
			Instructions string `json:"instructions"`
		}{request, request.Instructions}
		target = codexResponsesURL(provider, useAPIKey)
	}
	if err := security.ValidateOutboundURL(target); err != nil {
		return nil, err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if codex {
		applyCodexUpstreamHeaders(nil, req, false)
		if accountID != "" {
			req.Header.Set("chatgpt-account-id", accountID)
		}
	}
	client := http.Client{}
	if c.HTTPClient != nil {
		client = *c.HTTPClient
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if err := provider.CheckScheduledDowntime(); err != nil {
		return nil, err
	}
	return dispatchProviderHTTP(&client, req, &provider)
}
