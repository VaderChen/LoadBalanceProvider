package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"LoadBalanceProvider/src/codexauth"
	"LoadBalanceProvider/src/domain"
)

type CodexBillingTransaction struct {
	Type       string    `json:"type"`
	ID         string    `json:"id"`
	CreatedAt  time.Time `json:"created_at"`
	Amount     int64     `json:"amount"`
	Currency   string    `json:"currency"`
	Status     string    `json:"status"`
	InvoiceURL string    `json:"invoice_url"`
	Product    struct {
		Type string `json:"type"`
		Plan string `json:"plan"`
	} `json:"product"`
}

type CodexBillingHistory struct {
	Transactions []CodexBillingTransaction `json:"transactions"`
	NextCursor   string                    `json:"next_cursor"`
}

func (_c *Client) GetCodexBillingHistory(_ctx context.Context, _provider *domain.LLMProviderConfig, _cursor string) (CodexBillingHistory, error) {
	if !isOpenAICodexProviderConfig(_provider) {
		return CodexBillingHistory{}, fmt.Errorf("帳單查詢僅支援 ChatGPT／Codex OAuth 來源")
	}
	_ctx, _cancel := context.WithTimeout(_ctx, 20*time.Second)
	defer _cancel()
	_auth, _err := codexauth.EnsureContext(_ctx, _provider.ID)
	if _err != nil || strings.TrimSpace(_auth.AccessToken) == "" || strings.TrimSpace(_auth.AccountID) == "" {
		return CodexBillingHistory{}, fmt.Errorf("無法取得帳單查詢認證，請重新連線 OAuth")
	}
	_query := url.Values{"account_id": {_auth.AccountID}, "limit": {"4"}}
	if _cursor != "" {
		_query.Set("cursor", _cursor)
	}
	// 帳務認證僅送往官方站台，不使用可自訂的 Provider HOST 或跟隨轉址。
	_req, _err := http.NewRequestWithContext(_ctx, http.MethodGet, "https://chatgpt.com/backend-api/payments/transaction-history?"+_query.Encode(), nil)
	if _err != nil {
		return CodexBillingHistory{}, fmt.Errorf("無法建立帳單查詢")
	}
	_req.Header.Set("Authorization", "Bearer "+_auth.AccessToken)
	_req.Header.Set("ChatGPT-Account-Id", _auth.AccountID)
	_req.Header.Set("Accept", "application/json")
	_req.Header.Set("User-Agent", defaultCodexUpstreamUserAgent)
	_client := *usageRefreshHTTPClient(_c)
	_client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	_resp, _err := _client.Do(_req)
	if _err != nil {
		return CodexBillingHistory{}, fmt.Errorf("帳單查詢連線失敗或逾時，請稍後重試")
	}
	defer _resp.Body.Close()
	if _resp.StatusCode != http.StatusOK {
		return CodexBillingHistory{}, fmt.Errorf("上游帳單查詢失敗（HTTP %d）", _resp.StatusCode)
	}
	const _maxBody = 2 * 1024 * 1024
	_raw, _err := io.ReadAll(io.LimitReader(_resp.Body, _maxBody+1))
	if _err != nil || len(_raw) > _maxBody {
		return CodexBillingHistory{}, fmt.Errorf("帳單回應不完整或超過大小限制")
	}
	var _history CodexBillingHistory
	if json.Unmarshal(_raw, &_history) != nil || _history.Transactions == nil {
		return CodexBillingHistory{}, fmt.Errorf("上游帳單回應格式不正確")
	}
	for _i := range _history.Transactions {
		_history.Transactions[_i].InvoiceURL = safeCodexInvoiceURL(_history.Transactions[_i].InvoiceURL)
	}
	sort.SliceStable(_history.Transactions, func(_i, _j int) bool {
		return _history.Transactions[_i].CreatedAt.After(_history.Transactions[_j].CreatedAt)
	})
	return _history, nil
}

func safeCodexInvoiceURL(_raw string) string {
	_url, _err := url.Parse(_raw)
	if _err != nil || _url.Scheme != "https" || _url.Host != "invoice.stripe.com" || _url.User != nil || !strings.HasPrefix(_url.Path, "/i/") {
		return ""
	}
	return _url.String()
}
