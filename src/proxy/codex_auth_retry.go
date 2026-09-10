package proxy

import (
	"context"
	"net/http"
	"strings"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/codexauth"
)

// 只在 HTTP 401 且尚未轉交任何上游回應時刷新一次，不重播已開始的串流。
func doCodexHTTPRequest(client *http.Client, req *http.Request, provider *balancer.ProviderRuntime, apiKey bool) (*http.Response, error) {
	return doCodexHTTPRequestWithRefresh(client, req, provider, apiKey, codexauth.RefreshAfterUnauthorized)
}

func doCodexHTTPRequestWithRefresh(client *http.Client, req *http.Request, provider *balancer.ProviderRuntime, apiKey bool, refresh func(context.Context, string, string) (codexauth.Auth, error)) (*http.Response, error) {
	if err := provider.Config.CheckScheduledDowntime(); err != nil {
		return nil, err
	}
	resp, err := doProviderHTTPRequest(client, req, providerStreamIdleTimeout(provider), provider.Config)
	if err != nil || resp.StatusCode != http.StatusUnauthorized || apiKey || req.GetBody == nil {
		return resp, err
	}
	auth, refreshErr := refresh(req.Context(), provider.Config.ID, strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer "))
	if refreshErr != nil {
		return resp, nil
	}
	resp.Body.Close()
	retry := req.Clone(req.Context())
	retry.Body, err = req.GetBody()
	if err != nil {
		return nil, err
	}
	retry.Header.Set("Authorization", "Bearer "+auth.AccessToken)
	if auth.AccountID != "" {
		retry.Header.Set("chatgpt-account-id", auth.AccountID)
	} else {
		retry.Header.Del("chatgpt-account-id")
	}
	if err := provider.Config.CheckScheduledDowntime(); err != nil {
		retry.Body.Close()
		return nil, err
	}
	return doProviderHTTPRequest(client, retry, providerStreamIdleTimeout(provider), provider.Config)
}
