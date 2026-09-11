package proxy

import (
	"context"
	"net/http"
)

type verifiedTurnStateProviderKey struct{}

// 只能由已驗證呼叫者擁有之持久化路由授權，不採信客戶端自報的 Provider ID。
func WithVerifiedTurnStateProvider(ctx context.Context, providerID string) context.Context {
	return context.WithValue(ctx, verifiedTurnStateProviderKey{}, providerID)
}

func copyVerifiedCodexTurnState(source, target *http.Request, providerID string) {
	if source == nil || target == nil || providerID == "" {
		return
	}
	verified, _ := source.Context().Value(verifiedTurnStateProviderKey{}).(string)
	if verified == providerID {
		if value := source.Header.Get("X-Codex-Turn-State"); value != "" {
			target.Header.Set("X-Codex-Turn-State", value)
		}
	}
}
