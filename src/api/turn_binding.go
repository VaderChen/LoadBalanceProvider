package api

import (
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/proxy"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

type turnGateHeadersKey struct{}

type turnBindingError struct {
	message string
	missing bool
}

func (e *turnBindingError) Error() string { return e.message }
func turnError(message string) error      { return &turnBindingError{message: message} }

// 回合 ID 由呼叫端提供；缺省時固定整段對話，不用文字或歷史猜測新回合。
func responseTurnRoute(body []byte, r *http.Request) string {
	turn := strings.TrimSpace(r.Header.Get("X-Proxy-Turn-ID"))
	conversation := proxy.PromptCacheKeyFromBody(body)
	if turn == "" && conversation == "" {
		return ""
	}
	return fmt.Sprintf("turn-binding:%x", sha256.Sum256([]byte(proxy.ResponseRouteOwner(r)+"\x00"+conversation+"\x00"+turn)))
}

func (h *HTTPAPI) applyTurnBinding(req *domain.ChatCompletionRequest, body []byte, r *http.Request) (string, error) {
	key := responseTurnRoute(body, r)
	forced := strings.TrimSpace(req.ProviderID)
	if forced == "" {
		forced = strings.TrimSpace(req.Provider)
	}
	var target proxy.ResponseRouteTarget
	var found bool
	if key != "" {
		var err error
		target, found, err = h.lookupDurableTurnRoute(key, proxy.ResponseRouteOwner(r))
		if err != nil {
			return "", err
		}
	}
	// 增量請求仍依賴原帳號的伺服器狀態；不能刪除 response ID 假裝完整歷史。
	previous := previousResponseIDFromBody(body)
	if previous != "" {
		prior, ok, err := h.lookupDurableTurnRoute(previous, proxy.ResponseRouteOwner(r))
		if err != nil {
			return "", err
		}
		if !ok {
			return "", turnError("前文來源已遺失或到期，請提供完整歷史並開始新的提問")
		}
		if found && (target.ProviderID != prior.ProviderID || target.Model != prior.Model) {
			return "", turnError("回合綁定與前文來源衝突，不能跨 Provider 接續")
		}
		target, found = prior, true
	}
	if !found && (bodyCarriesConversationContinuity(body) || bodyEndsWithToolResult(body)) {
		legacy := proxy.PromptCacheRouteID(proxy.PromptCacheKeyFromBody(body))
		if legacy != "" {
			var err error
			target, found, err = h.lookupDurableTurnRoute(legacy, proxy.ResponseRouteOwner(r))
			if err != nil {
				return "", err
			}
		}
		if !found && forced == "" {
			return "", &turnBindingError{message: "原回合綁定無法確認", missing: true}
		}
	}
	if !found {
		return key, nil
	}
	if forced != "" && !providerReferenceMatchesTarget(h.Balancer, target.ProviderID, forced) {
		return "", turnError("本回合已固定 Provider，不能中途變更路由")
	}
	req.ProviderID, req.Provider, req.Model = target.ProviderID, target.ProviderID, target.Model
	return key, nil
}

func (h *HTTPAPI) bindTurnBeforeDispatch(key string, r *http.Request, provider, model string) error {
	if key == "" {
		return nil
	}
	target := proxy.ResponseRouteTarget{ProviderID: provider, Model: model, Owner: proxy.ResponseRouteOwner(r)}
	routes := map[string]proxy.ResponseRouteTarget{key: target}
	if recovered, _ := r.Context().Value(turnRecoveryContextKey{}).(bool); recovered {
		routes["recovered:"+key] = target
	}
	if err := h.saveTurnRoutes(routes); err != nil {
		return turnError("回合綁定無法持久化，尚未送往上游：" + err.Error())
	}
	if !h.Client.ClaimTurnRoute(key, target) {
		return turnError("本回合已有其他 Provider 綁定，拒絕跨來源送出")
	}
	return nil
}

type turnGate struct {
	token chan struct{}
	refs  int
}

// 同一回合先取得閘門再選來源；等待可隨請求取消，離開後回收閘門。
func (h *HTTPAPI) acquireTurnGate(key string, r *http.Request, heartbeat ...func() error) (func(), error) {
	if key == "" {
		return func() {}, nil
	}
	h.turnGateLock.Lock()
	if h.turnGates == nil {
		h.turnGates = make(map[string]*turnGate)
	}
	gate := h.turnGates[key]
	if gate == nil {
		gate = &turnGate{token: make(chan struct{}, 1)}
		h.turnGates[key] = gate
	}
	gate.refs++
	h.turnGateLock.Unlock()
	drop := func() {
		h.turnGateLock.Lock()
		gate.refs--
		if gate.refs == 0 {
			delete(h.turnGates, key)
		}
		h.turnGateLock.Unlock()
	}
	var ticks <-chan time.Time
	if len(heartbeat) > 0 && heartbeat[0] != nil {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		ticks = ticker.C
	}
	for {
		select {
		case gate.token <- struct{}{}:
			var once sync.Once
			return func() { once.Do(func() { <-gate.token; drop() }) }, nil
		case <-r.Context().Done():
			drop()
			return nil, r.Context().Err()
		case <-ticks:
			if err := heartbeat[0](); err != nil {
				drop()
				return nil, err
			}
		}
	}
}
