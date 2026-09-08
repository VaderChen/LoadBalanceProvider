package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// -------------------------------------------------------------------------------------
type responseRouteOwnerContextKey struct{}

// WithResponseRouteOwner 記錄「已通過驗證」的金鑰身分，作為 Response 資源的擁有者。
func WithResponseRouteOwner(_ctx context.Context, _owner string) context.Context {
	_owner = strings.TrimSpace(_owner)
	if _owner == "" {
		return _ctx
	}
	return context.WithValue(_ctx, responseRouteOwnerContextKey{}, _owner)
}

const promptCacheRoutePrefix = "prompt-cache:"

// PromptCacheRouteID 把 Codex 的 prompt_cache_key 轉成 route store 的鍵。
// Codex 在多輪工具呼叫時不一定會送 previous_response_id，但每一輪都固定帶同一個
// prompt_cache_key，而輸入又含有綁定帳號的加密推理內容 —— 因此必須以它維持
// 同一個 provider，否則換帳號會被上游拒絕。
func PromptCacheRouteID(_key string) string {
	_key = strings.TrimSpace(_key)
	if _key == "" {
		return ""
	}
	return promptCacheRoutePrefix + _key
}

// -------------------------------------------------------------------------------------
func PromptCacheKeyFromBody(_body []byte) string {
	if len(_body) == 0 {
		return ""
	}
	var _payload map[string]interface{}
	if _err := json.Unmarshal(_body, &_payload); _err != nil {
		return ""
	}
	_key, _ := _payload["prompt_cache_key"].(string)
	return strings.TrimSpace(_key)
}

// -------------------------------------------------------------------------------------
// RecordPromptCacheRoute 以「滑動 TTL」記錄 prompt_cache_key 對應的 provider。
// response id 每輪都是新的，用建立時間計算 TTL 沒問題；但 prompt_cache_key 在整段
// 對話中固定不變，若沿用首次建立時間，長對話會在進行中突然失去黏著。
func (_c *Client) RecordPromptCacheRoute(_routeID string, _providerID string, _model string, _owner string) {
	if _c == nil {
		return
	}
	_routeID = strings.TrimSpace(_routeID)
	_providerID = strings.TrimSpace(_providerID)
	if _routeID == "" || _providerID == "" {
		return
	}
	_now := time.Now()
	_target := ResponseRouteTarget{
		ProviderID: _providerID,
		Model:      strings.TrimSpace(_model),
		Owner:      strings.TrimSpace(_owner),
		CreatedAt:  _now,
	}
	_c.responseRouteMutationLock.Lock()
	if value, ok := _c.ResponseRoutes.Load(_routeID); ok {
		if prior, valid := value.(ResponseRouteTarget); valid && prior.ProviderID == _target.ProviderID && prior.Model == _target.Model && prior.Owner == _target.Owner {
			_target.Persisted = prior.Persisted
		}
	}
	if _, _loaded := _c.ResponseRoutes.Swap(_routeID, _target); !_loaded {
		atomic.AddInt64(&_c.responseRouteCount, 1)
	}
	_c.responseRouteMutationLock.Unlock()
	if atomic.LoadInt64(&_c.responseRouteCount) > int64(_c.responseRouteMaxEntriesValue()) {
		_c.pruneResponseRoutes(_now, true)
	}
}

// 只標記剛寫入資料庫的相同配對，保留快照內容與快取計數。
func (c *Client) MarkResponseRoutePersisted(route string, target ResponseRouteTarget) {
	if c == nil {
		return
	}
	c.responseRouteMutationLock.Lock()
	defer c.responseRouteMutationLock.Unlock()
	value, ok := c.ResponseRoutes.Load(route)
	prior, valid := value.(ResponseRouteTarget)
	if ok && valid && prior.ProviderID == target.ProviderID && prior.Model == target.Model && prior.Owner == target.Owner {
		prior.Persisted = true
		c.ResponseRoutes.Store(route, prior)
	}
}

// -------------------------------------------------------------------------------------
// PromptCacheRouteCounts 統計各 provider 目前綁定了多少段對話（prompt_cache_key）。
// 只計 prompt-cache 命名空間：response id 每輪都會新增一筆，拿來當「綁定數」會失真。
func (_c *Client) PromptCacheRouteCounts() map[string]int {
	_counts := map[string]int{}
	if _c == nil {
		return _counts
	}
	_now := time.Now()
	_ttl := _c.responseRouteTTLValue()
	_c.ResponseRoutes.Range(func(_key interface{}, _value interface{}) bool {
		_id, _ok := _key.(string)
		if !_ok || !strings.HasPrefix(_id, promptCacheRoutePrefix) {
			return true
		}
		_target, _ok := _value.(ResponseRouteTarget)
		if !_ok || strings.TrimSpace(_target.ProviderID) == "" {
			return true
		}
		if !_target.CreatedAt.IsZero() && _now.Sub(_target.CreatedAt) > _ttl {
			return true
		}
		_counts[_target.ProviderID]++
		return true
	})
	return _counts
}

// -------------------------------------------------------------------------------------
func (_c *Client) RecordResponseRoute(_responseID string, _providerID string, _model string) {
	_c.RecordResponseSnapshot(_responseID, _providerID, _model, nil, nil)
}

func (_c *Client) RecordResponseSnapshot(_responseID string, _providerID string, _model string, _input interface{}, _response map[string]interface{}) {
	_c.RecordResponseSnapshotForOwner(_responseID, _providerID, _model, "", _input, _response)
}

func (_c *Client) RecordResponseSnapshotForOwner(_responseID string, _providerID string, _model string, _owner string, _input interface{}, _response map[string]interface{}) {
	if _c == nil {
		return
	}
	_responseID = strings.TrimSpace(_responseID)
	_providerID = strings.TrimSpace(_providerID)
	if _responseID == "" || _providerID == "" {
		return
	}
	_now := time.Now()
	_c.pruneResponseRoutes(_now, false)
	_owner = strings.TrimSpace(_owner)
	_next := ResponseRouteTarget{ProviderID: _providerID, Model: strings.TrimSpace(_model), Owner: _owner, CreatedAt: _now}
	if _existing, _ok := _c.LookupResponseRouteForOwner(_responseID, _owner); _ok {
		_next = _existing
		_next.ProviderID = firstNonEmpty(_providerID, _existing.ProviderID)
		_next.Model = firstNonEmpty(strings.TrimSpace(_model), _existing.Model)
		_next.Owner = firstNonEmpty(_owner, _existing.Owner)
	}
	if _input != nil {
		_next.Input = cloneJSONValue(_input)
	}
	if _response != nil {
		_next.Response = cloneJSONMap(_response)
	}
	if _next.CreatedAt.IsZero() {
		_next.CreatedAt = _now
	}
	_c.responseRouteMutationLock.Lock()
	if _, _loaded := _c.ResponseRoutes.Swap(_responseID, _next); !_loaded {
		atomic.AddInt64(&_c.responseRouteCount, 1)
	}
	_c.responseRouteMutationLock.Unlock()
	if atomic.LoadInt64(&_c.responseRouteCount) > int64(_c.responseRouteMaxEntriesValue()) {
		_c.pruneResponseRoutes(_now, true)
	}
}

// ConfigureResponseRouteCache updates the in-memory Responses route policy at runtime.
// Existing entries are re-evaluated against the new TTL during the next lookup/sweep.
func (_c *Client) ConfigureResponseRouteCache(_ttl time.Duration, _maxEntries int) {
	if _c == nil {
		return
	}
	if _ttl <= 0 {
		_ttl = defaultResponseRouteTTL
	}
	if _maxEntries <= 0 {
		_maxEntries = defaultMaxResponseRoutes
	}
	_c.responseRouteTTLNanos.Store(int64(_ttl))
	_c.responseRouteMaxEntries.Store(int64(_maxEntries))
	_c.pruneResponseRoutes(time.Now(), true)
}

func (_c *Client) responseRouteTTLValue() time.Duration {
	if _c == nil {
		return defaultResponseRouteTTL
	}
	_value := time.Duration(_c.responseRouteTTLNanos.Load())
	if _value <= 0 {
		return defaultResponseRouteTTL
	}
	return _value
}

func (_c *Client) responseRouteMaxEntriesValue() int {
	if _c == nil {
		return defaultMaxResponseRoutes
	}
	_value := int(_c.responseRouteMaxEntries.Load())
	if _value <= 0 {
		return defaultMaxResponseRoutes
	}
	return _value
}

func (_c *Client) LookupResponseRoute(_responseID string) (ResponseRouteTarget, bool) {
	return _c.LookupResponseRouteForOwner(_responseID, "")
}

func (_c *Client) LookupResponseRouteForOwner(_responseID string, _owner string) (ResponseRouteTarget, bool) {
	if _c == nil {
		return ResponseRouteTarget{}, false
	}
	_now := time.Now()
	_c.pruneResponseRoutes(_now, false)
	_responseID = strings.TrimSpace(_responseID)
	_value, _ok := _c.ResponseRoutes.Load(_responseID)
	if !_ok {
		return ResponseRouteTarget{}, false
	}
	_target, _ok := _value.(ResponseRouteTarget)
	if !_ok {
		return ResponseRouteTarget{}, false
	}
	if !_target.CreatedAt.IsZero() && _now.Sub(_target.CreatedAt) > _c.responseRouteTTLValue() {
		_c.deleteResponseRoute(_responseID)
		return ResponseRouteTarget{}, false
	}
	_owner = strings.TrimSpace(_owner)
	if _owner != "" && _target.Owner != "" && _target.Owner != _owner {
		return ResponseRouteTarget{}, false
	}
	return _target, true
}

// ResponseRouteOwner 只採用驗證流程寫入 context 的金鑰身分。
// 先前是重新解析 Header/Cookie 推導擁有者，有三個問題：
//  1. 取的是「未經驗證」的值，任何人送出相同 X-API-Key 字串就會落在同一個 owner；
//  2. 優先序（X-API-Key 優先）與實際驗證順序（Authorization 優先）相反；
//  3. cookie 退路取的是第一個非空 cookie，可能是與登入無關的任意 cookie。
func ResponseRouteOwner(_request *http.Request) string {
	if _request == nil {
		return "anonymous"
	}
	if _owner, _ok := _request.Context().Value(responseRouteOwnerContextKey{}).(string); _ok {
		if _owner = strings.TrimSpace(_owner); _owner != "" {
			return _owner
		}
	}
	return "anonymous"
}

func (_c *Client) DeleteResponseRoute(_responseID string) {
	if _c == nil {
		return
	}
	_responseID = strings.TrimSpace(_responseID)
	if _responseID != "" {
		_c.deleteResponseRoute(_responseID)
	}
}

func (_c *Client) deleteResponseRoute(_responseID string) {
	_c.responseRouteMutationLock.Lock()
	defer _c.responseRouteMutationLock.Unlock()
	if _, _loaded := _c.ResponseRoutes.LoadAndDelete(strings.TrimSpace(_responseID)); _loaded {
		atomic.AddInt64(&_c.responseRouteCount, -1)
	}
}

func (_c *Client) pruneResponseRoutes(_now time.Time, _force bool) {
	if _c == nil {
		return
	}
	_nowNanos := _now.UnixNano()
	_lastSweep := atomic.LoadInt64(&_c.responseRouteLastSweepAt)
	if !_force && _lastSweep > 0 && _now.Sub(time.Unix(0, _lastSweep)) < responseRouteSweepInterval {
		return
	}
	if !atomic.CompareAndSwapInt64(&_c.responseRouteLastSweepAt, _lastSweep, _nowNanos) {
		return
	}

	type routeAge struct {
		id        string
		createdAt time.Time
	}
	_ttl := _c.responseRouteTTLValue()
	_maxEntries := _c.responseRouteMaxEntriesValue()
	_routes := make([]routeAge, 0, _maxEntries+1)
	_c.ResponseRoutes.Range(func(_key interface{}, _value interface{}) bool {
		_id, _idOK := _key.(string)
		_target, _targetOK := _value.(ResponseRouteTarget)
		if !_idOK || !_targetOK || (!_target.CreatedAt.IsZero() && _now.Sub(_target.CreatedAt) > _ttl) {
			if _idOK {
				_c.deleteResponseRoute(_id)
			}
			return true
		}
		_routes = append(_routes, routeAge{id: _id, createdAt: _target.CreatedAt})
		return true
	})
	if len(_routes) <= _maxEntries {
		return
	}
	sort.Slice(_routes, func(_i, _j int) bool {
		return _routes[_i].createdAt.Before(_routes[_j].createdAt)
	})
	for _idx := 0; _idx < len(_routes)-_maxEntries; _idx++ {
		_c.deleteResponseRoute(_routes[_idx].id)
	}
}
