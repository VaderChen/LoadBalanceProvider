package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"LoadBalanceProvider/src/proxy"
)

const reconnectRetention = 10 * time.Minute
const reconnectRetryCooldown = 30 * time.Second
const reconnectEntryLimit = 10000

type reconnectIdentityKey struct{}

// 完整 JSON 正規化保留數值精度；無對話識別的獨立請求不依內容猜測重送。
func withReconnectIdentity(r *http.Request, body []byte) *http.Request {
	owner := proxy.ResponseRouteOwner(r)
	if owner == "" || owner == "anonymous" || responseTurnRoute(body, r) == "" {
		return r
	}
	var payload interface{}
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	if !json.Valid(body) || d.Decode(&payload) != nil {
		return r
	}
	canonical, err := json.Marshal(payload)
	if err != nil {
		return r
	}
	identity, _ := json.Marshal([]string{owner, r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("X-Proxy-Turn-ID"), string(canonical)})
	key := fmt.Sprintf("%x", sha256.Sum256(identity))
	ctx := context.WithValue(r.Context(), turnRecoveryKey{}, responseTurnRoute(body, r))
	return r.WithContext(context.WithValue(ctx, reconnectIdentityKey{}, key))
}

type reconnectBudgetStore struct {
	mu      sync.Mutex
	entries map[string]*reconnectBudget
}

// 由 active 租約獨占讀寫；查詢既有 active 項目時不讀取租約內欄位。
type reconnectBudget struct {
	active          bool
	expires         time.Time
	retryAt         time.Time
	waitRetryAt     time.Time
	attempts        int
	limit           int
	waited          time.Duration
	admissionWaited time.Duration
	provider, model string
	rebindFrom      string
	usedProviders   []string
	delivered       bool
	probeAttempts   int
	probeWindowAt   time.Time
	round           int
	recoveryUsed    bool
}

type reconnectRejection struct {
	status        int
	retryAfter    int
	code, message string
	retryAt       time.Time
}

func (r *reconnectRejection) canWaitForRecovery() bool {
	return r != nil && !r.retryAt.IsZero() && (r.code == "request_retry_exhausted" || r.code == "request_probe_throttled" || r.code == "request_wait_throttled")
}

func (s *reconnectBudgetStore) acquire(key string, limit int) (*reconnectBudget, *reconnectRejection) {
	if key == "" {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, entry := range s.entries {
		if !entry.active && !entry.expires.After(now) {
			delete(s.entries, k)
		}
	}
	entry := s.entries[key]
	if entry != nil && entry.active {
		return nil, &reconnectRejection{status: http.StatusTooManyRequests, retryAfter: 3, code: "request_in_progress", message: "相同請求仍在處理，未重複送往上游"}
	}
	if entry != nil {
		if entry.delivered {
			return nil, &reconnectRejection{status: http.StatusBadRequest, code: "request_replay_unsafe", message: "此請求已交付完整工具呼叫後中斷，不自動重播；請確認工具結果並提出新的接續請求"}
		}
		entry.limit = min(entry.limit, limit)
		if now.Before(entry.waitRetryAt) {
			seconds := int((entry.waitRetryAt.Sub(now) + time.Second - 1) / time.Second)
			return nil, &reconnectRejection{status: http.StatusTooManyRequests, retryAfter: seconds, retryAt: entry.waitRetryAt, code: "request_wait_throttled", message: fmt.Sprintf("等待額度暫時用盡，約 %d 秒後恢復；原 Provider 的冷卻仍須等待", seconds)}
		}
		entry.waitRetryAt = time.Time{}
		// 退款不清除實際探測紀錄；短窗口結束後再放行，不延長既定期限。
		if !entry.probeWindowAt.IsZero() && !now.Before(entry.probeWindowAt.Add(reconnectRetryCooldown)) {
			entry.probeAttempts = 0
			entry.probeWindowAt = time.Time{}
			entry.usedProviders = nil
			entry.round = 0
			entry.waited, entry.admissionWaited = 0, 0
		}
		if entry.probeAttempts >= max(3, entry.limit) {
			retryAt := entry.probeWindowAt.Add(reconnectRetryCooldown)
			seconds := int((retryAt.Sub(now) + time.Second - 1) / time.Second)
			return nil, &reconnectRejection{status: http.StatusTooManyRequests, retryAfter: seconds, retryAt: retryAt, code: "request_probe_throttled", message: fmt.Sprintf("容量恢復探測暫停；請在 %d 秒後重試，上游較長的冷卻仍須等待", seconds)}
		}
		if entry.attempts >= entry.limit {
			// 已允許重新選路時略過代理冷卻；實際來源仍須通過選路與上游冷卻檢查。
			if entry.rebindFrom != "" {
				entry.retryAt = time.Time{}
			} else if entry.retryAt.IsZero() {
				entry.retryAt = now.Add(reconnectRetryCooldown)
			}
			if now.Before(entry.retryAt) {
				seconds := int((entry.retryAt.Sub(now) + time.Second - 1) / time.Second)
				return nil, &reconnectRejection{status: http.StatusTooManyRequests, retryAfter: seconds, retryAt: entry.retryAt, code: "request_retry_exhausted", message: fmt.Sprintf("代理上游嘗試額度（%d 次）已用盡，與用戶端重連次數分開計算；請在 %d 秒後重試，上游較長的冷卻仍須等待", entry.limit, seconds)}
			}
			// 冷卻後只增加一次探測機會，不重新發放整套內部重試額度。
			entry.attempts = entry.limit - 1
			entry.waited, entry.admissionWaited = 0, 0
			entry.retryAt = time.Time{}
		}
	} else {
		if len(s.entries) >= reconnectEntryLimit {
			return nil, &reconnectRejection{status: http.StatusServiceUnavailable, retryAfter: 3, code: "retry_tracker_full", message: "重試追蹤容量已滿，請稍後再試"}
		}
		if s.entries == nil {
			s.entries = make(map[string]*reconnectBudget)
		}
		entry = &reconnectBudget{limit: limit}
		s.entries[key] = entry
	}
	if entry.probeWindowAt.IsZero() {
		entry.probeWindowAt = now
	}
	entry.active = true
	return entry, nil
}

func (s *reconnectBudgetStore) release(key string, entry *reconnectBudget, success bool) {
	if entry == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if success || (entry.attempts == 0 && entry.probeAttempts == 0 && entry.admissionWaited == 0) {
		delete(s.entries, key)
		return
	}
	entry.active = false
	entry.expires = time.Now().Add(reconnectRetention)
	// 備妥換帳號的請求不進冷卻，理由同 acquire。
	if entry.attempts >= entry.limit && !entry.delivered && entry.rebindFrom == "" {
		entry.retryAt = time.Now().Add(reconnectRetryCooldown)
	}
}
