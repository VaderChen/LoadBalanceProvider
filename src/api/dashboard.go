package api

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"LoadBalanceProvider/src/codexauth"
	"LoadBalanceProvider/src/dashboard"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/history"
)

const dashboardDetailsTTL = time.Minute

// 快照發布後不再修改內容；讀取者只複製參照，背景工作不持鎖進行磁碟 I/O。
type dashboardSnapshotCache struct {
	sync.Mutex
	accounts         map[string]codexauth.Status
	fallbacks        map[string]history.ProviderMetricFallback
	baselines        dashboard.MetricBaselines
	accountsAt       time.Time
	historyAt        time.Time
	baselinesAt      time.Time
	nextRefresh      time.Time
	refreshing       bool
	refreshError     string
	baselineRevision uint64
}

type dashboardProvider struct {
	ID       string                   `json:"id"`
	Name     string                   `json:"name"`
	Kind     string                   `json:"kind"`
	Model    string                   `json:"model"`
	Enabled  bool                     `json:"enabled"`
	Downtime *domain.ProviderDowntime `json:"downtime,omitempty"`
}

func (_h *HTTPAPI) handleDashboardSnapshot(_w http.ResponseWriter) {
	if _h.Balancer == nil {
		_h.writeJSON(_w, http.StatusServiceUnavailable, domain.ErrorResponse("service_unavailable", "load balancer is not initialized"))
		return
	}
	_cache := &_h.dashboardCache
	_cache.Lock()
	if !_cache.refreshing && !time.Now().Before(_cache.nextRefresh) {
		_cache.refreshing = true
		go _h.refreshDashboardDetails()
	}
	_accounts, _fallbacks, _baselines := _cache.accounts, _cache.fallbacks, _cache.baselines
	_accountsAt, _historyAt, _baselinesAt := _cache.accountsAt, _cache.historyAt, _cache.baselinesAt
	_refreshing, _refreshError := _cache.refreshing, _cache.refreshError
	_cache.Unlock()

	_configs, _status := _h.Balancer.DashboardSnapshot()
	normalizeProviderOrder(_configs)
	_providers := make([]dashboardProvider, 0, len(_configs))
	for _, _provider := range _configs {
		_model := ""
		if len(_provider.Models) > 0 {
			_model = _provider.Models[0].Name
		}
		_providers = append(_providers, dashboardProvider{
			ID: _provider.ID, Name: _provider.Name, Kind: inferProviderKind(_provider),
			Model: _model, Enabled: _provider.Enabled,
			Downtime: _provider.Downtime,
		})
	}
	_h.applyProviderConversationBindings(_status)
	applyProviderMetricFallbacks(_status, _fallbacks)
	_items := make([]map[string]interface{}, 0, len(_status))
	for _, _item := range _status {
		_id, _ := _item["id"].(string)
		_account := _accounts[_id]
		_item["account"] = defaultString(_account.AccountEmail, _account.AccountName)
		// 儀表板只需要預設模型及一組欄位名稱，省略完整 catalog 與相容別名。
		_thin := make(map[string]interface{})
		for _, _key := range []string{
			"id", "name", "enabled", "active_requests", "max_concurrent", "successes", "failures",
			"circuit_open", "auth_error", "auth_error_message", "account", "bound_conversations",
			"latency_p50_ms", "reaction_time_ms", "last_reaction_time_ms", "processing_time_ms", "provider_generation_tps",
			"client_delivery_tps", "last_completion_tokens", "total_requests", "total_completion_tokens",
			"cumulative_successes", "cumulative_failures", "usage", "remaining_percent",
		} {
			if _value, _ok := _item[_key]; _ok {
				_thin[_key] = _value
			}
		}
		_items = append(_items, _thin)
	}
	_w.Header().Set("Cache-Control", "no-store")
	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"providers": _providers, "status": _items, "baselines": _baselines,
		"updatedAt": time.Now().Format(time.RFC3339Nano), "refreshing": _refreshing,
		"accountsReady": !_accountsAt.IsZero(), "historyReady": !_historyAt.IsZero(),
		"baselinesReady": !_baselinesAt.IsZero(), "historyUpdatedAt": _historyAt,
		"accountsUpdatedAt": _accountsAt, "refreshError": _refreshError,
	})
}

func (_h *HTTPAPI) refreshDashboardDetails() {
	_cache := &_h.dashboardCache
	_errors := []string{}
	defer func() {
		_cache.Lock()
		_cache.refreshing = false
		_cache.nextRefresh = time.Now().Add(dashboardDetailsTTL)
		_cache.refreshError = strings.Join(_errors, "；")
		_cache.Unlock()
	}()

	// 一次讀取全部帳號，過期 token 仍由既有用量更新／請求流程處理。
	_accounts, _err := codexauth.AccountStatusSnapshot()
	if _err == nil {
		_cache.Lock()
		_cache.accounts, _cache.accountsAt = _accounts, time.Now()
		_cache.Unlock()
	} else {
		_errors = append(_errors, "帳號資訊更新失敗")
	}
	_cache.Lock()
	_revision := _cache.baselineRevision
	_cache.Unlock()
	_baselines, _err := dashboard.LoadMetricBaselines()
	if _err == nil {
		_cache.Lock()
		if _revision == _cache.baselineRevision {
			_cache.baselines, _cache.baselinesAt = _baselines, time.Now()
		}
		_cache.Unlock()
	} else {
		_errors = append(_errors, "統計基準更新失敗")
	}
	_fallbacks := history.RecentProviderMetricFallbacks()
	_cache.Lock()
	_cache.fallbacks, _cache.historyAt = _fallbacks, time.Now()
	_cache.Unlock()
}
