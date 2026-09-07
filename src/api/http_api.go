package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"LoadBalanceProvider/src/auth"
	"LoadBalanceProvider/src/balancer"
	benchmarkrunner "LoadBalanceProvider/src/benchmark"
	"LoadBalanceProvider/src/codexauth"
	"LoadBalanceProvider/src/config"
	"LoadBalanceProvider/src/dashboard"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/history"
	"LoadBalanceProvider/src/keyusage"
	"LoadBalanceProvider/src/notification"
	"LoadBalanceProvider/src/providerusage"
	"LoadBalanceProvider/src/proxy"
	"LoadBalanceProvider/src/security"
	"LoadBalanceProvider/src/serviceupdate"
	"LoadBalanceProvider/src/systemmonitor"
	"LoadBalanceProvider/src/telemetry"

	"github.com/MarsSemi/MarsCloud-SaaS/SDK/HttpService"
	"github.com/MarsSemi/MarsCloud-SaaS/SDK/MarsJSON"
)

// -------------------------------------------------------------------------------------
var _serviceStartedAt = time.Now()

// -------------------------------------------------------------------------------------
const (
	sessionCookieName               = "lbp_api_key"
	defaultProviderCapacityCooldown = 10 * time.Second
	// 釘住 provider 的請求只在原帳號上重試，次數刻意保守，避免拖長使用者等待。
	pinnedProviderMaxRetries   = 2
	pinnedProviderRetryBackoff = 400 * time.Millisecond
)

// -------------------------------------------------------------------------------------
type HTTPAPI struct {
	reconnectBudgets       reconnectBudgetStore
	activeResponseRequests sync.Map
	updateRouteRestore     sync.Once
	turnGateLock           sync.Mutex
	turnGates              map[string]*turnGate
	dashboardCache         dashboardSnapshotCache
	bindingRefreshLock     sync.Mutex
	bindingRefreshedAt     time.Time

	Balancer                   *balancer.LoadBalancer
	Client                     *proxy.Client
	ConfigPath                 string
	NotificationConfigPath     string
	AdvancedSettingsConfigPath string
	MCPSettingsConfigPath      string
	BenchmarkManager           *benchmarkrunner.Manager
	SystemMonitor              *systemmonitor.Monitor
	ProviderUsageRecorder      *providerusage.Recorder
	DefaultAccount             string
	DefaultPassword            string
	advancedSettingsLock       sync.RWMutex
	advancedSettings           domain.AdvancedSettingsConfig
	advancedSettingsLoaded     bool
	quotaCooldowns             quotaCooldownState
}

// -------------------------------------------------------------------------------------
type ProviderForm struct {
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	Kind            string   `json:"kind"`
	Role            string   `json:"role,omitempty"`
	APIKey          string   `json:"apiKey"`
	APIKeyMasked    string   `json:"apiKeyMasked,omitempty"`
	HasAPIKey       bool     `json:"hasApiKey"`
	OAuth           bool     `json:"oauth,omitempty"`
	OAuthAccount    string   `json:"oauthAccount,omitempty"`
	Host            string   `json:"host"`
	ChatAPI         string   `json:"chatApi"`
	Model           string   `json:"model"`
	Purpose         string   `json:"purpose"`
	Scale           string   `json:"scale"`
	Responsibility  string   `json:"responsibility"`
	ReasoningEffort string   `json:"reasoningEffort,omitempty"`
	Capabilities    []string `json:"capabilities,omitempty"`
	Enabled         bool     `json:"enabled"`
	MaxConcurrent   int64    `json:"maxConcurrent"`
	Priority        int      `json:"priority,omitempty"`
}

// -------------------------------------------------------------------------------------
type ProviderReorderRequest struct {
	IDs []string `json:"ids"`
}

// -------------------------------------------------------------------------------------
type ProviderRateLimitResetRequest struct {
	IdempotencyKey string `json:"idempotencyKey"`
	CreditID       string `json:"creditId"`
}

// -------------------------------------------------------------------------------------
type DashboardBaselineResetRequest struct {
	Providers map[string]dashboard.ProviderBaseline `json:"providers"`
}

// -------------------------------------------------------------------------------------
type APIKeyCreateRequest struct {
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
}

// -------------------------------------------------------------------------------------
// 指標欄位：JSON 中沒出現的欄位代表「保留原值」，避免只改名的請求
// 把強制路由設定一併重設成 AUTO。要解除綁定請明確傳入 "AUTO"。
type APIKeyUpdateRequest struct {
	Name            *string `json:"name"`
	ProviderID      *string `json:"provider_id"`
	Model           *string `json:"model"`
	ReasoningEffort *string `json:"reasoning_effort"`
}

// -------------------------------------------------------------------------------------
type NotificationTargetForm struct {
	URL            string `json:"url"`
	APIKey         string `json:"apiKey,omitempty"`
	APIKeyMasked   string `json:"apiKeyMasked,omitempty"`
	HasAPIKey      bool   `json:"hasApiKey"`
	Payload        string `json:"payload"`
	PreserveAPIKey bool   `json:"preserveApiKey,omitempty"`
}

// -------------------------------------------------------------------------------------
type GeneralSettingsForm struct {
	ShowProviderModels bool `json:"showProviderModels"`
}

// -------------------------------------------------------------------------------------
type AdvancedSettingsForm struct {
	ProviderRetryRounds                      int     `json:"providerRetryRounds"`
	ProviderRetrySourcesPerRound             int     `json:"providerRetrySourcesPerRound"`
	ProviderRetryWaitSeconds                 int     `json:"providerRetryWaitSeconds"`
	PersistQuotaCooldown                     bool    `json:"persistQuotaCooldown"`
	ConversationAffinityTTLMinutes           int     `json:"conversationAffinityTTLMinutes"`
	ConversationAffinityQuotaTolerancePoints float64 `json:"conversationAffinityQuotaTolerancePoints"`
	ResponseRouteMaxEntries                  int     `json:"responseRouteMaxEntries"`
	ProviderCapacityCooldownSeconds          int     `json:"providerCapacityCooldownSeconds"`
	ProviderServerErrorCooldownSeconds       int     `json:"providerServerErrorCooldownSeconds"`
	MaxBindingsPerProvider                   int     `json:"maxBindingsPerProvider"`
	YieldLowMaxPercent                       float64 `json:"yieldLowMaxPercent"`
	YieldMidMaxPercent                       float64 `json:"yieldMidMaxPercent"`
	LowReasoningDemotionEnabled              bool    `json:"lowReasoningDemotionEnabled"`
	LowReasoningDemotionRequestsPerMin       float64 `json:"lowReasoningDemotionRequestsPerMin"`
	LowReasoningDemotionReasoningPercent     float64 `json:"lowReasoningDemotionReasoningPercent"`
	LowReasoningDemotionTargetTier           int     `json:"lowReasoningDemotionTargetTier"`
	LowReasoningDemotionMinutes              int     `json:"lowReasoningDemotionMinutes"`
	LowReasoningDemotionMinDailyUsagePercent float64 `json:"lowReasoningDemotionMinDailyUsagePercent"`
}

// -------------------------------------------------------------------------------------
// 指標欄位：JSON 中沒出現的欄位代表「沿用現值」。
// 若用值型別，只更新其中一項的請求會把其他欄位靜默重設為零值；
// 其中 tolerance 的 0 是合法值，會無聲關閉對話黏著而不報錯。
type AdvancedSettingsUpdateRequest struct {
	ProviderRetryRounds                      *int     `json:"providerRetryRounds"`
	ProviderRetrySourcesPerRound             *int     `json:"providerRetrySourcesPerRound"`
	ProviderRetryWaitSeconds                 *int     `json:"providerRetryWaitSeconds"`
	PersistQuotaCooldown                     *bool    `json:"persistQuotaCooldown"`
	ConversationAffinityTTLMinutes           *int     `json:"conversationAffinityTTLMinutes"`
	ConversationAffinityQuotaTolerancePoints *float64 `json:"conversationAffinityQuotaTolerancePoints"`
	ResponseRouteMaxEntries                  *int     `json:"responseRouteMaxEntries"`
	ProviderCapacityCooldownSeconds          *int     `json:"providerCapacityCooldownSeconds"`
	ProviderServerErrorCooldownSeconds       *int     `json:"providerServerErrorCooldownSeconds"`
	MaxBindingsPerProvider                   *int     `json:"maxBindingsPerProvider"`
	YieldLowMaxPercent                       *float64 `json:"yieldLowMaxPercent"`
	YieldMidMaxPercent                       *float64 `json:"yieldMidMaxPercent"`
	LowReasoningDemotionEnabled              *bool    `json:"lowReasoningDemotionEnabled"`
	LowReasoningDemotionRequestsPerMin       *float64 `json:"lowReasoningDemotionRequestsPerMin"`
	LowReasoningDemotionReasoningPercent     *float64 `json:"lowReasoningDemotionReasoningPercent"`
	LowReasoningDemotionTargetTier           *int     `json:"lowReasoningDemotionTargetTier"`
	LowReasoningDemotionMinutes              *int     `json:"lowReasoningDemotionMinutes"`
	LowReasoningDemotionMinDailyUsagePercent *float64 `json:"lowReasoningDemotionMinDailyUsagePercent"`
}

// -------------------------------------------------------------------------------------
type SystemUpdateSessionRequest struct {
	FileName  string `json:"file_name"`
	TotalSize int64  `json:"total_size"`
}

// -------------------------------------------------------------------------------------
type SystemUpdateCommitRequest struct {
	SessionID string `json:"session_id"`
	FileName  string `json:"file_name"`
}

// -------------------------------------------------------------------------------------
type LoginRequest struct {
	Account  string `json:"account"`
	Password string `json:"password"`
}

// -------------------------------------------------------------------------------------
type ProviderOAuthStartRequest struct {
	ID             string `json:"id"`
	FlowPreference string `json:"flow_preference,omitempty"`
	LaunchBrowser  bool   `json:"launch_browser,omitempty"`
}

// -------------------------------------------------------------------------------------
type ProviderOAuthCompleteRequest struct {
	ID    string `json:"id"`
	Input string `json:"input"`
}

// -------------------------------------------------------------------------------------
type MultimodalEndpointSpec struct {
	Path        string
	Requirement string
	TaskType    string
	Streamable  bool
}

// -------------------------------------------------------------------------------------
type MultimodalRequestMeta struct {
	Model      string
	Provider   string
	ProviderID string
	Text       string
	Stream     bool
}

// -------------------------------------------------------------------------------------
type requestAPIKeyCandidate struct {
	Key        string
	FromCookie bool
}

// -------------------------------------------------------------------------------------
type requestAPIKeyContextKey struct{}

// -------------------------------------------------------------------------------------
type apiKeyRoutingPolicy struct {
	ProviderID      string
	Model           string
	ReasoningEffort string
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) Process(_w http.ResponseWriter, _r *http.Request, _jwt *MarsJSON.JSONObject, _path []string, _params *MarsJSON.JSONObject, _body string) []byte {
	_h.restoreUpdateRouteCheckpoint()
	_route := normalizedAPIRoute(_r.URL.Path)
	if isMCPRoute(_route) {
		_allowed, _err := _h.mcpOriginAllowed(_r)
		if _err != nil {
			writeMCPHTTPError(_w, http.StatusInternalServerError, -32603, "MCP origin settings could not be loaded", nil)
			return responseHandled()
		}
		if !_allowed {
			writeMCPHTTPError(_w, http.StatusForbidden, -32000, "MCP Origin is not allowed", nil)
			return responseHandled()
		}
	}

	setCORSHeaders(_w, _r)
	if _r.Method == http.MethodOptions {
		_w.WriteHeader(http.StatusNoContent)
		return responseHandled()
	}

	if !_h.authorizeRequest(_w, _r, _route) {
		return responseHandled()
	}

	switch {
	case isMCPRoute(_route):
		_h.handleMCP(_w, _r, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodGet && _route == "/api/dashboard":
		_h.handleDashboardSnapshot(_w)
		return responseHandled()

	case _r.Method == http.MethodGet && (_route == "/api/health" || _route == "/v1/health"):
		_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
			"status":    "ok",
			"timestamp": time.Now().Format(time.RFC3339),
			"version":   serviceVersion(),
			"providers": _h.providerStatusWithHistoryFallback(),
		})
		return responseHandled()

	case _r.Method == http.MethodGet && (_route == "/api/version" || _route == "/v1/version"):
		_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
			"name":       "LLM Proxy",
			"version":    serviceVersion(),
			"started_at": _serviceStartedAt.Format(time.RFC3339),
		})
		return responseHandled()

	case _r.Method == http.MethodPost && (_route == "/api/login" || _route == "/v1/login"):
		_h.handleLogin(_w, _r, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodPost && (_route == "/api/logout" || _route == "/v1/logout"):
		_h.handleLogout(_w, _r)
		return responseHandled()

	case _r.Method == http.MethodGet && (_route == "/api/session" || _route == "/v1/session"):
		_h.handleSession(_w)
		return responseHandled()

	case _r.Method == http.MethodGet && isSettingsRoute(_route, "general"):
		_h.handleGetGeneralSettings(_w)
		return responseHandled()

	case _r.Method == http.MethodPut && isSettingsRoute(_route, "general"):
		_h.handleSaveGeneralSettings(_w, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodGet && isSettingsRoute(_route, "advanced"):
		_h.handleGetAdvancedSettings(_w)
		return responseHandled()

	case _r.Method == http.MethodPut && isSettingsRoute(_route, "advanced"):
		_h.handleSaveAdvancedSettings(_w, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodGet && isSettingsRoute(_route, "mcp"):
		_h.handleGetMCPSettings(_w, _r)
		return responseHandled()

	case _r.Method == http.MethodPut && isSettingsRoute(_route, "mcp"):
		_h.handleSaveMCPSettings(_w, _r, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodGet && isSettingsRoute(_route, "notification"):
		_h.handleGetNotificationTarget(_w)
		return responseHandled()

	case _r.Method == http.MethodPut && isSettingsRoute(_route, "notification"):
		_h.handleSaveNotificationTarget(_w, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodPost && isSettingsRoute(_route, "notification", "test"):
		_h.handleTestNotificationTarget(_w, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodPost && (_route == "/api/system/update/session" || _route == "/v1/system/update/session"):
		_h.handleCreateSystemUpdateSession(_w, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodPost && (_route == "/api/system/update/chunk" || _route == "/v1/system/update/chunk"):
		_h.handleSystemUpdateChunk(_w, _r, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodPost && (_route == "/api/system/update/commit" || _route == "/v1/system/update/commit"):
		_h.handleCommitSystemUpdate(_w, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodGet && (_route == "/api/system/update/status" || _route == "/v1/system/update/status"):
		_h.handleSystemUpdateStatus(_w)
		return responseHandled()

	case _r.Method == http.MethodPost && (_route == "/api/system/update" || _route == "/v1/system/update"):
		_h.handleSystemUpdate(_w, _r, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodGet && (_route == "/api/system/resources/usage" || _route == "/v1/system/resources/usage"):
		_h.handleSystemResourceUsage(_w, _r)
		return responseHandled()

	case _r.Method == http.MethodGet && (_route == "/api/system/resources/details" || _route == "/v1/system/resources/details"):
		_h.handleSystemResourceDetails(_w, _r)
		return responseHandled()

	case _r.Method == http.MethodGet && (_route == "/api/providers" || _route == "/v1/providers"):
		_h.writeJSON(_w, http.StatusOK, _h.providerStatusWithHistoryFallback())
		return responseHandled()

	case _r.Method == http.MethodGet && isModelsRoute(_route):
		_h.handleModels(_w, _r)
		return responseHandled()

	case _r.Method == http.MethodGet && isModelRetrieveRoute(_route):
		_h.handleRetrieveModel(_w, modelIDFromRetrieveRoute(_route))
		return responseHandled()

	case _r.Method == http.MethodGet && (_route == "/api/provider-configs" || _route == "/v1/provider-configs"):
		_h.handleListProviderConfigs(_w)
		return responseHandled()

	case _r.Method == http.MethodGet && (_route == "/api/dashboard-metric-baselines" || _route == "/v1/dashboard-metric-baselines"):
		_h.handleGetDashboardMetricBaselines(_w)
		return responseHandled()

	case _r.Method == http.MethodGet && (_route == "/api/provider-usage" || _route == "/v1/provider-usage"):
		_h.handleGetProviderUsageHistory(_w, _r.URL.Query().Get("month"))
		return responseHandled()

	case _r.Method == http.MethodPost && (_route == "/api/dashboard-metric-baselines/reset" || _route == "/v1/dashboard-metric-baselines/reset"):
		_h.handleResetDashboardMetricBaselines(_w, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodGet && (_route == "/api/api-keys" || _route == "/v1/api-keys"):
		_h.handleListAPIKeys(_w)
		return responseHandled()

	case _r.Method == http.MethodGet && (_route == "/api/api-key-routing-options" || _route == "/v1/api-key-routing-options"):
		_h.handleAPIKeyRoutingOptions(_w)
		return responseHandled()

	case _r.Method == http.MethodPost && (_route == "/api/api-keys" || _route == "/v1/api-keys"):
		_h.handleCreateAPIKey(_w, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodPost && isAPIKeyActionRoute(_route, "disable"):
		_h.handleSetAPIKeyEnabled(_w, apiKeyIDFromActionRoute(_route, "disable"), false)
		return responseHandled()

	case _r.Method == http.MethodPost && isAPIKeyActionRoute(_route, "enable"):
		_h.handleSetAPIKeyEnabled(_w, apiKeyIDFromActionRoute(_route, "enable"), true)
		return responseHandled()

	case _r.Method == http.MethodGet && isAPIKeyDensityRoute(_route):
		_h.handleGetAPIKeyDensity(_w, _r.URL.Query().Get("window"))
		return responseHandled()

	case _r.Method == http.MethodGet && isAPIKeyUsageQueryRoute(_route):
		_h.handleGetAPIKeyUsage(_w, _r.URL.Query().Get("id"), _r.URL.Query().Get("month"))
		return responseHandled()

	case _r.Method == http.MethodGet && isAPIKeyUsageRoute(_route):
		_h.handleGetAPIKeyUsage(_w, apiKeyIDFromUsageRoute(_route), _r.URL.Query().Get("month"))
		return responseHandled()

	case _r.Method == http.MethodPut && (strings.HasPrefix(_route, "/api/api-keys/") || strings.HasPrefix(_route, "/v1/api-keys/")):
		_h.handleUpdateAPIKey(_w, apiKeyIDFromRoute(_route), []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodDelete && (strings.HasPrefix(_route, "/api/api-keys/") || strings.HasPrefix(_route, "/v1/api-keys/")):
		_h.handleDeleteAPIKey(_w, apiKeyIDFromRoute(_route))
		return responseHandled()

	case _r.Method == http.MethodGet && (_route == "/api/benchmarks/intelligence/catalog" || _route == "/v1/benchmarks/intelligence/catalog"):
		_h.writeJSON(_w, http.StatusOK, map[string]interface{}{"groups": benchmarkrunner.Catalog()})
		return responseHandled()

	case _r.Method == http.MethodPost && (_route == "/api/benchmarks/intelligence" || _route == "/v1/benchmarks/intelligence"):
		_h.handleStartIntelligenceBenchmark(_w, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodGet && (strings.HasPrefix(_route, "/api/benchmarks/intelligence/") || strings.HasPrefix(_route, "/v1/benchmarks/intelligence/")):
		_h.handleGetIntelligenceBenchmark(_w, benchmarkIDFromRoute(_route))
		return responseHandled()

	case _r.Method == http.MethodPost && (strings.HasSuffix(_route, "/cancel") && (strings.HasPrefix(_route, "/api/benchmarks/intelligence/") || strings.HasPrefix(_route, "/v1/benchmarks/intelligence/"))):
		_h.handleCancelIntelligenceBenchmark(_w, benchmarkIDFromCancelRoute(_route))
		return responseHandled()

	case _r.Method == http.MethodGet && isProviderActionRoute(_route, "models"):
		_h.handleProviderModels(_w, providerIDFromActionRoute(_route, "models"))
		return responseHandled()

	case _r.Method == http.MethodPost && isProviderActionRoute(_route, "test"):
		_h.handleProviderTest(_w, providerIDFromActionRoute(_route, "test"))
		return responseHandled()

	case _r.Method == http.MethodGet && isProviderActionRoute(_route, "rate-limit-reset"):
		_h.handleGetProviderRateLimitReset(_w, providerIDFromActionRoute(_route, "rate-limit-reset"))
		return responseHandled()

	case _r.Method == http.MethodGet && isProviderActionRoute(_route, "billing"):
		_h.handleProviderBilling(_w, _r, providerIDFromActionRoute(_route, "billing"))
		return responseHandled()

	case _r.Method == http.MethodPost && isProviderActionRoute(_route, "rate-limit-reset"):
		_h.handleConsumeProviderRateLimitReset(_w, providerIDFromActionRoute(_route, "rate-limit-reset"), []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodPost && (_route == "/api/provider/oauth/start" || _route == "/v1/provider/oauth/start"):
		_h.handleProviderOAuthStart(_w, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodGet && (_route == "/api/provider/oauth/status" || _route == "/v1/provider/oauth/status"):
		_h.handleProviderOAuthStatus(_w, _r)
		return responseHandled()

	case _r.Method == http.MethodPost && (_route == "/api/provider/oauth/complete" || _route == "/v1/provider/oauth/complete"):
		_h.handleProviderOAuthComplete(_w, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodPost && (_route == "/api/provider-configs" || _route == "/v1/provider-configs"):
		_h.handleCreateProviderConfig(_w, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodPost && (_route == "/api/provider-configs/reorder" || _route == "/v1/provider-configs/reorder"):
		_h.handleReorderProviderConfigs(_w, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodPut && (strings.HasPrefix(_route, "/api/provider-configs/") || strings.HasPrefix(_route, "/v1/provider-configs/")):
		_h.handleUpdateProviderConfig(_w, providerIDFromRoute(_route), []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodDelete && (strings.HasPrefix(_route, "/api/provider-configs/") || strings.HasPrefix(_route, "/v1/provider-configs/")):
		_h.handleDeleteProviderConfig(_w, providerIDFromRoute(_route))
		return responseHandled()

	case _r.Method == http.MethodPost && (_route == "/v1/chat/completions" || _route == "/api/v1/chat/completions"):
		_h.handleChatCompletions(_w, _r, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodPost && (_route == "/v1/responses" || _route == "/api/v1/responses"):
		_h.handleResponsesProxy(_w, _r, []byte(_body))
		return responseHandled()

	case _r.Method == http.MethodPost && isLocalTokenCountRoute(_route):
		_h.handleLocalTokenCount(_w, []byte(_body))
		return responseHandled()

	case isResponsesProxySubroute(_r.Method, _route):
		_h.handleResponsesRawProxy(_w, _r, []byte(_body), responsesProxyRouteFromRequest(_r, _route))
		return responseHandled()

	case _r.Method == http.MethodPost && isMultimodalProxyRoute(_route):
		_spec, _ := multimodalEndpointSpecForRoute(_route)
		_h.handleMultimodalProxy(_w, _r, []byte(_body), _spec)
		return responseHandled()
	}

	_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "unsupported endpoint"))
	return responseHandled()
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleSystemResourceUsage(_w http.ResponseWriter, _r *http.Request) {
	if _h.SystemMonitor == nil {
		_h.writeJSON(_w, http.StatusServiceUnavailable, domain.ErrorResponse("service_unavailable", "系統資源監看服務尚未啟動"))
		return
	}
	_result, _err := _h.SystemMonitor.Query(_r.URL.Query().Get("date"), _r.URL.Query().Get("mode"))
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_resource_query", _err.Error()))
		return
	}
	_h.writeJSON(_w, http.StatusOK, _result)
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleSystemResourceDetails(_w http.ResponseWriter, _r *http.Request) {
	if _h.SystemMonitor == nil {
		_h.writeJSON(_w, http.StatusServiceUnavailable, domain.ErrorResponse("service_unavailable", "系統資源監看服務尚未啟動"))
		return
	}
	_ctx, _cancel := context.WithTimeout(_r.Context(), 12*time.Second)
	defer _cancel()
	_h.writeJSON(_w, http.StatusOK, _h.SystemMonitor.Details(_ctx))
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleCreateSystemUpdateSession(_w http.ResponseWriter, _body []byte) {
	var _request SystemUpdateSessionRequest
	if _err := json.Unmarshal(_body, &_request); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_update_session", "更新切片工作格式不正確"))
		return
	}
	_session, _err := serviceupdate.CreateUploadSession(_request.FileName, _request.TotalSize)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_update_session", _err.Error()))
		return
	}
	_h.writeJSON(_w, http.StatusCreated, map[string]interface{}{
		"success":    true,
		"session":    _session,
		"chunk_size": serviceupdate.UploadChunkBytes,
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleSystemUpdateChunk(_w http.ResponseWriter, _r *http.Request, _body []byte) {
	_sessionID := strings.TrimSpace(_r.URL.Query().Get("session_id"))
	_index, _indexErr := strconv.Atoi(strings.TrimSpace(_r.URL.Query().Get("index")))
	_offset, _offsetErr := strconv.ParseInt(strings.TrimSpace(_r.URL.Query().Get("offset")), 10, 64)
	if _sessionID == "" || _indexErr != nil || _offsetErr != nil || _index < 0 || _offset < 0 {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_update_chunk", "更新切片參數不正確"))
		return
	}
	_session, _err := serviceupdate.AppendUploadChunk(_sessionID, _index, _offset, _body)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_update_chunk", _err.Error()))
		return
	}
	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"success": true,
		"session": _session,
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleCommitSystemUpdate(_w http.ResponseWriter, _body []byte) {
	var _request SystemUpdateCommitRequest
	if _err := json.Unmarshal(_body, &_request); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_update_commit", "更新完成請求格式不正確"))
		return
	}
	_archive, _fileName, _err := serviceupdate.CompleteUploadSession(_request.SessionID, _request.FileName)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_update_commit", _err.Error()))
		return
	}
	if _err := _h.saveUpdateRouteCheckpoint(); _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("update_checkpoint_failed", "無法保存 Provider 綁定，更新尚未啟動："+_err.Error()))
		return
	}
	_result, _err := serviceupdate.PrepareAndLaunch(_archive, _fileName)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("update_failed", _err.Error()))
		return
	}
	_h.writeJSON(_w, http.StatusAccepted, map[string]interface{}{
		"success": true,
		"update":  _result,
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleSystemUpdate(_w http.ResponseWriter, _r *http.Request, _body []byte) {
	_archive, _fileName, _err := serviceupdate.ReadMultipartUpload(_r.Header.Get("Content-Type"), _body)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_update_package", _err.Error()))
		return
	}

	if _err := _h.saveUpdateRouteCheckpoint(); _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("update_checkpoint_failed", "無法保存 Provider 綁定，更新尚未啟動："+_err.Error()))
		return
	}
	_result, _err := serviceupdate.PrepareAndLaunch(_archive, _fileName)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("update_failed", _err.Error()))
		return
	}
	_h.writeJSON(_w, http.StatusAccepted, map[string]interface{}{
		"success": true,
		"update":  _result,
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleSystemUpdateStatus(_w http.ResponseWriter) {
	_status, _err := serviceupdate.CurrentStatus()
	if _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("update_status_failed", _err.Error()))
		return
	}
	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"success": true,
		"update":  _status,
	})
}

// -------------------------------------------------------------------------------------
func responseHandled() []byte {
	return []byte(HttpService.ResponseHandledMarker)
}

// -------------------------------------------------------------------------------------
func normalizedAPIRoute(_path string) string {
	_route := strings.TrimRight(strings.TrimSpace(_path), "/")
	if _route == "" {
		return "/"
	}
	if _idx := strings.LastIndex(_route, "/backend-api/codex"); _idx >= 0 {
		_suffix := strings.TrimPrefix(_route[_idx:], "/backend-api/codex")
		if _suffix == "" {
			return "/v1"
		}
		return "/v1/" + strings.TrimLeft(_suffix, "/")
	}
	if strings.HasSuffix(_route, "/api") {
		return "/api"
	}
	if strings.HasSuffix(_route, "/v1") {
		return "/v1"
	}
	_lastMarkerIdx := -1
	for _, _marker := range []string{"/api/", "/v1/"} {
		if _idx := strings.LastIndex(_route, _marker); _idx > _lastMarkerIdx {
			_lastMarkerIdx = _idx
		}
	}
	if _lastMarkerIdx > 0 {
		return _route[_lastMarkerIdx:]
	}
	for _, _marker := range []string{"/api/", "/v1/"} {
		if _idx := strings.Index(_route, _marker); _idx > 0 {
			return _route[_idx:]
		}
	}
	return _route
}

// -------------------------------------------------------------------------------------
// setCORSHeaders 只允許以 Authorization 或 X-API-Key 明確帶 token 的跨來源 API 呼叫。
// 管理介面登入 session 只能走同源 cookie，不透過 CORS 開放 credentials。
func setCORSHeaders(_w http.ResponseWriter, _r *http.Request) {
	_header := _w.Header()
	_origin := strings.TrimSpace(_r.Header.Get("Origin"))
	if _origin != "" {
		_header.Add("Vary", "Origin")
		if !corsUsesExplicitToken(_r) {
			return
		}
		_header.Set("Access-Control-Allow-Origin", _origin)
	} else {
		_header.Set("Access-Control-Allow-Origin", "*")
	}

	_header.Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
	_header.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-API-Key, Accept, OpenAI-Beta, OpenAI-Organization, OpenAI-Project, MCP-Protocol-Version, MCP-Session-Id")
	_header.Set("Access-Control-Max-Age", "86400")
	_header.Set("Access-Control-Expose-Headers", "Content-Type, ETag, MCP-Protocol-Version, MCP-Session-Id, X-Proxy-Provider, X-Proxy-Model, X-Proxy-Task-Type, X-Proxy-Strategy")
}

// -------------------------------------------------------------------------------------
func corsUsesExplicitToken(_r *http.Request) bool {
	if _r == nil {
		return false
	}
	if _r.Method == http.MethodOptions {
		_requestHeaders := _r.Header.Get("Access-Control-Request-Headers")
		return headerListContains(_requestHeaders, "authorization") || headerListContains(_requestHeaders, "x-api-key")
	}
	return strings.TrimSpace(_r.Header.Get("Authorization")) != "" || strings.TrimSpace(_r.Header.Get("X-API-Key")) != ""
}

// -------------------------------------------------------------------------------------
func headerListContains(_value string, _target string) bool {
	_target = strings.ToLower(strings.TrimSpace(_target))
	for _, _part := range strings.Split(_value, ",") {
		if strings.ToLower(strings.TrimSpace(_part)) == _target {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) authorizeRequest(_w http.ResponseWriter, _r *http.Request, _route string) bool {
	if isMCPInternalRequest(_r) {
		return true
	}
	if _r.Method == http.MethodPost && (_route == "/api/login" || _route == "/v1/login") {
		return true
	}
	if _r.Method == http.MethodPost && (_route == "/api/logout" || _route == "/v1/logout") {
		return true
	}

	_store := auth.DefaultAPIKeyStore()
	_foundValidKey := false
	for _, _candidate := range requestAPIKeys(_r) {
		// Verify 不會產生副作用；使用次數只在真正通過授權後才記錄，
		// 否則被拒絕的請求（403）也會計次，cookie 退路更會一次計到兩把金鑰。
		if _view, _ok, _err := _store.Verify(_candidate.Key); _err != nil {
			_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("api_key_store_error", _err.Error()))
			return false
		} else if _ok {
			_foundValidKey = true
			if canAccessRoute(_view, _candidate.FromCookie, _r.Method, _route) {
				// MCP 金鑰只做端點授權，不建立使用統計。
				if _view.KeyType != auth.APIKeyTypeMCP {
					if _err := _store.RecordUsage(_view.ID); _err != nil {
						_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("api_key_store_error", _err.Error()))
						return false
					}
				}
				_ctx := context.WithValue(_r.Context(), requestAPIKeyContextKey{}, _view)
				// Response 資源的擁有者用「已驗證」的金鑰 ID，不要重新解析 Header/Cookie。
				_ctx = proxy.WithResponseRouteOwner(_ctx, "key:"+_view.ID)
				*_r = *_r.WithContext(_ctx)
				return true
			}
		}
	}

	if _foundValidKey {
		_h.writeJSON(_w, http.StatusForbidden, domain.ErrorResponse("forbidden", "this credential is not allowed for this endpoint"))
		return false
	}

	_w.Header().Set("WWW-Authenticate", `Bearer realm="LLM Proxy"`)
	_h.writeJSON(_w, http.StatusUnauthorized, domain.ErrorResponse("unauthorized", "valid API key is required"))
	return false
}

// -------------------------------------------------------------------------------------
func canAccessRoute(_key auth.APIKeyView, _fromCookie bool, _method string, _route string) bool {
	if isMCPRoute(_route) {
		return !_key.Temporary && (_key.KeyType == auth.APIKeyTypeChat || _key.KeyType == auth.APIKeyTypeMCP)
	}
	if _key.Temporary {
		return _fromCookie
	}
	if _key.KeyType != auth.APIKeyTypeChat {
		return false
	}
	return isChatCompatibleRoute(_method, _route)
}

// -------------------------------------------------------------------------------------
func apiKeyRoutingPolicyFromRequest(_r *http.Request) (apiKeyRoutingPolicy, bool) {
	if _r == nil {
		return apiKeyRoutingPolicy{}, false
	}
	_view, _ok := _r.Context().Value(requestAPIKeyContextKey{}).(auth.APIKeyView)
	if !_ok || _view.Temporary || _view.KeyType != auth.APIKeyTypeChat {
		return apiKeyRoutingPolicy{}, false
	}
	return apiKeyRoutingPolicy{
		ProviderID:      normalizeAPIKeyRoutingValue(_view.ProviderID),
		Model:           normalizeAPIKeyRoutingValue(_view.Model),
		ReasoningEffort: normalizeAPIKeyRoutingValue(_view.ReasoningEffort),
	}, true
}

// -------------------------------------------------------------------------------------
// forcesNothing 表示三個欄位都是 AUTO，套用政策等同於什麼都不做。
func (_p apiKeyRoutingPolicy) forcesNothing() bool {
	return strings.EqualFold(_p.ProviderID, "AUTO") &&
		strings.EqualFold(_p.Model, "AUTO") &&
		strings.EqualFold(_p.ReasoningEffort, "AUTO")
}

// -------------------------------------------------------------------------------------
func normalizeAPIKeyRoutingValue(_value string) string {
	_value = strings.TrimSpace(_value)
	if _value == "" || strings.EqualFold(_value, "AUTO") {
		return "AUTO"
	}
	return _value
}

// -------------------------------------------------------------------------------------
func isChatCompatibleRoute(_method string, _route string) bool {
	if _method == http.MethodGet && isModelsRoute(_route) {
		return true
	}
	if _method == http.MethodGet && isModelRetrieveRoute(_route) {
		return true
	}
	if _method == http.MethodPost && (_route == "/v1/chat/completions" || _route == "/api/v1/chat/completions") {
		return true
	}
	if isResponsesProxyRoute(_method, _route) {
		return true
	}
	if _method == http.MethodPost && isLocalTokenCountRoute(_route) {
		return true
	}
	if _method == http.MethodPost && isMultimodalProxyRoute(_route) {
		return true
	}
	return false
}

// -------------------------------------------------------------------------------------
func isLocalTokenCountRoute(_route string) bool {
	_route = normalizeProxyRoute(_route)
	return _route == "/v1/responses/input_tokens"
}

// -------------------------------------------------------------------------------------
func isModelsRoute(_route string) bool {
	switch strings.TrimRight(strings.TrimSpace(_route), "/") {
	case "/models", "/v1/models", "/api/v1/models":
		return true
	default:
		return false
	}
}

// -------------------------------------------------------------------------------------
func isModelRetrieveRoute(_route string) bool {
	return modelIDFromRetrieveRoute(_route) != ""
}

// -------------------------------------------------------------------------------------
func modelIDFromRetrieveRoute(_route string) string {
	_route = strings.TrimRight(strings.TrimSpace(_route), "/")
	var _encoded string
	switch {
	case strings.HasPrefix(_route, "/v1/models/"):
		_encoded = strings.TrimPrefix(_route, "/v1/models/")
	case strings.HasPrefix(_route, "/api/v1/models/"):
		_encoded = strings.TrimPrefix(_route, "/api/v1/models/")
	case strings.HasPrefix(_route, "/models/"):
		_encoded = strings.TrimPrefix(_route, "/models/")
	default:
		return ""
	}
	_encoded = strings.TrimSpace(_encoded)
	if _encoded == "" || strings.Contains(_encoded, "/") {
		return ""
	}
	_model, _err := url.PathUnescape(_encoded)
	if _err != nil {
		return _encoded
	}
	return strings.TrimSpace(_model)
}

// -------------------------------------------------------------------------------------
func requestAPIKeys(_r *http.Request) []requestAPIKeyCandidate {
	_keys := make([]requestAPIKeyCandidate, 0, 3)
	_seen := map[string]bool{}
	_add := func(_key string, _fromCookie bool) {
		_key = strings.TrimSpace(_key)
		if _key == "" || _seen[_key] {
			return
		}
		_seen[_key] = true
		_keys = append(_keys, requestAPIKeyCandidate{Key: _key, FromCookie: _fromCookie})
	}

	_authHeader := strings.TrimSpace(_r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(_authHeader), "bearer ") {
		_add(_authHeader[7:], false)
	}

	_add(_r.Header.Get("X-API-Key"), false)

	// 只要呼叫端帶了明確憑證，就完全不讀 Session Cookie。
	// 否則失效或無權限的 Chat Key 會被瀏覽器既有的登入 session 掩蓋，
	// 在已登入的分頁看起來一切正常，實際上金鑰是壞的。
	if len(_keys) > 0 {
		return _keys
	}

	if _cookie, _err := _r.Cookie(sessionCookieName); _err == nil {
		_add(_cookie.Value, true)
	}

	return _keys
}

// -------------------------------------------------------------------------------------
func serviceVersion() string {
	return "1." + _serviceStartedAt.Format("06.0102 build 1504")
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) providerStatusWithHistoryFallback() []map[string]interface{} {
	if _h == nil || _h.Balancer == nil {
		return []map[string]interface{}{}
	}

	_status := _h.Balancer.ProviderStatus()
	_h.applyProviderAccountStatus(_status)
	_h.applyProviderConversationBindings(_status)
	_fallbacks := history.RecentProviderMetricFallbacks()
	applyProviderMetricFallbacks(_status, _fallbacks)
	return _status
}

func applyProviderMetricFallbacks(_status []map[string]interface{}, _fallbacks map[string]history.ProviderMetricFallback) {
	if len(_fallbacks) == 0 {
		return
	}

	for _idx := range _status {
		_providerID, _ok := _status[_idx]["id"].(string)
		if !_ok || _providerID == "" {
			continue
		}
		_fallback, _ok := _fallbacks[_providerID]
		if !_ok {
			continue
		}
		_tokenSpeed := balancer.NormalizeTokenRate(_fallback.Tokens, _fallback.DurationMS, _fallback.TokenSpeed)
		_clientDeliveryTPS := balancer.NormalizeTokenRate(_fallback.Tokens, _fallback.DurationMS, _fallback.ClientDeliveryTPS)
		applyFloatFallback(_status[_idx], _tokenSpeed, "provider_generation_tps", "token_generation_speed", "generation_tokens_per_second", "tokens_per_second", "output_tokens_per_second", "last_token_generation_speed")
		applyFloatFallback(_status[_idx], _clientDeliveryTPS, "client_delivery_tps", "last_client_delivery_tps", "stream_out_tps", "streaming_out_tps")
		applyFloatFallback(_status[_idx], _fallback.ReactionMS, "reaction_time_ms")
		applyFloatFallback(_status[_idx], _fallback.DurationMS, "processing_time_ms", "last_duration_ms")
		if mapNumber(_status[_idx]["last_completion_tokens"]) <= 0 && _fallback.Tokens > 0 {
			_status[_idx]["last_completion_tokens"] = _fallback.Tokens
		}
		if _fallback.TotalRequests > 0 {
			_status[_idx]["total_requests"] = _fallback.TotalRequests
			_status[_idx]["selected_count"] = _fallback.TotalRequests
			_status[_idx]["request_count"] = _fallback.TotalRequests
			_status[_idx]["cumulative_successes"] = _fallback.Successes
			_status[_idx]["cumulative_failures"] = _fallback.Failures
		}
		if _fallback.TotalCompletionTokens > 0 {
			_status[_idx]["total_completion_tokens"] = _fallback.TotalCompletionTokens
			_status[_idx]["cumulative_completion_tokens"] = _fallback.TotalCompletionTokens
			_status[_idx]["completion_tokens_total"] = _fallback.TotalCompletionTokens
		}
	}
}

// -------------------------------------------------------------------------------------
// applyProviderConversationBindings 補上「目前被對話黏著綁在該 provider 的對話數」。
func (_h *HTTPAPI) applyProviderConversationBindings(_status []map[string]interface{}) {
	if _h == nil || _h.Client == nil {
		return
	}
	_counts := _h.Client.PromptCacheRouteCounts()
	for _idx := range _status {
		_providerID, _ok := _status[_idx]["id"].(string)
		if !_ok || _providerID == "" {
			continue
		}
		_status[_idx]["bound_conversations"] = _counts[_providerID]
	}
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) applyProviderAccountStatus(_status []map[string]interface{}) {
	if _h == nil || _h.Balancer == nil {
		return
	}
	_snapshot := _h.Balancer.ConfigSnapshot()
	_providersByID := map[string]domain.LLMProviderConfig{}
	for _, _provider := range _snapshot.Providers {
		_providersByID[_provider.ID] = _provider
	}
	for _idx := range _status {
		_providerID, _ok := _status[_idx]["id"].(string)
		if !_ok || _providerID == "" {
			continue
		}
		_provider, _ok := _providersByID[_providerID]
		if !_ok || !strings.EqualFold(inferProviderKind(_provider), "openai-codex") {
			continue
		}
		if _oauthStatus, _err := codexauth.StatusFor(_provider.ID); _err == nil && _oauthStatus.Status == "connected" {
			_account := defaultString(_oauthStatus.AccountEmail, _oauthStatus.AccountName)
			if strings.TrimSpace(_account) != "" {
				_status[_idx]["account"] = _account
				_status[_idx]["account_email"] = _oauthStatus.AccountEmail
				_status[_idx]["account_name"] = _oauthStatus.AccountName
				_status[_idx]["oauth_account"] = _account
				_status[_idx]["oauthAccount"] = _account
			}
		}
	}
}

// -------------------------------------------------------------------------------------
func applyFloatFallback(_target map[string]interface{}, _fallback float64, _keys ...string) {
	if _fallback <= 0 {
		return
	}
	for _, _key := range _keys {
		if mapNumber(_target[_key]) <= 0 {
			_target[_key] = _fallback
		}
	}
}

// -------------------------------------------------------------------------------------
func mapNumber(_value interface{}) float64 {
	switch _typed := _value.(type) {
	case float64:
		return _typed
	case float32:
		return float64(_typed)
	case int:
		return float64(_typed)
	case int64:
		return float64(_typed)
	case json.Number:
		_value, _ := _typed.Float64()
		return _value
	default:
		return 0
	}
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleModels(_w http.ResponseWriter, _r *http.Request) {
	if isCodexModelsManifestRequest(_r) {
		_h.handleCodexModelsManifest(_w, _r)
		return
	}
	_h.handleStrategyModels(_w)
}

// -------------------------------------------------------------------------------------
func isCodexModelsManifestRequest(_r *http.Request) bool {
	if _r == nil || _r.URL == nil {
		return false
	}
	return strings.TrimSpace(_r.URL.Query().Get("client_version")) != "" || strings.Contains(_r.URL.Path, "/backend-api/codex/models")
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleStrategyModels(_w http.ResponseWriter) {
	_models := []map[string]interface{}{
		modelAPIObject("AUTO", "load-balance-provider", time.Now().Unix()),
	}
	_settings, _err := config.LoadGeneralSettingsConfig(_h.generalSettingsConfigPath())
	if _err == nil && _settings.ShowProviderModels {
		_models = append(_models, _h.providerModelListForModelsAPI()...)
	}

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"object": "list",
		"data":   _models,
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleCodexModelsManifest(_w http.ResponseWriter, _r *http.Request) {
	if _h.Balancer == nil || _h.Client == nil {
		_h.writeJSON(_w, http.StatusServiceUnavailable, domain.ErrorResponse("service_unavailable", "proxy service is not initialized"))
		return
	}

	var _lastErr error
	for _, _provider := range _h.Balancer.ProvidersSnapshot() {
		if _provider == nil || _provider.Config == nil || !_provider.Config.Enabled {
			continue
		}
		if !strings.EqualFold(inferProviderKind(*_provider.Config), "openai-codex") || providerAPIKey(*_provider.Config) != "" {
			continue
		}
		if _provider.HasAuthError() || _provider.CircuitOpen(time.Now()) {
			continue
		}

		_sourceRequest := _r.Clone(_r.Context())
		_sourceRequest.Header = _r.Header.Clone()
		_sourceRequest.Header.Del("If-None-Match")
		_manifest, _err := _h.Client.FetchCodexModelsManifest(_r.Context(), _provider, _sourceRequest)
		if _err != nil {
			_lastErr = _err
			continue
		}
		_manifest.Body, _err = proxy.AddAutoModelToCodexManifest(_manifest.Body)
		if _err != nil {
			_lastErr = _err
			continue
		}
		_etag := codexModelsManifestETag(_manifest.Body)
		for _, _name := range []string{"Content-Type", "Cache-Control"} {
			if _value := strings.TrimSpace(_manifest.Header.Get(_name)); _value != "" {
				_w.Header().Set(_name, _value)
			}
		}
		_w.Header().Set("ETag", _etag)
		if strings.TrimSpace(_w.Header().Get("Content-Type")) == "" {
			_w.Header().Set("Content-Type", "application/json")
		}
		if requestETagMatches(_r.Header.Get("If-None-Match"), _etag) {
			_w.WriteHeader(http.StatusNotModified)
			return
		}
		_w.WriteHeader(_manifest.StatusCode)
		if len(_manifest.Body) > 0 {
			_, _ = _w.Write(_manifest.Body)
		}
		return
	}

	_message := "no available OAuth Codex provider can load the model manifest"
	if _lastErr != nil {
		_message = _lastErr.Error()
	}
	_h.writeJSON(_w, http.StatusBadGateway, domain.ErrorResponse("provider_error", _message))
}

// -------------------------------------------------------------------------------------
func codexModelsManifestETag(_body []byte) string {
	_sum := sha256.Sum256(_body)
	return fmt.Sprintf("\"lbp-%x\"", _sum[:16])
}

// -------------------------------------------------------------------------------------
func requestETagMatches(_ifNoneMatch string, _etag string) bool {
	_etag = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(_etag), "W/"))
	for _, _candidate := range strings.Split(_ifNoneMatch, ",") {
		_candidate = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(_candidate), "W/"))
		if _candidate == "*" || (_candidate != "" && _candidate == _etag) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleRetrieveModel(_w http.ResponseWriter, _modelID string) {
	_modelID = strings.TrimSpace(_modelID)
	if _modelID == "" {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "model not found"))
		return
	}
	if strings.EqualFold(_modelID, "AUTO") {
		_h.writeJSON(_w, http.StatusOK, modelAPIObject("AUTO", "load-balance-provider", time.Now().Unix()))
		return
	}
	_settings, _err := config.LoadGeneralSettingsConfig(_h.generalSettingsConfigPath())
	if _err == nil && _settings.ShowProviderModels {
		for _, _model := range _h.providerModelListForModelsAPI() {
			if strings.EqualFold(stringValue(_model["id"]), _modelID) {
				_h.writeJSON(_w, http.StatusOK, _model)
				return
			}
		}
	}
	_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", fmt.Sprintf("model %q not found", _modelID)))
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) providerModelListForModelsAPI() []map[string]interface{} {
	if _h == nil || _h.Balancer == nil {
		return []map[string]interface{}{}
	}
	_created := time.Now().Unix()
	_seen := map[string]bool{"auto": true}
	_models := []map[string]interface{}{}
	_codexModels := make(map[string][]string)
	_codexModelsLoaded := make(map[string]bool)
	for _, _provider := range _h.Balancer.ProvidersSnapshot() {
		if _provider == nil || _provider.Config == nil || !_provider.Config.Enabled {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(_provider.Config.Role), "classifier") {
			continue
		}
		for _, _name := range _h.providerModelNamesForModelsAPI(_provider.Config, _codexModels, _codexModelsLoaded) {
			if _name == "" {
				continue
			}
			_key := strings.ToLower(_name)
			if _seen[_key] {
				continue
			}
			_seen[_key] = true
			_models = append(_models, modelAPIObject(_name, "load-balance-provider", _created))
		}
	}
	return _models
}

// -------------------------------------------------------------------------------------
func modelAPIObject(_id string, _ownedBy string, _created int64) map[string]interface{} {
	return map[string]interface{}{
		"id":       _id,
		"object":   "model",
		"created":  _created,
		"owned_by": _ownedBy,
		"root":     _id,
		"parent":   nil,
	}
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) providerModelNamesForModelsAPI(_provider *domain.LLMProviderConfig, _codexModels map[string][]string, _codexModelsLoaded map[string]bool) []string {
	if _provider == nil {
		return []string{}
	}
	if strings.EqualFold(inferProviderKind(*_provider), "openai-codex") && providerAPIKey(*_provider) == "" {
		_cacheKey := strings.TrimSpace(_provider.ID)
		if _cacheKey == "" {
			_cacheKey = strings.TrimSpace(_provider.Name)
		}
		if !_codexModelsLoaded[_cacheKey] {
			_codexModelsLoaded[_cacheKey] = true
			if _models, _, _err := _h.fetchProviderModels(*_provider); _err == nil {
				_codexModels[_cacheKey] = _models
			}
		}
		return append([]string(nil), _codexModels[_cacheKey]...)
	}
	return providerExposedModelNames(_provider)
}

// -------------------------------------------------------------------------------------
func providerExposedModelNames(_provider *domain.LLMProviderConfig) []string {
	if _provider == nil {
		return []string{}
	}

	_names := make([]string, 0)
	for _, _model := range _provider.Models {
		if publicProviderModelName(_provider, _model.Name) {
			_names = append(_names, _model.Name)
		}
		for _, _alias := range _model.Aliases {
			if publicProviderModelName(_provider, _alias) {
				_names = append(_names, _alias)
			}
		}
	}
	return uniqueSortedStrings(_names)
}

// -------------------------------------------------------------------------------------
func publicProviderModelName(_provider *domain.LLMProviderConfig, _name string) bool {
	_name = strings.TrimSpace(_name)
	if _name == "" || strings.EqualFold(_name, "auto") {
		return false
	}
	if _provider != nil {
		if strings.EqualFold(_name, strings.TrimSpace(_provider.Kind)) ||
			strings.EqualFold(_name, strings.TrimSpace(_provider.ID)) ||
			strings.EqualFold(_name, strings.TrimSpace(_provider.Name)) {
			return false
		}
	}
	return true
}

// -------------------------------------------------------------------------------------
func internalProviderModelAliases(_provider *domain.LLMProviderConfig, _aliases []string) []string {
	_internal := make([]string, 0, len(_aliases))
	for _, _alias := range _aliases {
		_alias = strings.TrimSpace(_alias)
		if _alias == "" {
			continue
		}
		if !publicProviderModelName(_provider, _alias) {
			_internal = append(_internal, _alias)
		}
	}
	return uniqueSortedStrings(_internal)
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleGetDashboardMetricBaselines(_w http.ResponseWriter) {
	_baselines, _err := dashboard.LoadMetricBaselines()
	if _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("load_failed", _err.Error()))
		return
	}
	_h.writeJSON(_w, http.StatusOK, _baselines)
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleGetProviderUsageHistory(_w http.ResponseWriter, _month string) {
	if _h == nil || _h.Balancer == nil {
		_h.writeJSON(_w, http.StatusServiceUnavailable, domain.ErrorResponse("service_unavailable", "load balancer is not initialized"))
		return
	}

	_providerIDs := make([]string, 0)
	for _, _provider := range _h.Balancer.ProvidersSnapshot() {
		if _provider == nil || _provider.Config == nil || !_provider.Config.Enabled {
			continue
		}
		_providerIDs = append(_providerIDs, _provider.Config.ID)
	}

	_stats, _err := _h.providerUsageRecorder().LoadMonth(_providerIDs, _month)
	if _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("load_failed", _err.Error()))
		return
	}
	_h.writeJSON(_w, http.StatusOK, _stats)
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) providerUsageRecorder() *providerusage.Recorder {
	if _h != nil && _h.ProviderUsageRecorder != nil {
		return _h.ProviderUsageRecorder
	}
	return providerusage.DefaultRecorder()
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleResetDashboardMetricBaselines(_w http.ResponseWriter, _body []byte) {
	var _req DashboardBaselineResetRequest
	if _err := json.Unmarshal(_body, &_req); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "dashboard baseline payload is not valid"))
		return
	}

	_baselines, _err := dashboard.LoadMetricBaselines()
	if _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("load_failed", _err.Error()))
		return
	}
	if _baselines.Providers == nil {
		_baselines.Providers = map[string]dashboard.ProviderBaseline{}
	}
	for _providerID, _baseline := range _req.Providers {
		if strings.TrimSpace(_providerID) == "" {
			continue
		}
		_baselines.Providers[_providerID] = _baseline
	}

	if _err := dashboard.SaveMetricBaselines(_baselines); _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("save_failed", _err.Error()))
		return
	}
	_h.dashboardCache.Lock()
	_h.dashboardCache.baselineRevision++
	_h.dashboardCache.baselines = _baselines
	_h.dashboardCache.baselinesAt = time.Now()
	_h.dashboardCache.Unlock()
	_h.writeJSON(_w, http.StatusOK, _baselines)
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleLogin(_w http.ResponseWriter, _r *http.Request, _body []byte) {
	var _request LoginRequest
	if _err := json.Unmarshal(_body, &_request); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "login payload is not valid"))
		return
	}

	_account := strings.TrimSpace(_h.DefaultAccount)
	_password := _h.DefaultPassword
	if _account == "" || _password == "" {
		_h.writeJSON(_w, http.StatusUnauthorized, domain.ErrorResponse("unauthorized", "login account is not configured"))
		return
	}

	if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(_request.Account)), []byte(_account)) != 1 ||
		subtle.ConstantTimeCompare([]byte(_request.Password), []byte(_password)) != 1 {
		_h.writeJSON(_w, http.StatusUnauthorized, domain.ErrorResponse("unauthorized", "account or password is invalid"))
		return
	}

	_expiresAt := time.Now().Add(24 * time.Hour)
	_key, _err := auth.DefaultAPIKeyStore().CreateTemporary("Web Login Session", 24*time.Hour)
	if _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("create_failed", _err.Error()))
		return
	}

	http.SetCookie(_w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    _key.Key,
		Path:     "/",
		Expires:  _expiresAt,
		MaxAge:   int((24 * time.Hour).Seconds()),
		HttpOnly: true,
		Secure:   isSecureRequest(_r),
		SameSite: http.SameSiteLaxMode,
	})

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"ok":         true,
		"expires_at": _expiresAt.Format(time.RFC3339),
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleLogout(_w http.ResponseWriter, _r *http.Request) {
	clearSessionCookie(_w, _r)
	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{"ok": true})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleSession(_w http.ResponseWriter) {
	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"ok": true,
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleGetGeneralSettings(_w http.ResponseWriter) {
	_config, _err := config.LoadGeneralSettingsConfig(_h.generalSettingsConfigPath())
	if _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("load_failed", _err.Error()))
		return
	}

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"general": generalSettingsForm(_config),
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleSaveGeneralSettings(_w http.ResponseWriter, _body []byte) {
	var _request GeneralSettingsForm
	if _err := json.Unmarshal(_body, &_request); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "general settings payload is not valid"))
		return
	}

	_saved := domain.GeneralSettingsConfig{
		ShowProviderModels: _request.ShowProviderModels,
	}
	if _err := config.SaveGeneralSettingsConfig(_h.generalSettingsConfigPath(), _saved); _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("save_failed", _err.Error()))
		return
	}

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"general": generalSettingsForm(_saved),
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleGetAdvancedSettings(_w http.ResponseWriter) {
	_settings, _err := _h.loadAdvancedSettings(true)
	if _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("load_failed", _err.Error()))
		return
	}
	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"advanced": advancedSettingsForm(_settings),
		"warning":  _h.advancedSettingsWarning(_settings),
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleSaveAdvancedSettings(_w http.ResponseWriter, _body []byte) {
	var _request AdvancedSettingsUpdateRequest
	if _err := json.Unmarshal(_body, &_request); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "advanced settings payload is not valid"))
		return
	}

	// 未出現在 payload 的欄位一律沿用現值（部分更新）。
	_saved := _h.currentAdvancedSettings()
	if _request.ProviderRetryRounds != nil {
		_saved.ProviderRetryRounds = *_request.ProviderRetryRounds
	}
	if _request.ProviderRetrySourcesPerRound != nil {
		_saved.ProviderRetrySourcesPerRound = *_request.ProviderRetrySourcesPerRound
	}
	if _request.ProviderRetryWaitSeconds != nil {
		_saved.ProviderRetryWaitSeconds = *_request.ProviderRetryWaitSeconds
	}
	if _request.PersistQuotaCooldown != nil {
		_saved.PersistQuotaCooldown = *_request.PersistQuotaCooldown
	}
	if _request.ConversationAffinityTTLMinutes != nil {
		_saved.ConversationAffinityTTLMinutes = *_request.ConversationAffinityTTLMinutes
	}
	if _request.ConversationAffinityQuotaTolerancePoints != nil {
		_saved.ConversationAffinityQuotaTolerancePoints = *_request.ConversationAffinityQuotaTolerancePoints
	}
	if _request.ResponseRouteMaxEntries != nil {
		_saved.ResponseRouteMaxEntries = *_request.ResponseRouteMaxEntries
	}
	if _request.ProviderCapacityCooldownSeconds != nil {
		_saved.ProviderCapacityCooldownSeconds = *_request.ProviderCapacityCooldownSeconds
	}
	if _request.ProviderServerErrorCooldownSeconds != nil {
		_saved.ProviderServerErrorCooldownSeconds = *_request.ProviderServerErrorCooldownSeconds
	}
	if _request.MaxBindingsPerProvider != nil {
		_saved.MaxBindingsPerProvider = *_request.MaxBindingsPerProvider
	}
	if _request.YieldLowMaxPercent != nil {
		_saved.YieldLowMaxPercent = *_request.YieldLowMaxPercent
	}
	if _request.YieldMidMaxPercent != nil {
		_saved.YieldMidMaxPercent = *_request.YieldMidMaxPercent
	}
	if _request.LowReasoningDemotionEnabled != nil {
		_saved.LowReasoningDemotionEnabled = *_request.LowReasoningDemotionEnabled
	}
	if _request.LowReasoningDemotionRequestsPerMin != nil {
		_saved.LowReasoningDemotionRequestsPerMin = *_request.LowReasoningDemotionRequestsPerMin
	}
	if _request.LowReasoningDemotionReasoningPercent != nil {
		_saved.LowReasoningDemotionReasoningPercent = *_request.LowReasoningDemotionReasoningPercent
	}
	if _request.LowReasoningDemotionTargetTier != nil {
		_saved.LowReasoningDemotionTargetTier = *_request.LowReasoningDemotionTargetTier
	}
	if _request.LowReasoningDemotionMinutes != nil {
		_saved.LowReasoningDemotionMinutes = *_request.LowReasoningDemotionMinutes
	}
	if _request.LowReasoningDemotionMinDailyUsagePercent != nil {
		_saved.LowReasoningDemotionMinDailyUsagePercent = *_request.LowReasoningDemotionMinDailyUsagePercent
	}
	if _err := config.ValidateAdvancedSettingsConfig(_saved); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", _err.Error()))
		return
	}
	if _err := config.SaveAdvancedSettingsConfig(_h.advancedSettingsConfigPath(), _saved); _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("save_failed", _err.Error()))
		return
	}
	_h.cacheAdvancedSettings(_saved)
	if !_saved.PersistQuotaCooldown {
		_h.quotaCooldowns.Lock()
		if err := _h.saveQuotaCooldownsLocked(false); err != nil {
			log.Printf("quota cooldown persistence disable failed: %v", err)
		}
		_h.quotaCooldowns.Unlock()
	}
	// 門檻改了就把既有降級清掉，否則舊門檻造成的降級會活到計時器到期為止。
	_defaultDemotionTracker.ClearDemotion("")
	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"advanced": advancedSettingsForm(_saved),
		"warning":  _h.advancedSettingsWarning(_saved),
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleGetNotificationTarget(_w http.ResponseWriter) {
	_config, _err := config.LoadNotificationTargetConfig(_h.notificationConfigPath())
	if _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("load_failed", _err.Error()))
		return
	}

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"notification": notificationTargetForm(_config),
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleSaveNotificationTarget(_w http.ResponseWriter, _body []byte) {
	var _request NotificationTargetForm
	if _err := json.Unmarshal(_body, &_request); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "notification target payload is not valid"))
		return
	}

	_saved, _err := _h.notificationConfigFromForm(_request)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", _err.Error()))
		return
	}
	if _err := config.SaveNotificationTargetConfig(_h.notificationConfigPath(), _saved); _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("save_failed", _err.Error()))
		return
	}

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"notification": notificationTargetForm(_saved),
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleTestNotificationTarget(_w http.ResponseWriter, _body []byte) {
	var _request NotificationTargetForm
	if _err := json.Unmarshal(_body, &_request); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "notification target payload is not valid"))
		return
	}

	_target, _err := _h.notificationConfigFromForm(_request)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", _err.Error()))
		return
	}
	if strings.TrimSpace(_target.URL) == "" {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "notification URL is required"))
		return
	}

	if _err := notification.SendMessage(_target, "LLM Proxy 通知測試"); _err != nil {
		_h.writeJSON(_w, http.StatusBadGateway, domain.ErrorResponse("notification_failed", _err.Error()))
		return
	}

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{"ok": true})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) notificationConfigFromForm(_request NotificationTargetForm) (domain.NotificationTargetConfig, error) {
	_payload := strings.TrimSpace(_request.Payload)
	if _payload == "" {
		_payload = config.DefaultNotificationTargetConfig().Payload
	}
	if !strings.Contains(_payload, "<msg>") {
		return domain.NotificationTargetConfig{}, fmt.Errorf("payload must include <msg> placeholder")
	}
	if strings.TrimSpace(_request.URL) != "" {
		if _err := security.ValidateOutboundURL(_request.URL); _err != nil {
			return domain.NotificationTargetConfig{}, fmt.Errorf("notification URL is not allowed: %w", _err)
		}
	}

	_apiKey := strings.TrimSpace(_request.APIKey)
	if _request.PreserveAPIKey && _apiKey == "" {
		_current, _err := config.LoadNotificationTargetConfig(_h.notificationConfigPath())
		if _err != nil {
			return domain.NotificationTargetConfig{}, fmt.Errorf("load current notification target failed: %w", _err)
		}
		_apiKey = _current.APIKey
	}

	return domain.NotificationTargetConfig{
		URL:     strings.TrimSpace(_request.URL),
		APIKey:  _apiKey,
		Payload: _payload,
	}, nil
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) notificationConfigPath() string {
	if _h.NotificationConfigPath != "" {
		return _h.NotificationConfigPath
	}
	return "data/notification_target.json"
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) generalSettingsConfigPath() string {
	return "data/general_settings.json"
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) advancedSettingsConfigPath() string {
	if strings.TrimSpace(_h.AdvancedSettingsConfigPath) != "" {
		return _h.AdvancedSettingsConfigPath
	}
	return "data/advanced_settings.json"
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) loadAdvancedSettings(_refresh bool) (domain.AdvancedSettingsConfig, error) {
	if !_refresh {
		_h.advancedSettingsLock.RLock()
		if _h.advancedSettingsLoaded {
			_settings := _h.advancedSettings
			_h.advancedSettingsLock.RUnlock()
			return _settings, nil
		}
		_h.advancedSettingsLock.RUnlock()
	}

	_settings, _err := config.LoadAdvancedSettingsConfig(_h.advancedSettingsConfigPath())
	if _err != nil {
		return domain.AdvancedSettingsConfig{}, _err
	}
	_h.cacheAdvancedSettings(_settings)
	return _settings, nil
}

// -------------------------------------------------------------------------------------
// providerCapacityCooldown 是上游回報容量／限流錯誤後，該 provider 暫停被選中的時間。
// 對話黏著會因為這段冷卻而降級重新負載平衡，因此設定值需要可調。
func (_h *HTTPAPI) providerCapacityCooldown() time.Duration {
	_seconds := _h.currentAdvancedSettings().ProviderCapacityCooldownSeconds
	if _seconds <= 0 {
		return defaultProviderCapacityCooldown
	}
	return time.Duration(_seconds) * time.Second
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) providerServerErrorCooldown() time.Duration {
	seconds := _h.currentAdvancedSettings().ProviderServerErrorCooldownSeconds
	if seconds <= 0 {
		seconds = 30
	}
	return time.Duration(seconds) * time.Second
}

func (_h *HTTPAPI) currentAdvancedSettings() domain.AdvancedSettingsConfig {
	_settings, _err := _h.loadAdvancedSettings(false)
	if _err == nil {
		return _settings
	}
	log.Printf("advanced settings load failed, using defaults: %v", _err)
	_settings = config.DefaultAdvancedSettingsConfig()
	_h.cacheAdvancedSettings(_settings)
	return _settings
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) cacheAdvancedSettings(_settings domain.AdvancedSettingsConfig) {
	_h.advancedSettingsLock.Lock()
	_h.advancedSettings = _settings
	_h.advancedSettingsLoaded = true
	_h.advancedSettingsLock.Unlock()
	if _h.Client != nil {
		_h.Client.ConfigureResponseRouteCache(
			time.Duration(_settings.ConversationAffinityTTLMinutes)*time.Minute,
			_settings.ResponseRouteMaxEntries,
		)
	}
}

// -------------------------------------------------------------------------------------
func generalSettingsForm(_config domain.GeneralSettingsConfig) GeneralSettingsForm {
	return GeneralSettingsForm{
		ShowProviderModels: _config.ShowProviderModels,
	}
}

// -------------------------------------------------------------------------------------
func advancedSettingsForm(_config domain.AdvancedSettingsConfig) AdvancedSettingsForm {
	return AdvancedSettingsForm{
		ProviderRetryRounds:                      _config.ProviderRetryRounds,
		ProviderRetrySourcesPerRound:             _config.ProviderRetrySourcesPerRound,
		ProviderRetryWaitSeconds:                 _config.ProviderRetryWaitSeconds,
		PersistQuotaCooldown:                     _config.PersistQuotaCooldown,
		ConversationAffinityTTLMinutes:           _config.ConversationAffinityTTLMinutes,
		ConversationAffinityQuotaTolerancePoints: _config.ConversationAffinityQuotaTolerancePoints,
		ResponseRouteMaxEntries:                  _config.ResponseRouteMaxEntries,
		ProviderCapacityCooldownSeconds:          _config.ProviderCapacityCooldownSeconds,
		ProviderServerErrorCooldownSeconds:       _config.ProviderServerErrorCooldownSeconds,
		MaxBindingsPerProvider:                   _config.MaxBindingsPerProvider,
		YieldLowMaxPercent:                       _config.YieldLowMaxPercent,
		YieldMidMaxPercent:                       _config.YieldMidMaxPercent,
		LowReasoningDemotionEnabled:              _config.LowReasoningDemotionEnabled,
		LowReasoningDemotionRequestsPerMin:       _config.LowReasoningDemotionRequestsPerMin,
		LowReasoningDemotionReasoningPercent:     _config.LowReasoningDemotionReasoningPercent,
		LowReasoningDemotionTargetTier:           _config.LowReasoningDemotionTargetTier,
		LowReasoningDemotionMinutes:              _config.LowReasoningDemotionMinutes,
		LowReasoningDemotionMinDailyUsagePercent: _config.LowReasoningDemotionMinDailyUsagePercent,
	}
}

// -------------------------------------------------------------------------------------
func notificationTargetForm(_config domain.NotificationTargetConfig) NotificationTargetForm {
	_hasAPIKey := strings.TrimSpace(_config.APIKey) != ""
	return NotificationTargetForm{
		URL:          _config.URL,
		APIKey:       "",
		APIKeyMasked: maskedPlainAPIKey(_config.APIKey),
		HasAPIKey:    _hasAPIKey,
		Payload:      _config.Payload,
	}
}

// -------------------------------------------------------------------------------------
func maskedPlainAPIKey(_apiKey string) string {
	if strings.TrimSpace(_apiKey) == "" {
		return ""
	}
	return "••••••••••••"
}

// -------------------------------------------------------------------------------------
func clearSessionCookie(_w http.ResponseWriter, _r *http.Request) {
	http.SetCookie(_w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
		HttpOnly: true,
		Secure:   isSecureRequest(_r),
		SameSite: http.SameSiteLaxMode,
	})
}

// -------------------------------------------------------------------------------------
func isSecureRequest(_r *http.Request) bool {
	if _r == nil {
		return false
	}
	if _r.TLS != nil {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(_r.Header.Get("X-Forwarded-Proto")), "https")
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleListAPIKeys(_w http.ResponseWriter) {
	_keys, _err := auth.DefaultAPIKeyStore().List()
	if _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("load_failed", _err.Error()))
		return
	}

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{"keys": _keys})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleAPIKeyRoutingOptions(_w http.ResponseWriter) {
	if _h.Balancer == nil {
		_h.writeJSON(_w, http.StatusServiceUnavailable, domain.ErrorResponse("service_unavailable", "proxy service is not initialized"))
		return
	}

	_snapshot := _h.Balancer.ConfigSnapshot()
	normalizeProviderOrder(_snapshot.Providers)
	_providers := make([]map[string]interface{}, 0, len(_snapshot.Providers))
	_allModels := make([]string, 0)
	for _, _provider := range _snapshot.Providers {
		_models := make([]string, 0)
		for _, _model := range _provider.Models {
			if _name := strings.TrimSpace(_model.Name); _name != "" {
				_models = append(_models, _name)
			}
			_models = append(_models, _model.Aliases...)
		}
		_models = uniqueSortedStrings(_models)
		if _provider.Enabled {
			_allModels = append(_allModels, _models...)
		}
		_providers = append(_providers, map[string]interface{}{
			"id":      _provider.ID,
			"name":    _provider.Name,
			"kind":    inferProviderKind(_provider),
			"enabled": _provider.Enabled,
			"models":  _models,
		})
	}

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"providers":           _providers,
		"models":              uniqueSortedStrings(_allModels),
		"reasoning_efforts":   []string{"AUTO", "none", "minimal", "low", "medium", "high", "xhigh"},
		"default_provider_id": "AUTO",
		"default_model":       "AUTO",
		"default_effort":      "AUTO",
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleCreateAPIKey(_w http.ResponseWriter, _body []byte) {
	var _request APIKeyCreateRequest
	if len(_body) > 0 {
		if _err := json.Unmarshal(_body, &_request); _err != nil {
			_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "api key payload is not valid"))
			return
		}
	}

	// 名稱在 API 層就要驗證；只靠前端擋，直接呼叫 API 仍會產生「未命名金鑰」。
	_request.Name = strings.TrimSpace(_request.Name)
	if _request.Name == "" {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "api key name is required"))
		return
	}

	_keyType := strings.ToLower(strings.TrimSpace(_request.Type))
	if _keyType == "" {
		_keyType = auth.APIKeyTypeChat
	}
	if _keyType != auth.APIKeyTypeChat && _keyType != auth.APIKeyTypeMCP {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "api key type must be chat or mcp"))
		return
	}

	_key, _err := auth.DefaultAPIKeyStore().CreateForType(_request.Name, _keyType)
	if _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("create_failed", _err.Error()))
		return
	}

	_h.writeJSON(_w, http.StatusCreated, map[string]interface{}{"key": _key})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleSetAPIKeyEnabled(_w http.ResponseWriter, _id string, _enabled bool) {
	if strings.TrimSpace(_id) == "" {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "api key id is required"))
		return
	}

	_key, _err := auth.DefaultAPIKeyStore().SetEnabled(_id, _enabled)
	if _err != nil {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", _err.Error()))
		return
	}

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{"key": _key})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleUpdateAPIKey(_w http.ResponseWriter, _id string, _body []byte) {
	if strings.TrimSpace(_id) == "" {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "api key id is required"))
		return
	}

	var _request APIKeyUpdateRequest
	if _err := json.Unmarshal(_body, &_request); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "api key payload is not valid"))
		return
	}
	_existing, _found := findAPIKeyViewByID(_id)
	if !_found {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "api key not found"))
		return
	}

	// 未出現在 payload 的欄位一律沿用原值（部分更新）。
	_name := _existing.Name
	if _request.Name != nil {
		_name = strings.TrimSpace(*_request.Name)
		if _name == "" {
			_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "api key name cannot be blank"))
			return
		}
	}
	_providerID := normalizeAPIKeyRoutingValue(_existing.ProviderID)
	if _request.ProviderID != nil {
		_providerID = normalizeAPIKeyRoutingValue(*_request.ProviderID)
	}
	_model := normalizeAPIKeyRoutingValue(_existing.Model)
	if _request.Model != nil {
		_model = normalizeAPIKeyRoutingValue(*_request.Model)
	}
	_effort := _existing.ReasoningEffort
	if _request.ReasoningEffort != nil {
		_effort = *_request.ReasoningEffort
	}
	_reasoningEffort, _err := normalizeAPIKeyReasoningSetting(_effort)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", _err.Error()))
		return
	}
	if !strings.EqualFold(_providerID, "AUTO") {
		if _, _ok := _h.findProviderConfig(_providerID); !_ok {
			_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "selected provider does not exist"))
			return
		}
	}
	// 指定的模型也要驗證存在，否則要等到每次請求都選不到 provider（通用 503）才會發現打錯字。
	if !strings.EqualFold(_model, "AUTO") && !_h.apiKeyModelExists(_providerID, _model) {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", apiKeyModelNotFoundMessage(_providerID, _model)))
		return
	}

	_key, _err := auth.DefaultAPIKeyStore().Update(_id, _name, _providerID, _model, _reasoningEffort)
	if _err != nil {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", _err.Error()))
		return
	}

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{"key": _key})
}

// -------------------------------------------------------------------------------------
func findAPIKeyViewByID(_id string) (auth.APIKeyView, bool) {
	_keys, _err := auth.DefaultAPIKeyStore().List()
	if _err != nil {
		return auth.APIKeyView{}, false
	}
	for _, _key := range _keys {
		if _key.ID == _id {
			return _key, true
		}
	}
	return auth.APIKeyView{}, false
}

// -------------------------------------------------------------------------------------
// apiKeyModelExists 檢查金鑰要綁定的模型是否真的存在。
// _providerID 為 AUTO 時，只要任一 provider 提供該模型即可；否則必須屬於指定的 provider。
func (_h *HTTPAPI) apiKeyModelExists(_providerID string, _model string) bool {
	if _h == nil || _h.Balancer == nil {
		return false
	}
	_model = strings.TrimSpace(_model)
	if _model == "" {
		return false
	}
	_pinnedProvider := !strings.EqualFold(_providerID, "AUTO")
	for _, _provider := range _h.Balancer.ConfigSnapshot().Providers {
		if _pinnedProvider && !strings.EqualFold(_provider.ID, _providerID) {
			continue
		}
		for _, _candidate := range _provider.Models {
			if _candidate.MatchName(_model) {
				return true
			}
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func apiKeyModelNotFoundMessage(_providerID string, _model string) string {
	if strings.EqualFold(_providerID, "AUTO") {
		return fmt.Sprintf("selected model %q is not provided by any configured provider", _model)
	}
	return fmt.Sprintf("selected model %q is not provided by provider %q", _model, _providerID)
}

// -------------------------------------------------------------------------------------
func normalizeAPIKeyReasoningSetting(_effort string) (string, error) {
	_effort = strings.ToLower(strings.TrimSpace(_effort))
	if _effort == "" || _effort == "auto" {
		return "AUTO", nil
	}
	switch _effort {
	case "none", "minimal", "low", "medium", "high", "xhigh":
		return _effort, nil
	default:
		return "", fmt.Errorf("reasoning effort must be AUTO, none, minimal, low, medium, high, or xhigh")
	}
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleDeleteAPIKey(_w http.ResponseWriter, _id string) {
	if strings.TrimSpace(_id) == "" {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "api key id is required"))
		return
	}

	if _err := auth.DefaultAPIKeyStore().Delete(_id); _err != nil {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", _err.Error()))
		return
	}

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{"deleted": true})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleGetAPIKeyUsage(_w http.ResponseWriter, _id string, _month string) {
	if strings.TrimSpace(_id) == "" {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "api key id is required"))
		return
	}
	_key, _found := findAPIKeyViewByID(_id)
	if !_found {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "api key not found"))
		return
	}
	if _key.KeyType == auth.APIKeyTypeMCP {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("statistics_not_supported", "MCP keys do not collect usage statistics"))
		return
	}

	_stats, _err := keyusage.DefaultRecorder().LoadMonth(_id, _month)
	if _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("load_failed", _err.Error()))
		return
	}

	_h.writeJSON(_w, http.StatusOK, _stats)
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) apiKeyExists(_id string) bool {
	_keys, _err := auth.DefaultAPIKeyStore().List()
	if _err != nil {
		return false
	}
	for _, _key := range _keys {
		if _key.ID == _id {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleProviderModels(_w http.ResponseWriter, _id string) {
	_provider, _ok := _h.findProviderConfig(_id)
	if !_ok {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "provider config not found"))
		return
	}

	_models, _status, _err := _h.fetchProviderModels(_provider)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadGateway, map[string]interface{}{
			"ok":      false,
			"status":  _status,
			"message": _err.Error(),
			"models":  []string{},
		})
		return
	}
	if _err := _h.syncFetchedProviderModels(_id, _models); _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("save_failed", _err.Error()))
		return
	}

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"ok":     true,
		"status": _status,
		"models": _models,
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleProviderTest(_w http.ResponseWriter, _id string) {
	_provider, _ok := _h.findProviderConfig(_id)
	if !_ok {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "provider config not found"))
		return
	}

	_runtime, _ok := _h.findProviderRuntime(_id)
	if !_ok {
		_runtime = &balancer.ProviderRuntime{Config: &_provider}
	}
	_client := _h.Client
	if _client == nil {
		_client = proxy.NewClient()
	}
	_ctx, _cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer _cancel()
	if _err := _client.TestProviderMinimalChat(_ctx, _runtime); _err != nil {
		_h.writeJSON(_w, http.StatusBadGateway, map[string]interface{}{
			"ok":      false,
			"status":  http.StatusBadGateway,
			"message": _err.Error(),
		})
		return
	}

	_models, _status, _err := _h.fetchProviderModels(_provider)
	if _err != nil {
		_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
			"ok":           true,
			"status":       http.StatusOK,
			"chat_test":    true,
			"model_status": _status,
			"model_count":  0,
			"warning":      _err.Error(),
		})
		return
	}
	if _err := _h.syncFetchedProviderModels(_id, _models); _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("save_failed", _err.Error()))
		return
	}

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"ok":           true,
		"status":       http.StatusOK,
		"chat_test":    true,
		"model_status": _status,
		"model_count":  len(_models),
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleGetProviderRateLimitReset(_w http.ResponseWriter, _id string) {
	_provider, _ok := _h.findCodexOAuthProviderForReset(_w, _id)
	if !_ok {
		return
	}
	_client := _h.Client
	if _client == nil {
		_client = proxy.NewClient()
	}
	_ctx, _cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer _cancel()
	_credits, _err := _client.GetCodexRateLimitResetCredits(_ctx, &_provider)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadGateway, domain.ErrorResponse("codex_reset_error", _err.Error()))
		return
	}
	_h.clearProviderAuthError(_provider.ID)
	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"ok":               true,
		"availableCount":   _credits.AvailableCount,
		"expiresAt":        _credits.NextExpiresAt,
		"hasCreditDetails": _credits.HasCreditDetails,
		"credits":          _credits.Credits,
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleConsumeProviderRateLimitReset(_w http.ResponseWriter, _id string, _body []byte) {
	_provider, _ok := _h.findCodexOAuthProviderForReset(_w, _id)
	if !_ok {
		return
	}
	var _request ProviderRateLimitResetRequest
	if _err := json.Unmarshal(_body, &_request); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "rate-limit reset payload is not valid"))
		return
	}
	_request.IdempotencyKey = strings.TrimSpace(_request.IdempotencyKey)
	_request.CreditID = strings.TrimSpace(_request.CreditID)
	if len(_request.CreditID) > 200 {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "creditId must not exceed 200 characters"))
		return
	}
	if _request.IdempotencyKey == "" || len(_request.IdempotencyKey) > 200 {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "idempotencyKey is required and must not exceed 200 characters"))
		return
	}

	_client := _h.Client
	if _client == nil {
		_client = proxy.NewClient()
	}
	_ctx, _cancel := context.WithTimeout(context.Background(), 18*time.Second)
	defer _cancel()
	_result, _err := _client.ConsumeCodexRateLimitResetCredit(_ctx, &_provider, _request.IdempotencyKey, _request.CreditID)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadGateway, domain.ErrorResponse("codex_reset_error", _err.Error()))
		return
	}
	_h.clearProviderAuthError(_provider.ID)
	if _result.Outcome == "reset" && _result.WindowsReset > 0 {
		_h.clearQuotaCooldown(_provider.ID)
	}
	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"ok":           true,
		"outcome":      _result.Outcome,
		"windowsReset": _result.WindowsReset,
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) findCodexOAuthProviderForReset(_w http.ResponseWriter, _id string) (domain.LLMProviderConfig, bool) {
	_provider, _ok := _h.findProviderConfig(strings.TrimSpace(_id))
	if !_ok {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "provider config not found"))
		return domain.LLMProviderConfig{}, false
	}
	if !strings.EqualFold(inferProviderKind(_provider), "openai-codex") {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "rate-limit reset is supported for OpenAI Codex providers only"))
		return domain.LLMProviderConfig{}, false
	}
	_status, _err := codexauth.StatusFor(_provider.ID)
	if _err != nil || _status.Status != "connected" {
		_h.writeJSON(_w, http.StatusConflict, domain.ErrorResponse("oauth_required", "OpenAI Codex OAuth is not connected"))
		return domain.LLMProviderConfig{}, false
	}
	return _provider, true
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleProviderOAuthStart(_w http.ResponseWriter, _body []byte) {
	var _request ProviderOAuthStartRequest
	if _err := json.Unmarshal(_body, &_request); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "oauth start payload is not valid"))
		return
	}

	_provider, _ok := _h.findProviderConfig(_request.ID)
	if !_ok {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "provider config not found"))
		return
	}
	if !strings.EqualFold(inferProviderKind(_provider), "openai-codex") {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "OAuth is currently supported for OpenAI Codex providers only"))
		return
	}

	_result, _err := codexauth.Start(codexauth.StartOptions{
		ProviderID:     _provider.ID,
		FlowPreference: _request.FlowPreference,
		LaunchBrowser:  _request.LaunchBrowser,
	})
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadGateway, domain.ErrorResponse("oauth_error", _err.Error()))
		return
	}
	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{"success": true, "oauth": _result})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleProviderOAuthStatus(_w http.ResponseWriter, _r *http.Request) {
	_id := strings.TrimSpace(_r.URL.Query().Get("id"))
	if _id == "" {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "provider id is required"))
		return
	}

	_provider, _ok := _h.findProviderConfig(_id)
	if !_ok {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "provider config not found"))
		return
	}
	if !strings.EqualFold(inferProviderKind(_provider), "openai-codex") {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "OAuth is currently supported for OpenAI Codex providers only"))
		return
	}

	_status, _err := codexauth.StatusFor(_provider.ID)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadGateway, domain.ErrorResponse("oauth_error", _err.Error()))
		return
	}
	if _status.Status == "connected" {
		_h.clearProviderAuthError(_provider.ID)
	}
	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{"success": true, "oauth": _status})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleProviderOAuthComplete(_w http.ResponseWriter, _body []byte) {
	var _request ProviderOAuthCompleteRequest
	if _err := json.Unmarshal(_body, &_request); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "oauth complete payload is not valid"))
		return
	}

	_provider, _ok := _h.findProviderConfig(_request.ID)
	if !_ok {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "provider config not found"))
		return
	}
	if !strings.EqualFold(inferProviderKind(_provider), "openai-codex") {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "OAuth is currently supported for OpenAI Codex providers only"))
		return
	}

	_record, _err := codexauth.CompleteManual(_provider.ID, _request.Input)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadGateway, domain.ErrorResponse("oauth_error", _err.Error()))
		return
	}
	_h.clearProviderAuthError(_provider.ID)
	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{"success": true, "oauth": map[string]interface{}{
		"provider_id":   _record.ProviderID,
		"status":        "connected",
		"account_email": _record.AccountEmail,
		"account_name":  _record.AccountName,
		"expires_at":    _record.ExpiresAt,
		"token_type":    _record.TokenType,
		"scope":         _record.Scope,
	}})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) clearProviderAuthError(_id string) {
	if _runtime, _ok := _h.findProviderRuntime(_id); _ok {
		_runtime.ClearAuthError()
	}
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) findProviderConfig(_id string) (domain.LLMProviderConfig, bool) {
	_snapshot := _h.Balancer.ConfigSnapshot()
	for _, _provider := range _snapshot.Providers {
		if _provider.ID == _id {
			return _provider, true
		}
	}
	return domain.LLMProviderConfig{}, false
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) findProviderRuntime(_id string) (*balancer.ProviderRuntime, bool) {
	if _h == nil || _h.Balancer == nil {
		return nil, false
	}
	for _, _provider := range _h.Balancer.ProvidersSnapshot() {
		if _provider == nil || _provider.Config == nil {
			continue
		}
		if _provider.Config.ID == _id {
			return _provider, true
		}
	}
	return nil, false
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) fetchProviderModels(_provider domain.LLMProviderConfig) ([]string, int, error) {
	_kind := inferProviderKind(_provider)
	if strings.EqualFold(_kind, "openai-codex") && providerAPIKey(_provider) == "" {
		return _h.fetchOAuthCodexProviderModels(_provider)
	}

	_modelsURL, _err := providerModelsURL(_provider)
	if _err != nil {
		return nil, 0, _err
	}

	_ctx, _cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer _cancel()

	_req, _err := http.NewRequestWithContext(_ctx, http.MethodGet, _modelsURL, nil)
	if _err != nil {
		return nil, 0, _err
	}

	_req.Header.Set("Accept", "application/json")
	if _apiKey := providerAPIKey(_provider); _apiKey != "" {
		_req.Header.Set("Authorization", "Bearer "+_apiKey)
	}

	_client := security.GuardedHTTPClient(&http.Client{Timeout: 15 * time.Second})
	_resp, _err := _client.Do(_req)
	if _err != nil {
		return nil, 0, _err
	}
	defer _resp.Body.Close()

	_body, _err := io.ReadAll(io.LimitReader(_resp.Body, 4*1024*1024))
	if _err != nil {
		return nil, _resp.StatusCode, _err
	}

	if _resp.StatusCode < http.StatusOK || _resp.StatusCode >= http.StatusMultipleChoices {
		return nil, _resp.StatusCode, fmt.Errorf("provider models endpoint returned status %d", _resp.StatusCode)
	}

	_models, _err := normalizeProviderModelList(_body)
	if _err != nil {
		return nil, _resp.StatusCode, _err
	}
	if len(_models) == 0 {
		return nil, _resp.StatusCode, fmt.Errorf("provider models endpoint returned empty model list")
	}

	return _models, _resp.StatusCode, nil
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) fetchOAuthCodexProviderModels(_provider domain.LLMProviderConfig) ([]string, int, error) {
	_ctx, _cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer _cancel()

	_runtime, _ok := _h.findProviderRuntime(_provider.ID)
	if !_ok {
		_providerCopy := _provider
		_runtime = &balancer.ProviderRuntime{Config: &_providerCopy}
	}

	_client := _h.Client
	if _client == nil {
		_client = proxy.NewClient()
	}
	_manifest, _err := _client.FetchCodexModelsManifest(_ctx, _runtime, nil)
	if _err != nil {
		return nil, _manifest.StatusCode, _err
	}
	_models, _err := proxy.CodexUsableModelNames(_manifest.Body)
	if _err != nil {
		return nil, _manifest.StatusCode, _err
	}
	return _models, _manifest.StatusCode, nil
}

// -------------------------------------------------------------------------------------
func normalizeOpenAICodexModelID(_model string) string {
	return strings.ToLower(strings.TrimSpace(_model))
}

// -------------------------------------------------------------------------------------
func providerModelsURL(_provider domain.LLMProviderConfig) (string, error) {
	_base := strings.TrimSpace(_provider.BaseURL)
	if _base == "" {
		return "", fmt.Errorf("provider host is empty")
	}

	_parsed, _err := url.Parse(_base)
	if _err != nil {
		return "", _err
	}
	if _parsed.Scheme == "" || _parsed.Host == "" {
		return "", fmt.Errorf("provider host is not a valid absolute URL")
	}
	if _err := security.ValidateOutboundParsedURL(_parsed); _err != nil {
		return "", _err
	}

	_parsed.Path = "/v1/models"
	_parsed.RawQuery = ""
	_parsed.Fragment = ""
	return _parsed.String(), nil
}

// -------------------------------------------------------------------------------------
func providerAPIKey(_provider domain.LLMProviderConfig) string {
	if _provider.APIKey != "" {
		return _provider.APIKey
	}
	if _provider.APIKeyEnv != "" {
		return os.Getenv(_provider.APIKeyEnv)
	}
	return ""
}

// -------------------------------------------------------------------------------------
func normalizeProviderModelList(_body []byte) ([]string, error) {
	var _payload interface{}
	if _err := json.Unmarshal(_body, &_payload); _err != nil {
		return nil, _err
	}

	_models := make([]string, 0)
	switch _value := _payload.(type) {
	case []interface{}:
		_models = append(_models, modelNamesFromArray(_value)...)
	case map[string]interface{}:
		if _items, _ok := _value["data"].([]interface{}); _ok {
			_models = append(_models, modelNamesFromArray(_items)...)
		}
		if _items, _ok := _value["models"].([]interface{}); _ok {
			_models = append(_models, modelNamesFromArray(_items)...)
		}
	}

	return uniqueSortedStrings(_models), nil
}

// -------------------------------------------------------------------------------------
func modelNamesFromArray(_items []interface{}) []string {
	_models := make([]string, 0, len(_items))
	for _, _item := range _items {
		switch _model := _item.(type) {
		case string:
			_models = append(_models, _model)
		case map[string]interface{}:
			for _, _key := range []string{"id", "name", "model"} {
				if _name, _ok := _model[_key].(string); _ok && strings.TrimSpace(_name) != "" {
					_models = append(_models, _name)
					break
				}
			}
		}
	}
	return _models
}

// -------------------------------------------------------------------------------------
func uniqueSortedStrings(_values []string) []string {
	_seen := map[string]bool{}
	_result := make([]string, 0, len(_values))
	for _, _value := range _values {
		_value = strings.TrimSpace(_value)
		if _value == "" || _seen[_value] {
			continue
		}
		_seen[_value] = true
		_result = append(_result, _value)
	}
	sort.Strings(_result)
	return _result
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleListProviderConfigs(_w http.ResponseWriter) {
	_snapshot := _h.Balancer.ConfigSnapshot()
	normalizeProviderOrder(_snapshot.Providers)

	_items := make([]ProviderForm, 0, len(_snapshot.Providers))
	for _, _provider := range _snapshot.Providers {
		_items = append(_items, providerConfigToForm(_provider))
	}

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{"providers": _items})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleCreateProviderConfig(_w http.ResponseWriter, _body []byte) {
	var _form ProviderForm
	if _err := json.Unmarshal(_body, &_form); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "provider config payload is not valid"))
		return
	}

	_form.ID = strings.TrimSpace(_form.ID)
	if _form.ID == "" {
		_form.ID = generateProviderID(_form.Kind)
	}

	_snapshot := _h.Balancer.ConfigSnapshot()
	for _, _provider := range _snapshot.Providers {
		if strings.EqualFold(_provider.ID, _form.ID) {
			_h.writeJSON(_w, http.StatusConflict, domain.ErrorResponse("conflict", "provider id already exists"))
			return
		}
	}

	_providerConfig := formToProviderConfig(_form)
	if _err := validateProviderOutboundURL(_providerConfig); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", _err.Error()))
		return
	}
	_snapshot.Providers = append(_snapshot.Providers, _providerConfig)
	if !_h.saveAndReload(_w, &_snapshot) {
		return
	}

	_h.writeJSON(_w, http.StatusCreated, providerConfigToForm(_snapshot.Providers[len(_snapshot.Providers)-1]))
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) benchmarkManager() *benchmarkrunner.Manager {
	if _h.BenchmarkManager == nil {
		_h.BenchmarkManager = benchmarkrunner.NewManager()
	}
	return _h.BenchmarkManager
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleStartIntelligenceBenchmark(_w http.ResponseWriter, _body []byte) {
	var _request benchmarkrunner.StartRequest
	if _err := json.Unmarshal(_body, &_request); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "benchmark payload is not valid"))
		return
	}

	_provider, _ok := _h.findProviderConfig(_request.ProviderID)
	if !_ok {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "provider config not found"))
		return
	}
	if !_provider.Enabled {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "provider is disabled"))
		return
	}

	if strings.TrimSpace(_request.Model) == "" && len(_provider.Models) > 0 {
		_request.Model = _provider.Models[0].Name
	}
	_request.ProviderName = _provider.Name
	_request.ProviderBaseURL = _provider.BaseURL
	_request.ProviderAPIKey = benchmarkrunner.ProviderAPIKey(_provider.APIKey, _provider.APIKeyEnv)
	_request.ChatAPI = _provider.ChatCompletionsPath
	_request.BenchmarkRoot = benchmarkrunner.DefaultDataRoot

	_job, _err := _h.benchmarkManager().Start(_request)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", _err.Error()))
		return
	}
	_h.writeJSON(_w, http.StatusAccepted, _job)
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleGetIntelligenceBenchmark(_w http.ResponseWriter, _id string) {
	_job, _ok := _h.benchmarkManager().Get(_id)
	if !_ok {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "benchmark job not found"))
		return
	}
	_h.writeJSON(_w, http.StatusOK, _job)
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleCancelIntelligenceBenchmark(_w http.ResponseWriter, _id string) {
	_job, _ok := _h.benchmarkManager().Cancel(_id)
	if !_ok {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "benchmark job not found"))
		return
	}
	_h.writeJSON(_w, http.StatusOK, _job)
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleReorderProviderConfigs(_w http.ResponseWriter, _body []byte) {
	var _request ProviderReorderRequest
	if _err := json.Unmarshal(_body, &_request); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "provider reorder payload is not valid"))
		return
	}

	_snapshot := _h.Balancer.ConfigSnapshot()
	_byID := make(map[string]domain.LLMProviderConfig, len(_snapshot.Providers))
	for _, _provider := range _snapshot.Providers {
		_byID[_provider.ID] = _provider
	}

	_seen := make(map[string]bool, len(_request.IDs))
	_reordered := make([]domain.LLMProviderConfig, 0, len(_snapshot.Providers))
	for _, _id := range _request.IDs {
		_id = strings.TrimSpace(_id)
		if _id == "" || _seen[_id] {
			continue
		}
		_provider, _ok := _byID[_id]
		if !_ok {
			continue
		}
		_seen[_id] = true
		_reordered = append(_reordered, _provider)
	}

	for _, _provider := range _snapshot.Providers {
		if !_seen[_provider.ID] {
			_reordered = append(_reordered, _provider)
		}
	}

	for _idx := range _reordered {
		_reordered[_idx].Priority = _idx + 1
	}
	_snapshot.Providers = _reordered

	if !_h.saveAndReload(_w, &_snapshot) {
		return
	}

	_items := make([]ProviderForm, 0, len(_snapshot.Providers))
	for _, _provider := range _snapshot.Providers {
		_items = append(_items, providerConfigToForm(_provider))
	}
	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{"providers": _items})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleUpdateProviderConfig(_w http.ResponseWriter, _id string, _body []byte) {
	if _id == "" {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "provider id is required"))
		return
	}

	var _form ProviderForm
	if _err := json.Unmarshal(_body, &_form); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "provider config payload is not valid"))
		return
	}

	_form.ID = _id
	_snapshot := _h.Balancer.ConfigSnapshot()
	for _idx := range _snapshot.Providers {
		if _snapshot.Providers[_idx].ID == _id {
			_updated := mergeProviderConfig(_snapshot.Providers[_idx], _form)
			if _err := validateProviderOutboundURL(_updated); _err != nil {
				_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", _err.Error()))
				return
			}
			_snapshot.Providers[_idx] = _updated
			if !_h.saveAndReload(_w, &_snapshot) {
				return
			}
			_h.writeJSON(_w, http.StatusOK, providerConfigToForm(_snapshot.Providers[_idx]))
			return
		}
	}

	_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "provider config not found"))
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleDeleteProviderConfig(_w http.ResponseWriter, _id string) {
	if _id == "" {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "provider id is required"))
		return
	}

	_snapshot := _h.Balancer.ConfigSnapshot()
	_next := make([]domain.LLMProviderConfig, 0, len(_snapshot.Providers))
	_deleted := false
	for _, _provider := range _snapshot.Providers {
		if _provider.ID == _id {
			_deleted = true
			continue
		}
		_next = append(_next, _provider)
	}

	if !_deleted {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "provider config not found"))
		return
	}

	_snapshot.Providers = _next
	if !_h.saveAndReload(_w, &_snapshot) {
		return
	}

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{"deleted": true, "id": _id})
}

// -------------------------------------------------------------------------------------
func validateProviderOutboundURL(_provider domain.LLMProviderConfig) error {
	if strings.TrimSpace(_provider.BaseURL) == "" {
		return fmt.Errorf("provider host is empty")
	}
	if _err := security.ValidateOutboundURL(_provider.BaseURL); _err != nil {
		return fmt.Errorf("provider host is not allowed: %w", _err)
	}
	return nil
}

// -------------------------------------------------------------------------------------
func normalizeProviderOrder(_providers []domain.LLMProviderConfig) {
	sort.SliceStable(_providers, func(_left int, _right int) bool {
		_leftPriority := _providers[_left].Priority
		_rightPriority := _providers[_right].Priority
		if _leftPriority <= 0 && _rightPriority <= 0 {
			return _left < _right
		}
		if _leftPriority <= 0 {
			return false
		}
		if _rightPriority <= 0 {
			return true
		}
		return _leftPriority < _rightPriority
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) syncFetchedProviderModels(_id string, _models []string) error {
	if _h == nil || _h.Balancer == nil || strings.TrimSpace(_id) == "" || len(_models) == 0 {
		return nil
	}

	_snapshot := _h.Balancer.ConfigSnapshot()
	_changed := false
	for _idx := range _snapshot.Providers {
		if _snapshot.Providers[_idx].ID != _id {
			continue
		}
		_before := providerModelFingerprint(_snapshot.Providers[_idx])
		syncProviderModelAliases(&_snapshot.Providers[_idx], _models)
		_changed = _before != providerModelFingerprint(_snapshot.Providers[_idx])
		break
	}
	if !_changed {
		return nil
	}

	_path := _h.ConfigPath
	if _path == "" {
		_path = "data/llm_proxy.json"
	}
	if _err := config.SaveProxyConfig(_path, &_snapshot); _err != nil {
		return _err
	}
	_h.Balancer.ReloadConfig(&_snapshot)
	return nil
}

// -------------------------------------------------------------------------------------
func syncProviderModelAliases(_provider *domain.LLMProviderConfig, _models []string) {
	if _provider == nil {
		return
	}
	_kind := inferProviderKind(*_provider)
	if len(_provider.Models) == 0 {
		_name := firstPublicProviderModelName(_provider, _models)
		if _name == "" {
			return
		}
		_provider.Models = []domain.LLMModelConfig{{
			Name:            _name,
			Aliases:         []string{"auto", _kind},
			MaxInputTokens:  maxInputTokensForKind(_kind),
			MaxOutputTokens: maxOutputTokensForKind(_kind),
			Capabilities:    capabilitiesForKindAndPurpose(_kind, _provider.Purpose),
			CostTier:        costTierForScale(_provider.Scale),
			QualityTier:     qualityTierForScale(_provider.Scale),
		}}
	}

	_publicModels := publicProviderModelNamesFromFetchedList(_provider, _models)
	if len(_publicModels) > 0 {
		if _matched := matchingModelName(_publicModels, _provider.Models[0].Name); _matched != "" {
			_provider.Models[0].Name = _matched
		} else {
			_provider.Models[0].Name = _publicModels[0]
		}
	}

	_aliases := internalProviderModelAliases(_provider, _provider.Models[0].Aliases)
	for _, _model := range _publicModels {
		_aliases = append(_aliases, _model)
	}
	_provider.Models[0].Aliases = uniqueSortedStrings(_aliases)
}

// -------------------------------------------------------------------------------------
func publicProviderModelNamesFromFetchedList(_provider *domain.LLMProviderConfig, _models []string) []string {
	_names := make([]string, 0, len(_models))
	for _, _model := range _models {
		if publicProviderModelName(_provider, _model) {
			_names = append(_names, _model)
		}
	}
	return uniqueSortedStrings(_names)
}

// -------------------------------------------------------------------------------------
func matchingModelName(_models []string, _model string) string {
	_model = strings.TrimSpace(_model)
	if _model == "" {
		return ""
	}
	for _, _candidate := range _models {
		if strings.EqualFold(strings.TrimSpace(_candidate), _model) {
			return strings.TrimSpace(_candidate)
		}
	}
	return ""
}

// -------------------------------------------------------------------------------------
func firstPublicProviderModelName(_provider *domain.LLMProviderConfig, _models []string) string {
	_names := publicProviderModelNamesFromFetchedList(_provider, _models)
	if len(_names) > 0 {
		return _names[0]
	}
	return ""
}

// -------------------------------------------------------------------------------------
func providerModelFingerprint(_provider domain.LLMProviderConfig) string {
	_parts := make([]string, 0, len(_provider.Models)*2)
	for _, _model := range _provider.Models {
		_parts = append(_parts, strings.TrimSpace(_model.Name))
		_parts = append(_parts, strings.Join(uniqueSortedStrings(_model.Aliases), "\x00"))
	}
	return strings.Join(_parts, "\x01")
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) saveAndReload(_w http.ResponseWriter, _proxyConfig *domain.ProxyConfig) bool {
	_path := _h.ConfigPath
	if _path == "" {
		_path = "data/llm_proxy.json"
	}

	if _err := config.SaveProxyConfig(_path, _proxyConfig); _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("save_failed", _err.Error()))
		return false
	}

	_h.Balancer.ReloadConfig(_proxyConfig)
	return true
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleChatCompletions(_w http.ResponseWriter, _r *http.Request, _body []byte) {
	if _h.Balancer == nil || _h.Client == nil {
		_h.writeJSON(_w, http.StatusServiceUnavailable, domain.ErrorResponse("service_unavailable", "proxy service is not initialized"))
		return
	}

	_body, _err := applyAPIKeyRoutingPolicyToJSONRequest(_r, _body, false)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", _err.Error()))
		return
	}

	var _chatReq domain.ChatCompletionRequest
	if _err := json.Unmarshal(_body, &_chatReq); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "request body is not a valid chat completion payload"))
		return
	}
	_requestSignals := telemetry.AnalyzeRequestJSON(_body)
	_started := time.Now()
	_releaseContinuity := func(string) bool {
		return strings.TrimSpace(_chatReq.ProviderID) == "" && strings.TrimSpace(_chatReq.Provider) == ""
	}
	_h.executeProviderRequest(_w, _r, _chatReq, _started, _requestSignals, proxy.ChatRefusalTerminal, proxy.ChatRefusalTerminal, proxy.ChatStreamHeartbeat(), _releaseContinuity, func(_ctx context.Context, _attemptWriter http.ResponseWriter, _target *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _profile domain.RequestProfile, _selectionMeta balancer.SelectionMeta) (proxy.ChatMetrics, error) {
		return _h.Client.ForwardChatCompletion(_ctx, _attemptWriter, _r, _target, _model, &_chatReq, _body, _profile, _selectionMeta)
	})
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleResponsesProxy(_w http.ResponseWriter, _r *http.Request, _body []byte) {
	_r = withReconnectIdentity(_r, _body)
	if key, _ := _r.Context().Value(reconnectIdentityKey{}).(string); key != "" {
		if _, active := _h.activeResponseRequests.LoadOrStore(key, struct{}{}); active {
			_w.Header().Set("Retry-After", "3")
			_h.writeJSON(_w, http.StatusTooManyRequests, domain.ErrorResponse("request_in_progress", "相同請求仍在處理，未重複送往上游"))
			return
		}
		defer _h.activeResponseRequests.Delete(key)
	}
	if _h.Balancer == nil || _h.Client == nil {
		_h.writeJSON(_w, http.StatusServiceUnavailable, domain.ErrorResponse("service_unavailable", "proxy service is not initialized"))
		return
	}

	_body, _err := applyAPIKeyRoutingPolicyToJSONRequest(_r, _body, true)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", _err.Error()))
		return
	}

	_chatReq, _err := responsesSelectionRequest(_body)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", _err.Error()))
		return
	}
	// 必須在 continuity 被移除前分析，否則會遺失 previous_response_id 與工具回傳訊號。
	_requestSignals := telemetry.AnalyzeRequestJSON(_body)
	_explicitProvider := strings.TrimSpace(_chatReq.ProviderID) != "" || strings.TrimSpace(_chatReq.Provider) != ""

	_gateWriter := newDeferredResponseWriter(_w, _chatReq.Stream)
	var _gateHeartbeat func() error
	if _chatReq.Stream {
		_gateHeartbeat = func() error { return _gateWriter.WriteStreamHeartbeat(proxy.ResponsesStreamHeartbeat()) }
	}
	_unlockTurn, _gateErr := _h.acquireTurnGate(responseTurnRoute(_body, _r), _r, _gateHeartbeat)
	if _gateErr != nil {
		return
	}
	defer _unlockTurn()
	if _gateWriter.Committed() {
		_r = _r.WithContext(context.WithValue(_r.Context(), turnGateHeadersKey{}, true))
	}
	// 先完成回合綁定檢查，再允許來源選擇。
	_turnRoute, _turnErr := _h.applyTurnBinding(&_chatReq, _body, _r)
	_recoveryRoute := responseTurnRoute(_body, _r)
	var _bindingErr *turnBindingError
	_recoverMissing := errors.As(_turnErr, &_bindingErr) && _bindingErr.missing
	_recovered := false
	if _turnErr == nil && _recoveryRoute != "" {
		_, _recovered, _turnErr = _h.lookupDurableTurnRoute("recovered:"+_recoveryRoute, proxy.ResponseRouteOwner(_r))
	}
	if (_recoverMissing || _recovered) && _recoveryRoute != "" {
		if _restored, _restoreErr := prepareRecoveryBody(_body, !_recovered); _restoreErr == nil {
			_body, _turnRoute, _turnErr = _restored, _recoveryRoute, nil
			_r = _r.WithContext(context.WithValue(_r.Context(), turnRecoveryContextKey{}, true))
			if _recoverMissing {
				log.Printf("turn binding recovery: full history preserved; account-specific reasoning discarded")
			}
		} else {
			_turnErr = _restoreErr
		}
	}
	if _turnErr != nil {
		if _gateWriter.Committed() && _h.writeGracefulStreamTerminal(_w, _gateWriter, _chatReq.Stream, proxy.ResponsesFailureTerminal, _turnErr) {
			return
		}
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("turn_binding_unavailable", _turnErr.Error()))
		return
	}

	_started := time.Now()
	_rebindFrom := ""
	_releaseContinuity := func(provider string) bool {
		if _explicitProvider {
			return false
		}
		restored, err := recoverFullHistoryBody(_body)
		if err != nil {
			log.Printf("capacity rebind unavailable: provider=%s reason=%v", provider, err)
			return false
		}
		_body, _rebindFrom = restored, provider
		_r = _r.WithContext(context.WithValue(_r.Context(), turnRecoveryContextKey{}, true))
		return true
	}
	_h.executeProviderRequest(_w, _r, _chatReq, _started, _requestSignals, proxy.ResponsesFailureTerminal, proxy.ResponsesRefusalTerminal, proxy.ResponsesStreamHeartbeat(), _releaseContinuity, func(_ctx context.Context, _attemptWriter http.ResponseWriter, _target *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _profile domain.RequestProfile, _selectionMeta balancer.SelectionMeta) (proxy.ChatMetrics, error) {
		var err error
		if _rebindFrom != "" {
			err = _h.rebindTurnBeforeDispatch(_turnRoute, _r, _rebindFrom, _target.Config.ID, _model.Name)
		} else {
			err = _h.bindTurnBeforeDispatch(_turnRoute, _r, _target.Config.ID, _model.Name)
		}
		if err != nil {
			return proxy.ChatMetrics{}, turnError(err.Error())
		}
		if _rebindFrom != "" {
			_h.Client.RecordPromptCacheRoute(proxy.PromptCacheRouteID(proxy.PromptCacheKeyFromBody(_body)), _target.Config.ID, _model.Name, proxy.ResponseRouteOwner(_r))
		}
		_rebindFrom = ""
		_metrics, _forwardErr := _h.Client.ForwardResponses(_ctx, _attemptWriter, _r, _target, _model, _body, _profile, _selectionMeta)
		_routes := make(map[string]proxy.ResponseRouteTarget)
		_h.Client.ResponseRoutes.Range(func(key, value interface{}) bool {
			id, ok := key.(string)
			target, valid := value.(proxy.ResponseRouteTarget)
			if ok && valid && target.Owner == proxy.ResponseRouteOwner(_r) && target.ProviderID == _target.Config.ID && !target.CreatedAt.Before(_started) && !strings.HasPrefix(id, "prompt-cache:") {
				_routes[id] = target
			}
			return true
		})
		if err := _h.saveTurnRoutes(_routes); err != nil {
			log.Printf("response route persistence failed: %v", err)
		}
		return _metrics, _forwardErr
	})
}

// -------------------------------------------------------------------------------------
func applyAPIKeyRoutingPolicyToJSONRequest(_r *http.Request, _body []byte, _responses bool) ([]byte, error) {
	_policy, _ok := apiKeyRoutingPolicyFromRequest(_r)
	// 全部 AUTO 時不需要改寫；直接原封傳回，避免無謂的 JSON 重新序列化
	// （重新序列化會讓超過 float64 精度的整數失真，也會把空 body 變成 "{}"）。
	if !_ok || _policy.forcesNothing() {
		return _body, nil
	}

	_payload := map[string]interface{}{}
	if len(strings.TrimSpace(string(_body))) > 0 {
		// UseNumber 保留原始數值字面值，避免 seed 這類大整數被轉成 float64 後失真。
		_decoder := json.NewDecoder(bytes.NewReader(_body))
		_decoder.UseNumber()
		if _err := _decoder.Decode(&_payload); _err != nil {
			return nil, fmt.Errorf("request body is not valid JSON")
		}
	}
	applyAPIKeyRoutingPolicyToPayload(_payload, _policy, _responses)
	_rewritten, _err := json.Marshal(_payload)
	if _err != nil {
		return nil, fmt.Errorf("request routing policy cannot be encoded: %w", _err)
	}
	return _rewritten, nil
}

// -------------------------------------------------------------------------------------
// AUTO 代表「不強制」：保留呼叫端原本送來的值，不覆寫也不刪除。
// 只有被明確指定的欄位才會被改寫。
func applyAPIKeyRoutingPolicyToPayload(_payload map[string]interface{}, _policy apiKeyRoutingPolicy, _responses bool) {
	if !strings.EqualFold(_policy.ProviderID, "AUTO") {
		delete(_payload, "provider")
		_payload["provider_id"] = _policy.ProviderID
	}
	if !strings.EqualFold(_policy.Model, "AUTO") {
		_payload["model"] = _policy.Model
	}

	_effort := normalizeAPIKeyRoutingValue(_policy.ReasoningEffort)
	if strings.EqualFold(_effort, "AUTO") {
		return
	}

	_reasoning, _hasReasoning := _payload["reasoning"].(map[string]interface{})
	if _responses {
		if !_hasReasoning {
			_reasoning = map[string]interface{}{}
			_payload["reasoning"] = _reasoning
		}
		_reasoning["effort"] = _effort
		delete(_payload, "reasoning_effort")
		return
	}
	_payload["reasoning_effort"] = _effort
	// chat wire 也可能夾帶 reasoning 物件；一併覆寫才不會留下互相衝突的設定。
	if _hasReasoning {
		if _, _exists := _reasoning["effort"]; _exists {
			_reasoning["effort"] = _effort
		}
	}
}

// -------------------------------------------------------------------------------------
// applyLowReasoningDemotion 套用低推理降級的模型等級上限。
// 必須在 Select 之前呼叫；重試時沿用同一個上限，不重新評估。
func (_h *HTTPAPI) applyLowReasoningDemotion(_r *http.Request, _request *domain.ChatCompletionRequest) {
	if _h == nil || _r == nil || _request == nil {
		return
	}
	_view, _ok := _r.Context().Value(requestAPIKeyContextKey{}).(auth.APIKeyView)
	if !_ok || strings.TrimSpace(_view.ID) == "" {
		return
	}
	if _tier := _defaultDemotionTracker.MaxQualityTierForKey(_view.ID, _h.currentAdvancedSettings(), _h.todayQuotaUsage); _tier > 0 {
		_request.MaxQualityTier = _tier
	}
}

// -------------------------------------------------------------------------------------
// todayQuotaUsage 回傳今日已消耗的配額百分比（跨啟用中的 provider 平均）。
// 第二個回傳值為 false 代表今天還沒有可用的觀測。
func (_h *HTTPAPI) todayQuotaUsage() (float64, bool) {
	if _h == nil || _h.Balancer == nil {
		return 0, false
	}
	_ids := make([]string, 0, len(_h.Balancer.Providers))
	for _, _provider := range _h.Balancer.Providers {
		if _provider == nil || _provider.Config == nil || !_provider.Config.Enabled {
			continue
		}
		_ids = append(_ids, _provider.Config.ID)
	}
	if len(_ids) == 0 {
		return 0, false
	}
	return _h.providerUsageRecorder().TodayUsagePercent(_ids, time.Now())
}

// -------------------------------------------------------------------------------------
func applyAPIKeyRoutingPolicyToSelectionRequest(_r *http.Request, _request *domain.ChatCompletionRequest) {
	_policy, _ok := apiKeyRoutingPolicyFromRequest(_r)
	if !_ok || _request == nil {
		return
	}
	// 與 applyAPIKeyRoutingPolicyToPayload 同語意：AUTO 不強制，保留呼叫端的選擇。
	if !strings.EqualFold(_policy.ProviderID, "AUTO") {
		_request.Provider = ""
		_request.ProviderID = _policy.ProviderID
	}
	if !strings.EqualFold(_policy.Model, "AUTO") {
		_request.Model = _policy.Model
	}
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleResponsesRawProxy(_w http.ResponseWriter, _r *http.Request, _body []byte, _route proxy.ResponsesProxyRoute) {
	if _h.Balancer == nil || _h.Client == nil {
		_h.writeJSON(_w, http.StatusServiceUnavailable, domain.ErrorResponse("service_unavailable", "proxy service is not initialized"))
		return
	}

	var _err error
	if responsesRouteResponseID(_route.Path) == "" && strings.EqualFold(_route.Method, http.MethodPost) {
		_body, _err = applyAPIKeyRoutingPolicyToJSONRequest(_r, _body, true)
		if _err != nil {
			_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", _err.Error()))
			return
		}
	}
	_requestSignals := telemetry.AnalyzeRequestJSON(_body)
	_selectionReq, _err := responsesRouteSelectionRequest(_route, _body)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", _err.Error()))
		return
	}
	_routeFound := _h.applyResponseRouteTarget(&_selectionReq, _route, _r)
	if responsesRouteResponseID(_route.Path) != "" && !_routeFound {
		_h.writeJSON(_w, http.StatusNotFound, domain.ErrorResponse("not_found", "response route not found or expired"))
		return
	}
	if _h.writeLocalResponsesSnapshotIfAvailable(_w, _r, _route) {
		return
	}

	_h.applyLowReasoningDemotion(_r, &_selectionReq)
	_h.refreshConversationBindings()
	_started := time.Now()
	_target, _model, _profile, _selectionMeta, _err := _h.Balancer.Select(&_selectionReq)
	if _err != nil {
		_ = history.RecordChat(history.RecordFromSelection(_started, time.Now(), _selectionReq, nil, nil, _profile, _selectionMeta, false, _err))
		_h.writeSelectionUnavailable(_w, _r, _err)
		return
	}

	noteKeyRequestComplexity(_r, _profile, _requestSignals)
	_target.StartRequest()
	defer _target.FinishRequest()

	_timeout := time.Duration(_target.Config.TimeoutSeconds) * time.Second
	if _timeout <= 0 {
		_timeout = time.Duration(domain.DefaultProviderTimeoutSeconds) * time.Second
	}

	_ctx, _cancel := requestForwardContext(_r.Context(), _timeout, _selectionReq.Stream)
	defer _cancel()

	_metrics, _forwardErr := _h.Client.ForwardResponsesRoute(_ctx, _w, _r, _target, _model, _route, _body, _profile, _selectionMeta)
	if _forwardErr != nil {
		recordProviderForwardFailure(_target, _forwardErr, _r, time.Since(_started), _h.providerCapacityCooldown(), _h.providerServerErrorCooldown(), _model.Name)
		if proxy.ClassifyFailure(_forwardErr).Quota {
			_h.persistQuotaCooldown(_target)
		}
		_ = history.RecordChat(history.RecordFromSelection(_started, time.Now(), _selectionReq, _target, _model, _profile, _selectionMeta, false, _forwardErr))
		if proxy.ResponseAlreadyForwarded(_forwardErr) {
			return
		}
		_h.writeJSON(_w, http.StatusBadGateway, domain.ErrorResponse("provider_error", _forwardErr.Error()))
		return
	}

	_duration := time.Since(_started)
	_tokenSpeed := _metrics.TokenGenerationSpeed(_duration)
	_clientDeliveryTPS := _metrics.ClientDeliveryTPS
	_reactionMS := observedReactionMS(_metrics)
	_target.MarkSuccessWithMetrics(_duration, _metrics.CompletionTokens, _reactionMS, _tokenSpeed, _clientDeliveryTPS)
	_target.RecordProviderReportedTPS(_metrics.ProviderReportedGenerationTPS())
	noteKeyRequestConsumption(_r, _model, _profile, _metrics)
	_ = history.RecordChat(history.RecordFromSelectionWithUsage(_started, time.Now(), _selectionReq, _target, _model, _profile, _selectionMeta, true, nil, _metrics.CompletionTokens, _tokenSpeed, _clientDeliveryTPS, _metrics.EstimatedTokens, _reactionMS))
	if responseRouteIsDelete(_route) && _h.Client != nil {
		_h.Client.DeleteResponseRoute(responsesRouteResponseID(_route.Path))
	}
}

// -------------------------------------------------------------------------------------
func observedReactionMS(_metrics proxy.ChatMetrics) float64 {
	if _metrics.FirstResponseMS <= 0 {
		return 0
	}
	return _metrics.FirstResponseMS
}

// -------------------------------------------------------------------------------------
func requestForwardContext(_parent context.Context, _timeout time.Duration, _stream bool) (context.Context, context.CancelFunc) {
	if _parent == nil {
		_parent = context.Background()
	}
	if _stream {
		return context.WithCancel(_parent)
	}
	if _timeout <= 0 {
		_timeout = time.Duration(domain.DefaultProviderTimeoutSeconds) * time.Second
	}
	return context.WithTimeout(_parent, _timeout)
}

// -------------------------------------------------------------------------------------
// 保活心跳間隔。上游過載時單次嘗試約 1.5～4 秒，換三個帳號就是十幾秒的靜默；
// 客戶端等不到第一個 byte 就會斷線重連（Codex 顯示「正在重新連線」），
// 而重連會把整個 context 重送一次，比等待昂貴得多。
const providerRetryKeepaliveInterval = 3 * time.Second

// -------------------------------------------------------------------------------------
// startRetryKeepalive 在還沒有任何回應內容送出的期間，定期送出保活心跳。
// 心跳不帶回應內容，所以不會剝奪換帳號重試的能力。
func startRetryKeepalive(_ctx context.Context, _deferred *deferredResponseWriter, _stream bool, _heartbeat []byte, _cancel context.CancelFunc) func() {
	if !_stream || _deferred == nil || len(_heartbeat) == 0 {
		return func() {}
	}
	_stop := make(chan struct{})
	_done := make(chan struct{})
	go func() {
		defer close(_done)
		_ticker := time.NewTicker(providerRetryKeepaliveInterval)
		defer _ticker.Stop()
		for {
			select {
			case <-_stop:
				return
			case <-_ctx.Done():
				return
			case <-_ticker.C:
				// 內容一旦開始送出就交給上游串流自己的心跳，這裡必須讓開，
				// 否則會把 ping 插進正在傳輸的回應中間。
				if _deferred.ContentWritten() {
					return
				}
				if _err := _deferred.WriteStreamHeartbeat(_heartbeat); _err != nil {
					if _cancel != nil {
						_cancel()
					}
					return
				}
			}
		}
	}()
	return func() {
		close(_stop)
		<-_done
	}
}

// -------------------------------------------------------------------------------------
type providerForwardAttempt func(context.Context, http.ResponseWriter, *balancer.ProviderRuntime, *domain.LLMModelConfig, domain.RequestProfile, balancer.SelectionMeta) (proxy.ChatMetrics, error)

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) executeProviderRequest(_w http.ResponseWriter, _r *http.Request, _request domain.ChatCompletionRequest, _started time.Time, _signals telemetry.RequestSignals, _refusalTerminal func(string) []byte, _throttleTerminal func(string) []byte, _heartbeat []byte, _releaseContinuity func(string) bool, _forward providerForwardAttempt) {
	_trace := newRequestDiagnosticID()
	_w.Header().Set("X-Proxy-Request-ID", _trace)
	// 綁定請求維持較小的重試預算；容量備援也不能突破此上限。
	_pinnedProvider := strings.TrimSpace(_request.ProviderID) != "" || strings.TrimSpace(_request.Provider) != ""
	_maxRetries := _h.providerRetryCount()
	if _pinnedProvider && _maxRetries > pinnedProviderMaxRetries {
		_maxRetries = pinnedProviderMaxRetries
	}
	_reconnectKey, _ := _r.Context().Value(reconnectIdentityKey{}).(string)
	_shared, _rejection := _h.reconnectBudgets.acquire(_reconnectKey, _maxRetries+1)
	if _rejection != nil {
		if _rejection.retryAfter > 0 {
			_w.Header().Set("Retry-After", strconv.Itoa(_rejection.retryAfter))
		}
		log.Printf("provider replay blocked: trace=%s reason=%s", _trace, _rejection.code)
		_rejectionWriter := newDeferredResponseWriter(_w, _request.Stream)
		_gateHeaders, _ := _r.Context().Value(turnGateHeadersKey{}).(bool)
		if _gateHeaders {
			_rejectionWriter.AdoptCommitted()
		}
		// 額度用盡是代理的節流決定，不是上游故障。送 response.failed 會讓客戶端
		// 判定回合中斷（Codex 顯示 stream disconnected before completion）並立刻重連，
		// 反而把節流變成更吵的重試。改用「完成」型終止事件把原因當成助理訊息交付，
		// agent 讀得到「請在 N 秒後重試」而不是崩潰。
		_terminal := _refusalTerminal
		if (_rejection.code == "request_retry_exhausted" || _rejection.code == "request_replay_unsafe") && _throttleTerminal != nil {
			_terminal = _throttleTerminal
		}
		if (_rejection.code == "request_retry_exhausted" || _rejection.code == "request_replay_unsafe" || _gateHeaders) && _h.writeGracefulStreamTerminal(_w, _rejectionWriter, _request.Stream, _terminal, errors.New(_rejection.message)) {
			return
		}
		_h.writeJSON(_w, _rejection.status, domain.ErrorResponse(_rejection.code, _rejection.message))
		return
	}
	_success := false
	defer func() { _h.reconnectBudgets.release(_reconnectKey, _shared, _success) }()
	if _shared != nil {
		_maxRetries = _shared.limit - _shared.attempts - 1
		if _shared.rebindFrom != "" {
			if _h.conversationPinIsReleasable(_r) && _releaseContinuity != nil && _releaseContinuity(_shared.rebindFrom) {
				_request.ProviderID, _request.Provider = "", ""
				_pinnedProvider = false
			} else {
				_request.ProviderID, _request.Provider = _shared.rebindFrom, _shared.rebindFrom
				_pinnedProvider = true
			}
		}
		if _shared.provider != "" {
			if (_request.ProviderID != "" && _request.ProviderID != _shared.provider) || (_request.Provider != "" && _request.Provider != _shared.provider) {
				_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("request_route_conflict", "重連請求與原 Provider 不一致，未送往上游"))
				return
			}
			_request.ProviderID, _request.Provider, _request.Model = _shared.provider, _shared.provider, _shared.model
		}
	}
	_h.applyLowReasoningDemotion(_r, &_request)
	_h.refreshConversationBindings()

	_complexityRecorded := false
	// 保活心跳送出後 header 就已經出去了，後續嘗試的 writer 必須承接這個狀態。
	_headersSent, _ := _r.Context().Value(turnGateHeadersKey{}).(bool)
	var _lastErr error
	var _lastDeferred *deferredResponseWriter
	_waitedForCooldown := time.Duration(0)
	_waitedForAdmission := time.Duration(0)
	_hasDispatched := false
	if _shared != nil {
		_waitedForCooldown = _shared.waited
		_waitedForAdmission = _shared.admissionWaited
		_hasDispatched = _shared.attempts > 0
	}
	_retrySettings := _h.currentAdvancedSettings()
	_budget := providerRetryBudget{maxRounds: _retrySettings.ProviderRetryRounds, maxSources: _retrySettings.ProviderRetrySourcesPerRound}
	if _shared != nil {
		_budget.used = append([]string(nil), _shared.usedProviders...)
	}

	for _attempt := 0; _attempt <= _maxRetries; _attempt++ {
		_target, _model, _profile, _selectionMeta, _err := _h.selectRetryProvider(&_request, &_budget)
		_lastPolicy := proxy.ClassifyFailure(_lastErr)
		if _err != nil && (_lastErr == nil || _lastPolicy.Capacity || _lastPolicy.RetryableServer) {
			if _err != nil && _r.Context().Err() == nil {
				_waitWriter := newDeferredResponseWriter(_w, _request.Stream)
				_waitHeartbeat := _heartbeat
				if !_request.Stream {
					_waitHeartbeat = nil
				}
				if _headersSent {
					_waitWriter.AdoptCommitted()
				}
				// 首次排隊不可吃掉失敗後的退避額度；兩者均跨重連累計，不逐次重設。
				_waitPhase := "retry"
				_spent := &_waitedForCooldown
				if !_hasDispatched {
					_waitPhase, _spent = "admission", &_waitedForAdmission
				}
				_remaining := time.Duration(_retrySettings.ProviderRetryWaitSeconds)*time.Second - *_spent
				_elapsed, _ready := waitForProviderCooldown(_r.Context(), _waitWriter, _waitHeartbeat, _err, _remaining)
				*_spent += _elapsed
				if _shared != nil {
					_shared.waited, _shared.admissionWaited = _waitedForCooldown, _waitedForAdmission
				}
				log.Printf("provider wait result: trace=%s phase=%s remaining=%s elapsed=%s ready=%t reason=%s", _trace, _waitPhase, _remaining, _elapsed, _ready, cooldownWaitReason(_r.Context(), _err, _remaining, _ready))
				_headersSent = _headersSent || _waitWriter.Committed()
				if _lastDeferred == nil {
					_lastDeferred = _waitWriter
				} else if _headersSent {
					_lastDeferred.AdoptCommitted()
				}
				if _r.Context().Err() != nil {
					return
				}
				if _ready {
					_attempt--
					continue
				}
			}
		}
		if _err != nil {
			if !_request.Stream && _lastDeferred != nil && !_lastDeferred.Committed() && _lastDeferred.HasBufferedResponse() {
				_ = _lastDeferred.Commit()
				return
			}
			// 串流請求不能回 HTTP 錯誤：客戶端會判定連線失敗並自動重連
			// （Codex 顯示「正在重新連線 N/5」），使用者看到的是網路問題而不是原因。
			// 先前只有「重試用盡」那條路做了這件事，選擇階段失敗卻直接回 503／502，
			// 於是所有 provider 都不可用時每一輪都變成一次重連。
			_terminalErr := _err
			if _lastErr != nil {
				_terminalErr = _lastErr
			}
			_terminalWriter := _lastDeferred
			if _terminalWriter == nil {
				_terminalWriter = newDeferredResponseWriter(_w, _request.Stream)
			}
			if _h.writeGracefulStreamTerminal(_w, _terminalWriter, _request.Stream, _refusalTerminal, _terminalErr) {
				if _lastErr == nil {
					_ = history.RecordChat(history.RecordFromSelection(_started, time.Now(), _request, nil, nil, _profile, _selectionMeta, false, _err))
				}
				return
			}
			if _lastErr != nil {
				_h.writeJSON(_w, http.StatusBadGateway, domain.ErrorResponse("provider_error", _lastErr.Error()))
				return
			}
			_ = history.RecordChat(history.RecordFromSelection(_started, time.Now(), _request, nil, nil, _profile, _selectionMeta, false, _err))
			_h.writeSelectionUnavailable(_w, _r, _err)
			return
		}

		if !_complexityRecorded {
			// 只在第一次成功選擇時記錄：重試是同一個請求，不該重複計數。
			noteKeyRequestComplexity(_r, _profile, _signals)
			_complexityRecorded = true
		}

		// 每次送出前固定來源，僅經下方容量與歷史檢查通過後才允許改綁。
		_request.ProviderID = _target.Config.ID
		_request.Provider = _target.Config.ID
		_request.Model = _model.Name
		_hasDispatched = true
		if _shared != nil {
			_shared.provider, _shared.model = _target.Config.ID, _model.Name
			_shared.rebindFrom = ""
			_shared.usedProviders = append(append([]string(nil), _budget.used...), _target.Config.ID)
			_shared.attempts++
			log.Printf("provider replay budget: trace=%s attempt=%d limit=%d waited=%s", _trace, _shared.attempts, _shared.limit, _shared.waited)
		}
		_pinnedProvider = true
		_attemptStarted := time.Now()
		_budget.used = append(_budget.used, _target.Config.ID)
		log.Printf("provider attempt: provider=%q model=%q attempt=%d round=%d trace=%s", _target.Config.ID, _model.Name, _attempt+1, _budget.round, _trace)
		_target.StartRequest()
		_timeout := time.Duration(_target.Config.TimeoutSeconds) * time.Second
		if _timeout <= 0 {
			_timeout = time.Duration(domain.DefaultProviderTimeoutSeconds) * time.Second
		}
		_ctx, _cancel := requestForwardContext(_r.Context(), _timeout, _request.Stream)
		_deferred := newDeferredResponseWriter(_w, _request.Stream)
		if _headersSent {
			_deferred.AdoptCommitted()
		}
		// 保活必須涵蓋整個嘗試：上游過載時要 1.5～4 秒才回錯誤，
		// 那段期間內建心跳照不到（還沒有上游串流可監看）。
		_stopKeepalive := startRetryKeepalive(_ctx, _deferred, _request.Stream, _heartbeat, _cancel)
		_metrics, _forwardErr := _forward(_ctx, _deferred, _target, _model, _profile, _selectionMeta)
		_stopKeepalive()
		_cancel()
		// 跨重連封鎖以已交付的完整工具呼叫為準；文字與推理可能重複顯示，
		// 但不因此觸發工具防重播。同一條串流的重試仍受 ContentWritten 限制。
		if _shared != nil && _deferred.ToolCallDelivered() {
			_shared.delivered = true
		}
		proxy.EnrichFailure(_forwardErr, _deferred.BufferedBody())
		if _deferred.Committed() {
			_headersSent = true
		}
		_target.FinishRequest()
		_attemptDuration := time.Since(_attemptStarted)
		log.Printf("provider attempt result: trace=%s provider=%q model=%q attempt=%d elapsed=%s failed=%t delivered=%t first_commit=%q content_items=%d upstream_request_id=%q",
			_trace, _target.Config.ID, _model.Name, _attempt+1, _attemptDuration.Round(time.Millisecond), _forwardErr != nil,
			_deferred.ContentWritten(), _deferred.FirstCommitEvent(), _metrics.ClientContentItems, diagnosticHeaderID(_deferred.Header().Get("X-Request-ID")))

		if _forwardErr == nil {
			if _commitErr := _deferred.Commit(); _commitErr != nil {
				_forwardErr = _commitErr
				if _shared != nil && _deferred.ToolCallDelivered() {
					_shared.delivered = true
				}
			} else {
				_success = true
				if _metrics.FirstResponseMS > 0 {
					_metrics.FirstResponseMS += float64(_attemptStarted.Sub(_started).Milliseconds())
				}
				_duration := time.Since(_started)
				_tokenSpeed := _metrics.TokenGenerationSpeed(_attemptDuration)
				_clientDeliveryTPS := _metrics.ClientDeliveryTPS
				_reactionMS := observedReactionMS(_metrics)
				_target.MarkSuccessWithMetrics(_duration, _metrics.CompletionTokens, _reactionMS, _tokenSpeed, _clientDeliveryTPS)
				_target.RecordProviderReportedTPS(_metrics.ProviderReportedGenerationTPS())
				noteKeyRequestConsumption(_r, _model, _profile, _metrics)
				_ = history.RecordChat(history.RecordFromSelectionWithUsage(_started, time.Now(), _request, _target, _model, _profile, _selectionMeta, true, nil, _metrics.CompletionTokens, _tokenSpeed, _clientDeliveryTPS, _metrics.EstimatedTokens, _reactionMS))
				return
			}
		}

		recordProviderForwardFailure(_target, _forwardErr, _r, _attemptDuration, _h.providerCapacityCooldown(), _h.providerServerErrorCooldown(), _model.Name)
		if proxy.ClassifyFailure(_forwardErr).Quota {
			_h.persistQuotaCooldown(_target)
		}
		_ = history.RecordChat(history.RecordFromSelection(_attemptStarted, time.Now(), _request, _target, _model, _profile, _selectionMeta, false, _forwardErr))
		_lastErr = _forwardErr
		_lastDeferred = _deferred
		_failurePolicy := proxy.ClassifyFailure(_forwardErr)
		// 未向下游送出內容的容量拒絕，退回代理跨重連額度；每輪上限與來源冷卻不變。
		// 這不是上游用量退款，也不能證明上游沒有執行工作。
		if _shared != nil && _failurePolicy.Capacity && !_deferred.ContentWritten() && _shared.attempts > 0 {
			_shared.attempts--
			log.Printf("provider replay budget refunded: trace=%s content_written=false attempts=%d limit=%d", _trace, _shared.attempts, _shared.limit)
		}
		if _failurePolicy.Capacity && !_deferred.ContentWritten() && providerFailureCanRetryBeforeFirstToken(_forwardErr, _deferred) && _h.conversationPinIsReleasable(_r) && _releaseContinuity != nil && _releaseContinuity(_target.Config.ID) {
			_request.ProviderID, _request.Provider = "", ""
			_pinnedProvider = false
			if _shared != nil {
				_shared.provider, _shared.model = "", ""
				_shared.rebindFrom = _target.Config.ID
			}
			log.Printf("provider capacity rebind prepared: trace=%s from=%s attempts_preserved=true", _trace, _target.Config.ID)
		}
		if _deferred.ContentWritten() || _attempt >= _maxRetries || !providerFailureCanRetryBeforeFirstToken(_forwardErr, _deferred) {
			_reason := "not_retryable"
			if _deferred.ContentWritten() {
				_reason = "content_delivered"
			} else if _attempt >= _maxRetries {
				_reason = "attempt_limit"
			}
			log.Printf("provider retry stopped: trace=%s reason=%s admission_waited=%s retry_waited=%s", _trace, _reason, _waitedForAdmission, _waitedForCooldown)
			if _deferred.ContentWritten() {
				return
			}
			if !_request.Stream && !_deferred.Committed() && _deferred.HasBufferedResponse() {
				_ = _deferred.Commit()
				return
			}
			// 重試用盡且尚未送出有效內容：使用對應協定的終止事件帶出錯誤。
			if _h.writeGracefulStreamTerminal(_w, _deferred, _request.Stream, _refusalTerminal, _forwardErr) {
				return
			}
			_h.writeJSON(_w, http.StatusBadGateway, domain.ErrorResponse("provider_error", _forwardErr.Error()))
			return
		}

		if _pinnedProvider {
			// 其餘暫時性故障：保留原 provider，短暫退避讓它有機會恢復。
			if !_failurePolicy.Capacity && !_failurePolicy.RetryableServer && !waitBeforeRetry(_r.Context(), _attempt) {
				return
			}
			continue
		}
	}
}

// -------------------------------------------------------------------------------------
// writeGracefulStreamTerminal 在重試用盡時，以對應協定的串流終止事件帶出原因。
// 只有「串流請求」且「確定尚未送出任何內容」時適用；回傳 false 代表無法收尾，
// 呼叫端應改回一般錯誤回應。
func (_h *HTTPAPI) writeGracefulStreamTerminal(_w http.ResponseWriter, _deferred *deferredResponseWriter, _stream bool, _refusalTerminal func(string) []byte, _err error) bool {
	if !_stream || _refusalTerminal == nil || _err == nil || _deferred == nil {
		return false
	}
	if !_deferred.ResetForGracefulTerminal() {
		return false
	}
	_deferred.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	_deferred.Header().Set("Cache-Control", "no-cache")
	_deferred.Header().Set("X-Accel-Buffering", "no")
	_deferred.WriteHeader(http.StatusOK)
	if _, _writeErr := _deferred.Write(_refusalTerminal(_err.Error())); _writeErr != nil {
		return false
	}
	return _deferred.Commit() == nil
}

// -------------------------------------------------------------------------------------
// noteKeyRequestComplexity 記錄這支金鑰本次請求的複雜度，供密集度統計使用。
// 必須在 Select 成功之後呼叫（RequestProfile 這時才算好），且每個請求只記一次 ——
// 重試不應該灌大分級次數。
func noteKeyRequestComplexity(_r *http.Request, _profile domain.RequestProfile, _requestSignals ...telemetry.RequestSignals) {
	if _r == nil {
		return
	}
	_view, _ok := _r.Context().Value(requestAPIKeyContextKey{}).(auth.APIKeyView)
	if !_ok || strings.TrimSpace(_view.ID) == "" {
		return
	}
	_sample := keyusage.RequestSample{Complexity: _profile.ComplexityScore}
	if len(_requestSignals) > 0 {
		_signals := _requestSignals[0]
		_sample.Continuation = _signals.Continuation
		_sample.Fingerprint = _signals.Fingerprint
		_sample.ToolCalls = _signals.ToolCalls
		_sample.ToolRounds = _signals.ToolRounds
		_sample.ToolOutputTokens = _signals.ToolOutputTokens
	}
	if _err := keyusage.DefaultRecorder().RecordRequest(_view.ID, time.Now(), _sample); _err != nil {
		log.Printf("key complexity record failed: key=%s error=%v", _view.ID, _err)
	}
}

// -------------------------------------------------------------------------------------
// estimateStreamedTokens 只用來判斷「這筆回應拆得出用途分類嗎」，不是比例的分母。
// 分母用的是上游回報的 completion tokens（已含推理），否則 provider 不串流
// 推理內容時，分母會少掉絕大部分而讓文字比虛高。
func estimateStreamedTokens(_metrics proxy.ChatMetrics) int {
	return _metrics.ProseTokens() + _metrics.ReasoningTokens() + _metrics.ToolTokens()
}

// -------------------------------------------------------------------------------------
// noteKeyRequestConsumption 記錄這支金鑰本次請求的實際輸出量。
// 只有成功完成才會呼叫 —— 進行中或失敗的請求沒有可信的消耗量。
func noteKeyRequestConsumption(_r *http.Request, _model *domain.LLMModelConfig, _profile domain.RequestProfile, _metrics proxy.ChatMetrics) {
	if _r == nil || _metrics.CompletionTokens <= 0 {
		return
	}
	_view, _ok := _r.Context().Value(requestAPIKeyContextKey{}).(auth.APIKeyView)
	if !_ok || strings.TrimSpace(_view.ID) == "" {
		return
	}
	_sample := keyusage.ConsumptionSample{
		Tokens:       _metrics.CompletionTokens,
		Complexity:   _profile.ComplexityScore,
		PromptTokens: _profile.EstimatedInputTokens,
	}
	// 只有串流回應拆得出用途分類。非串流時兩者都是 0，該筆就不列入文字比統計 ——
	// 留成 0 會被誤讀成「整輪都在呼叫工具」。
	_sample.ProseTokens = _metrics.ProseTokens()
	_sample.ReasoningTokens = _metrics.ReasoningTokens()
	_sample.ReasoningReported = _metrics.ReasoningReported
	_sample.StreamedTokens = estimateStreamedTokens(_metrics)
	// 實際選中的模型等級。強制路由或退回其他 provider 時模型會變，
	// 所以要記錄「真的用到的」而不是請求要的。
	if _model != nil {
		_sample.QualityTier = _model.QualityTier
	}
	if _err := keyusage.DefaultRecorder().RecordConsumption(_view.ID, time.Now(), _sample); _err != nil {
		log.Printf("key consumption record failed: key=%s error=%v", _view.ID, _err)
	}
}

// -------------------------------------------------------------------------------------
// conversationPinIsReleasable 判斷目前的 provider 綁定能不能為了避開故障而解除。
// 金鑰明確強制的 provider 是管理員政策，不能擅自更換；
// 對話黏著造成的綁定則可以解除（代價是該輪失去推理脈絡）。
func (_h *HTTPAPI) conversationPinIsReleasable(_r *http.Request) bool {
	_policy, _ok := apiKeyRoutingPolicyFromRequest(_r)
	if !_ok {
		return true
	}
	return strings.EqualFold(_policy.ProviderID, "AUTO")
}

// -------------------------------------------------------------------------------------
// waitBeforeRetry 在重試同一個 provider 前做短暫退避。回傳 false 代表請求已被取消。
func waitBeforeRetry(_ctx context.Context, _attempt int) bool {
	_delay := time.Duration(_attempt+1) * pinnedProviderRetryBackoff
	_timer := time.NewTimer(_delay)
	defer _timer.Stop()
	select {
	case <-_timer.C:
		return true
	case <-_ctx.Done():
		return false
	}
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) providerRetryCount() int {
	if _h == nil || _h.Balancer == nil {
		return 0
	}
	_count := _h.Balancer.ConfigSnapshot().RetryCount
	if _count < 0 {
		return 0
	}
	if _count > 10 {
		return 10
	}
	return _count
}

// -------------------------------------------------------------------------------------
func providerFailureCanRetryBeforeFirstToken(_err error, _deferred *deferredResponseWriter) bool {
	if errors.Is(_err, errResponseBufferLimit) {
		return false
	}
	// 判準是「內容有沒有送達客戶端」而不是「有沒有 commit」：
	// 保活心跳會 commit，但它不帶回應內容，送過心跳仍然可以換帳號重試。
	if _err == nil || _deferred == nil || _deferred.ContentWritten() {
		return false
	}
	if _policy := proxy.ClassifyFailure(_err); _policy.Action() == proxy.FailureStop || _policy.Action() == proxy.FailureStopAndCooldown {
		return false
	} else if _policy.Capacity || _policy.RetryableServer || _policy.Auth {
		return true
	}
	_statusCode := _deferred.StatusCode()
	switch _statusCode {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusRequestTimeout, http.StatusConflict,
		http.StatusTooEarly, http.StatusTooManyRequests, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	if proxy.IsRetryableCapacityError(_err) {
		return true
	}
	// 上游沒送 terminal 就關掉串流：一個字都還沒送出去，換個 provider 重試無痕。
	if proxy.IsTruncatedStreamError(_err) {
		return true
	}
	_text := strings.ToLower(strings.TrimSpace(_err.Error() + " " + _deferred.BufferedBody()))
	for _, _marker := range []string{
		"capacity", "overloaded", "rate limit", "rate_limit", "temporarily unavailable",
		"connection reset", "connection refused", "unexpected eof", "timeout", "timed out",
		"unauthorized", "authentication", "token expired", "token_revoked", "token_invalidated",
	} {
		if strings.Contains(_text, _marker) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
// applyConversationAffinity 讓帶有 previous_response_id 的後續請求回到同一個 provider。
// Responses 對話是有狀態的：previous_response_id 與 reasoning 的 encrypted_content 都綁定
// 發出它的帳號，換帳號會被上游拒絕（Codex 端看到 stream closed before response.completed）。
//
// 沒有 previous_response_id 時完全不介入，維持原本的負載平衡。
// 回傳 (是否已釘住, 是否必須移除 continuity 後重新負載平衡)。
func (_h *HTTPAPI) applyConversationAffinity(_req *domain.ChatCompletionRequest, _body []byte, _request *http.Request) (bool, bool) {
	if _h == nil || _h.Client == nil || _h.Balancer == nil || _req == nil {
		return false, false
	}

	// Codex 多輪工具呼叫時往往「不送」previous_response_id，只帶 prompt_cache_key
	// 與綁定帳號的加密推理內容，所以 cache key 必須列為對等的黏著鍵。
	_previousID := previousResponseIDFromBody(_body)
	_routeID := _previousID
	if _routeID == "" {
		_routeID = proxy.PromptCacheRouteID(proxy.PromptCacheKeyFromBody(_body))
	}
	if _routeID == "" {
		return false, false
	}

	// 先套用持久化 policy 再查 route；否則自訂 TTL 大於預設值時，lookup 會先用
	// 預設 30 分鐘把其實仍有效的 route 清掉。
	_settings := _h.currentAdvancedSettings()
	_target, _ok := _h.Client.LookupResponseRouteForOwner(_routeID, proxy.ResponseRouteOwner(_request))
	if !_ok || strings.TrimSpace(_target.ProviderID) == "" {
		// 全新的對話沒有任何延續性內容可重設，直接照常負載平衡即可。
		if !bodyCarriesConversationContinuity(_body) {
			return false, false
		}
		// 程式重啟、TTL 到期或 cache 淘汰時仍要重新負載平衡；但既有的延續性內容
		// 綁定原 provider，不能原樣送給隨機 provider，否則只會得到上游錯誤。
		log.Printf("conversation affinity route unavailable, resetting continuity: route=%s", _routeID)
		return false, true
	}

	_explicitProvider := strings.TrimSpace(_req.ProviderID)
	if _explicitProvider == "" {
		_explicitProvider = strings.TrimSpace(_req.Provider)
	}
	if _explicitProvider != "" && !providerReferenceMatchesTarget(_h.Balancer, _target.ProviderID, _explicitProvider) {
		log.Printf(
			"conversation affinity conflicts with forced provider, resetting continuity: previous_provider=%s forced_provider=%s previous_response=%s",
			_target.ProviderID, _explicitProvider, _previousID,
		)
		return false, true
	}

	// 釘住一個當下不可選的 provider（滿載、熔斷、配額暫時不可用…）會讓選擇階段
	// 找不到候選而回 503（temporarily overloaded）。降級重新負載平衡優於讓整輪失敗。
	if !_h.Balancer.ProviderAvailableForSelection(_target.ProviderID) {
		log.Printf(
			"conversation affinity provider unavailable, resetting continuity: provider=%s route=%s",
			_target.ProviderID, _routeID,
		)
		return false, true
	}

	_tolerancePoints := _settings.ConversationAffinityQuotaTolerancePoints
	if !bodyEndsWithToolResult(_body) && _h.Balancer.QuotaBelowPeerAverage(_target.ProviderID, _tolerancePoints) {
		log.Printf(
			"conversation affinity dropped for quota balance: provider=%s previous_response=%s tolerance=%.0f%%",
			_target.ProviderID, _previousID, _tolerancePoints,
		)
		return false, true
	}

	_req.ProviderID = _target.ProviderID
	_req.Provider = _target.ProviderID
	if (strings.TrimSpace(_req.Model) == "" || strings.EqualFold(strings.TrimSpace(_req.Model), "AUTO")) && strings.TrimSpace(_target.Model) != "" {
		_req.Model = _target.Model
	}
	return true, false
}

// -------------------------------------------------------------------------------------
// 工具結果到下一輪生成之間保留原帳號，不因配額均衡而丟失推理上下文。
func bodyEndsWithToolResult(body []byte) bool {
	var payload struct {
		Input []struct {
			Type string `json:"type"`
			Role string `json:"role"`
		} `json:"input"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return false
	}
	for i := len(payload.Input) - 1; i >= 0; i-- {
		item := payload.Input[i]
		kind := strings.ToLower(strings.TrimSpace(item.Type))
		if strings.HasSuffix(kind, "_call_output") || strings.EqualFold(item.Role, "tool") {
			return true
		}
		if strings.EqualFold(item.Role, "user") {
			return false
		}
	}
	return false
}

func providerReferenceMatchesTarget(_balancer *balancer.LoadBalancer, _targetProviderID string, _reference string) bool {
	_targetProviderID = strings.TrimSpace(_targetProviderID)
	_reference = strings.TrimSpace(_reference)
	if _balancer == nil || _targetProviderID == "" || _reference == "" {
		return false
	}
	if strings.EqualFold(_targetProviderID, _reference) {
		return true
	}
	for _, _provider := range _balancer.ProvidersSnapshot() {
		if _provider == nil || _provider.Config == nil || !strings.EqualFold(strings.TrimSpace(_provider.Config.ID), _targetProviderID) {
			continue
		}
		for _, _candidate := range []string{
			_provider.Config.ID,
			_provider.Config.Name,
			_provider.Config.Kind,
			_provider.Config.Type,
		} {
			if strings.EqualFold(strings.TrimSpace(_candidate), _reference) {
				return true
			}
		}
		return false
	}
	return false
}

// -------------------------------------------------------------------------------------
func previousResponseIDFromBody(_body []byte) string {
	if len(bytes.TrimSpace(_body)) == 0 {
		return ""
	}
	var _payload map[string]interface{}
	if _err := json.Unmarshal(_body, &_payload); _err != nil {
		return ""
	}
	_value, _ := _payload["previous_response_id"].(string)
	return strings.TrimSpace(_value)
}

// -------------------------------------------------------------------------------------
// bodyCarriesConversationContinuity 判斷請求是否帶有「只有原帳號能解讀」的延續性內容。
// 全新對話沒有這些內容，不需要重設，也不該產生日誌噪音。
func bodyCarriesConversationContinuity(_body []byte) bool {
	if len(bytes.TrimSpace(_body)) == 0 {
		return false
	}
	var _payload map[string]interface{}
	if _err := json.Unmarshal(_body, &_payload); _err != nil {
		return false
	}
	if _value, _ := _payload["previous_response_id"].(string); strings.TrimSpace(_value) != "" {
		return true
	}
	_items, _ok := _payload["input"].([]interface{})
	if !_ok {
		return false
	}
	for _, _item := range _items {
		if itemCarriesEncryptedReasoning(_item) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
// stripConversationContinuity 移除只有原帳號才能解讀的延續性內容。
// 放棄黏著後若原樣轉送，新的 provider 一定會失敗，這樣的「負載平衡」沒有意義；
// 拿掉之後該輪會少了先前的推理脈絡，但至少能正常完成。
func stripConversationContinuity(_body []byte) ([]byte, error) {
	if len(bytes.TrimSpace(_body)) == 0 {
		return _body, nil
	}
	_payload := map[string]interface{}{}
	_decoder := json.NewDecoder(bytes.NewReader(_body))
	_decoder.UseNumber()
	if _err := _decoder.Decode(&_payload); _err != nil {
		return _body, _err
	}

	delete(_payload, "previous_response_id")
	if _items, _ok := _payload["input"].([]interface{}); _ok {
		_kept := make([]interface{}, 0, len(_items))
		for _, _item := range _items {
			if itemCarriesEncryptedReasoning(_item) {
				continue
			}
			_kept = append(_kept, _item)
		}
		_payload["input"] = _kept
	}
	return json.Marshal(_payload)
}

// -------------------------------------------------------------------------------------
func itemCarriesEncryptedReasoning(_item interface{}) bool {
	_map, _ok := _item.(map[string]interface{})
	if !_ok {
		return false
	}
	if _, _exists := _map["encrypted_content"]; _exists {
		return true
	}
	_type, _ := _map["type"].(string)
	return strings.EqualFold(strings.TrimSpace(_type), "reasoning")
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) applyResponseRouteTarget(_req *domain.ChatCompletionRequest, _route proxy.ResponsesProxyRoute, _request *http.Request) bool {
	if _h == nil || _h.Client == nil || _req == nil {
		return false
	}
	_responseID := responsesRouteResponseID(_route.Path)
	if _responseID == "" {
		return false
	}
	_target, _ok := _h.Client.LookupResponseRouteForOwner(_responseID, proxy.ResponseRouteOwner(_request))
	if !_ok || strings.TrimSpace(_target.ProviderID) == "" {
		return false
	}
	_req.ProviderID = _target.ProviderID
	_req.Provider = _target.ProviderID
	if strings.TrimSpace(_target.Model) != "" {
		_req.Model = _target.Model
	}
	return true
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) writeLocalResponsesSnapshotIfAvailable(_w http.ResponseWriter, _r *http.Request, _route proxy.ResponsesProxyRoute) bool {
	if _h == nil || _h.Client == nil || _r == nil || _r.Method != http.MethodGet {
		return false
	}
	_responseID := responsesRouteResponseID(_route.Path)
	if _responseID == "" {
		return false
	}
	_snapshot, _ok := _h.Client.LookupResponseRouteForOwner(_responseID, proxy.ResponseRouteOwner(_r))
	if !_ok {
		return false
	}
	_parts := strings.Split(strings.Trim(normalizeProxyRoute(_route.Path), "/"), "/")
	if len(_parts) == 3 && _snapshot.Response != nil {
		_h.writeJSON(_w, http.StatusOK, _snapshot.Response)
		return true
	}
	if len(_parts) == 4 && _parts[3] == "input_items" && _snapshot.Input != nil {
		_h.writeJSON(_w, http.StatusOK, responsesItemList(_snapshot.Input))
		return true
	}
	if len(_parts) == 4 && _parts[3] == "output_items" && _snapshot.Response != nil {
		_h.writeJSON(_w, http.StatusOK, responsesItemList(_snapshot.Response["output"]))
		return true
	}
	return false
}

// -------------------------------------------------------------------------------------
func responsesItemList(_value interface{}) map[string]interface{} {
	_items := responsesItemsSlice(_value)
	return map[string]interface{}{
		"object":   "list",
		"data":     _items,
		"first_id": responseItemID(_items, 0),
		"last_id":  responseItemID(_items, len(_items)-1),
		"has_more": false,
	}
}

// -------------------------------------------------------------------------------------
func responsesItemsSlice(_value interface{}) []interface{} {
	switch _typed := _value.(type) {
	case nil:
		return []interface{}{}
	case []interface{}:
		return _typed
	default:
		return []interface{}{_typed}
	}
}

// -------------------------------------------------------------------------------------
func responseItemID(_items []interface{}, _idx int) string {
	if _idx < 0 || _idx >= len(_items) {
		return ""
	}
	_item, _ok := _items[_idx].(map[string]interface{})
	if !_ok {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(_item["id"]))
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleMultimodalProxy(_w http.ResponseWriter, _r *http.Request, _body []byte, _spec MultimodalEndpointSpec) {
	if _h.Balancer == nil || _h.Client == nil {
		_h.writeJSON(_w, http.StatusServiceUnavailable, domain.ErrorResponse("service_unavailable", "proxy service is not initialized"))
		return
	}

	_meta, _err := multimodalRequestMetaFromBody(_body, _r.Header.Get("Content-Type"))
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", _err.Error()))
		return
	}

	_selectionReq := domain.ChatCompletionRequest{
		Model:                defaultString(_meta.Model, "AUTO"),
		Provider:             _meta.Provider,
		ProviderID:           _meta.ProviderID,
		Stream:               _spec.Streamable && _meta.Stream,
		Messages:             []domain.ChatMessage{{Role: "user", Content: multimodalSelectionText(_spec, _meta)}},
		RequiredCapabilities: []string{_spec.Requirement},
	}
	applyAPIKeyRoutingPolicyToSelectionRequest(_r, &_selectionReq)
	_h.applyLowReasoningDemotion(_r, &_selectionReq)

	_h.refreshConversationBindings()
	_started := time.Now()
	_target, _model, _profile, _selectionMeta, _err := _h.Balancer.Select(&_selectionReq)
	if _err != nil {
		_h.writeSelectionUnavailable(_w, _r, _err)
		return
	}

	noteKeyRequestComplexity(_r, _profile)
	_target.StartRequest()
	defer _target.FinishRequest()

	_timeout := time.Duration(_target.Config.TimeoutSeconds) * time.Second
	if _timeout <= 0 {
		_timeout = time.Duration(domain.DefaultProviderTimeoutSeconds) * time.Second
	}

	_ctx, _cancel := requestForwardContext(_r.Context(), _timeout, _selectionReq.Stream)
	defer _cancel()

	_forwardErr := _h.Client.ForwardMultimodal(_ctx, _w, _r, _target, _model, providerEndpointURL(_target.Config, _spec.Path), _body, _selectionReq.Stream, _profile, _selectionMeta)
	if _forwardErr != nil {
		recordProviderForwardFailure(_target, _forwardErr, _r, time.Since(_started), _h.providerCapacityCooldown(), _h.providerServerErrorCooldown(), _model.Name)
		if proxy.ClassifyFailure(_forwardErr).Quota {
			_h.persistQuotaCooldown(_target)
		}
		if proxy.ResponseAlreadyForwarded(_forwardErr) {
			return
		}
		_h.writeJSON(_w, http.StatusBadGateway, domain.ErrorResponse("provider_error", _forwardErr.Error()))
		return
	}

	_duration := time.Since(_started)
	_target.MarkSuccessWithMetrics(_duration, 0, float64(_duration.Milliseconds()), 0, 0)
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) writeSelectionUnavailable(_w http.ResponseWriter, _r *http.Request, _err error) {
	if _retryAfter := balancer.SelectionRetryAfterSeconds(_err); _retryAfter > 0 {
		_w.Header().Set("Retry-After", strconv.Itoa(_retryAfter))
	}
	if _reason := _h.pinnedRoutingUnavailableReason(_r); _reason != "" {
		_h.writeJSON(_w, http.StatusServiceUnavailable, domain.ErrorResponse("service_unavailable", _reason))
		return
	}
	_h.writeJSON(_w, http.StatusServiceUnavailable, domain.ErrorResponse("service_unavailable", _err.Error()))
}

// -------------------------------------------------------------------------------------
// pinnedRoutingUnavailableReason 在金鑰綁定的 provider／模型已被停用或移除時回傳明確原因，
// 避免與「真的沒有容量」共用同一個通用訊息而難以診斷。
func (_h *HTTPAPI) pinnedRoutingUnavailableReason(_r *http.Request) string {
	if _h == nil || _h.Balancer == nil {
		return ""
	}
	_policy, _ok := apiKeyRoutingPolicyFromRequest(_r)
	if !_ok {
		return ""
	}

	if !strings.EqualFold(_policy.ProviderID, "AUTO") {
		_provider, _found := _h.findProviderConfig(_policy.ProviderID)
		if !_found {
			return fmt.Sprintf("this API key is pinned to provider %q, which no longer exists", _policy.ProviderID)
		}
		if !_provider.Enabled {
			return fmt.Sprintf("this API key is pinned to provider %q, which is currently disabled", defaultString(_provider.Name, _policy.ProviderID))
		}
	}

	if !strings.EqualFold(_policy.Model, "AUTO") && !_h.apiKeyModelExists(_policy.ProviderID, _policy.Model) {
		return apiKeyModelNotFoundMessage(_policy.ProviderID, _policy.Model)
	}
	return ""
}

// -------------------------------------------------------------------------------------
func recordProviderForwardFailure(_provider *balancer.ProviderRuntime, _err error, _request *http.Request, _latency time.Duration, _capacityCooldown time.Duration, _serverCooldown time.Duration, _models ...string) {
	if errors.Is(_err, errResponseBufferLimit) {
		return
	}
	var _bindingErr *turnBindingError
	if errors.As(_err, &_bindingErr) {
		return
	}
	if _provider == nil || _err == nil {
		return
	}
	if errors.Is(_err, context.Canceled) || (_request != nil && _request.Context().Err() != nil) {
		return
	}
	if proxy.IsUpstreamRequestRejected(_err) {
		return
	}
	_policy := proxy.ClassifyFailure(_err)
	if _policy.Request {
		return
	}
	if _policy.Auth || providerForwardFailureIsAuth(_err) {
		_provider.MarkAuthError(_err.Error())
		return
	}
	if _policy.Capacity || _policy.RetryableServer {
		if _policy.Quota {
			if _policy.RetryAfter > 0 {
				_capacityCooldown = _policy.RetryAfter
			}
			_provider.MarkQuotaUnavailable(_latency, _capacityCooldown)
			logProviderCooldown(_provider, _err, _capacityCooldown, _models)
			return
		}
		if _policy.TransientCapacity {
			_capacityCooldown = _provider.NextOverloadBackoff(_policy.RetryAfter)
		} else if _policy.RetryableServer {
			_capacityCooldown = _serverCooldown
			if _capacityCooldown <= 0 {
				_capacityCooldown = 30 * time.Second
			}
			if _policy.RetryAfter > 0 {
				_capacityCooldown = _policy.RetryAfter
			}
		} else if _policy.RetryAfter > 0 {
			_capacityCooldown = _policy.RetryAfter
		}
		if _policy.ModelOnly && len(_models) > 0 && _models[0] != "" {
			_provider.MarkModelUnavailable(_models[0], _latency, _capacityCooldown)
			logProviderCooldown(_provider, _err, _capacityCooldown, _models)
			return
		}
		_provider.MarkTemporaryUnavailable(_latency, _capacityCooldown)
		logProviderCooldown(_provider, _err, _capacityCooldown, _models)
		return
	}
	if providerForwardFailureCountsForCircuit(_err, _request) {
		_provider.MarkFailure(_latency)
	}
}

// -------------------------------------------------------------------------------------
func logProviderCooldown(provider *balancer.ProviderRuntime, err error, duration time.Duration, models []string) {
	if provider.Config == nil {
		return
	}
	model := ""
	if len(models) > 0 {
		model = models[0]
	}
	stage := "before_response"
	statusCode := 0
	var stream *proxy.ProviderStreamError
	var status *proxy.ProviderStatusError
	if errors.As(err, &stream) {
		stage = "stream_before_content"
		if stream.ResponseForwarded {
			stage = "stream_after_content"
		}
	} else if errors.As(err, &status) {
		stage = "http_rejection"
		statusCode = status.StatusCode
	}
	policy := proxy.ClassifyFailure(err)
	log.Printf("provider cooldown: provider=%q model=%q stage=%s status=%d capacity=%t server_error=%t delay=%s retry_after=%s until=%s action=%s quota=%t",
		provider.Config.ID, model, stage, statusCode, policy.Capacity, policy.RetryableServer,
		duration, policy.RetryAfter, provider.CooldownUntil(model).UTC().Format(time.RFC3339Nano), policy.Action(), policy.Quota)
}

func providerForwardFailureCountsForCircuit(_err error, _request *http.Request) bool {
	if _err == nil {
		return false
	}
	if _request != nil && _request.Context().Err() == context.Canceled {
		return false
	}
	if errors.Is(_err, context.Canceled) {
		return false
	}
	_errorText := strings.ToLower(_err.Error())
	if strings.Contains(_errorText, "broken pipe") ||
		strings.Contains(_errorText, "connection reset by peer") ||
		strings.Contains(_errorText, "client disconnected") ||
		strings.Contains(_errorText, "stream closed") {
		return false
	}

	var _providerStatusErr *proxy.ProviderStatusError
	if errors.As(_err, &_providerStatusErr) {
		return _providerStatusErr.StatusCode == http.StatusRequestTimeout ||
			_providerStatusErr.StatusCode == http.StatusTooManyRequests ||
			_providerStatusErr.StatusCode >= http.StatusInternalServerError
	}
	return true
}

// -------------------------------------------------------------------------------------
func providerForwardFailureIsAuth(_err error) bool {
	if _err == nil {
		return false
	}
	var _providerStatusErr *proxy.ProviderStatusError
	if errors.As(_err, &_providerStatusErr) {
		return _providerStatusErr.StatusCode == http.StatusUnauthorized || _providerStatusErr.StatusCode == http.StatusForbidden
	}
	_errorText := strings.ToLower(_err.Error())
	return strings.Contains(_errorText, "status 401") ||
		strings.Contains(_errorText, "status 403") ||
		strings.Contains(_errorText, "unauthorized") ||
		strings.Contains(_errorText, "invalid api key") ||
		strings.Contains(_errorText, "invalid token") ||
		strings.Contains(_errorText, "token expired")
}

// -------------------------------------------------------------------------------------
func isMultimodalProxyRoute(_route string) bool {
	_, _ok := multimodalEndpointSpecForRoute(_route)
	return _ok
}

// -------------------------------------------------------------------------------------
func isResponsesProxyRoute(_method string, _route string) bool {
	if _method == http.MethodPost && (_route == "/v1/responses" || _route == "/api/v1/responses") {
		return true
	}
	return isResponsesProxySubroute(_method, _route)
}

// -------------------------------------------------------------------------------------
func isResponsesProxySubroute(_method string, _route string) bool {
	_path := normalizeProxyRoute(_route)
	_parts := strings.Split(strings.Trim(_path, "/"), "/")
	if len(_parts) < 3 || _parts[0] != "v1" || _parts[1] != "responses" {
		return false
	}
	if _method == http.MethodPost && len(_parts) == 3 && _parts[2] == "input_tokens" {
		return true
	}
	if _method == http.MethodPost && len(_parts) == 3 && _parts[2] == "compact" {
		return true
	}
	if strings.TrimSpace(_parts[2]) == "" || _parts[2] == "input_tokens" || _parts[2] == "compact" {
		return false
	}
	if _method == http.MethodGet && len(_parts) == 3 {
		return true
	}
	if _method == http.MethodGet && len(_parts) == 4 && (_parts[3] == "input_items" || _parts[3] == "output_items") {
		return true
	}
	if _method == http.MethodPost && len(_parts) == 4 && (_parts[3] == "cancel" || _parts[3] == "compact") {
		return true
	}
	if _method == http.MethodDelete && len(_parts) == 3 {
		return true
	}
	return false
}

// -------------------------------------------------------------------------------------
func responseRouteIsDelete(_route proxy.ResponsesProxyRoute) bool {
	return strings.EqualFold(strings.TrimSpace(_route.Method), http.MethodDelete) && responsesRouteResponseID(_route.Path) != ""
}

// -------------------------------------------------------------------------------------
func responsesProxyRouteFromRequest(_r *http.Request, _route string) proxy.ResponsesProxyRoute {
	_query := ""
	if _r != nil && _r.URL != nil {
		_query = _r.URL.RawQuery
	}
	_method := ""
	if _r != nil {
		_method = strings.ToUpper(strings.TrimSpace(_r.Method))
	}
	return proxy.ResponsesProxyRoute{
		Method: _method,
		Path:   normalizeProxyRoute(_route),
		Query:  _query,
	}
}

// -------------------------------------------------------------------------------------
func responsesRouteResponseID(_route string) string {
	_path := normalizeProxyRoute(_route)
	_parts := strings.Split(strings.Trim(_path, "/"), "/")
	if len(_parts) < 3 || _parts[0] != "v1" || _parts[1] != "responses" {
		return ""
	}
	if _parts[2] == "input_tokens" || _parts[2] == "compact" {
		return ""
	}
	return strings.TrimSpace(_parts[2])
}

// -------------------------------------------------------------------------------------
func multimodalEndpointSpecForRoute(_route string) (MultimodalEndpointSpec, bool) {
	_normalized := normalizeProxyRoute(_route)
	_specs := map[string]MultimodalEndpointSpec{
		"/v1/responses": {
			Path:        "/v1/responses",
			Requirement: "responses",
			TaskType:    "chat",
			Streamable:  true,
		},
		"/v1/images/generations": {
			Path:        "/v1/images/generations",
			Requirement: "image_generation",
			TaskType:    "image_generation",
		},
		"/v1/images/edits": {
			Path:        "/v1/images/edits",
			Requirement: "image_edit",
			TaskType:    "image_generation",
		},
		"/v1/images/variations": {
			Path:        "/v1/images/variations",
			Requirement: "image_variation",
			TaskType:    "image_generation",
		},
		"/v1/audio/transcriptions": {
			Path:        "/v1/audio/transcriptions",
			Requirement: "transcription",
			TaskType:    "transcription",
		},
		"/v1/audio/translations": {
			Path:        "/v1/audio/translations",
			Requirement: "audio_translation",
			TaskType:    "translation",
		},
		"/v1/audio/speech": {
			Path:        "/v1/audio/speech",
			Requirement: "tts",
			TaskType:    "tts",
		},
		"/v1/videos/analysis": {
			Path:        "/v1/videos/analysis",
			Requirement: "video_analysis",
			TaskType:    "video_analysis",
		},
		"/v1/videos/analyze": {
			Path:        "/v1/videos/analyze",
			Requirement: "video_analysis",
			TaskType:    "video_analysis",
		},
		"/v1/videos/generations": {
			Path:        "/v1/videos/generations",
			Requirement: "video_generation",
			TaskType:    "video_generation",
		},
	}
	_spec, _ok := _specs[_normalized]
	return _spec, _ok
}

// -------------------------------------------------------------------------------------
func normalizeProxyRoute(_route string) string {
	_route = strings.TrimRight(strings.TrimSpace(_route), "/")
	if strings.HasPrefix(_route, "/api/v1/") {
		return strings.TrimPrefix(_route, "/api")
	}
	return _route
}

// -------------------------------------------------------------------------------------
func providerEndpointURL(_provider *domain.LLMProviderConfig, _path string) string {
	if _provider == nil {
		return _path
	}
	return strings.TrimRight(_provider.BaseURL, "/") + "/" + strings.TrimLeft(_path, "/")
}

// -------------------------------------------------------------------------------------
func responsesSelectionRequest(_body []byte) (domain.ChatCompletionRequest, error) {
	_payload := map[string]interface{}{}
	if len(strings.TrimSpace(string(_body))) > 0 {
		if _err := json.Unmarshal(_body, &_payload); _err != nil {
			return domain.ChatCompletionRequest{}, fmt.Errorf("request body is not a valid responses payload")
		}
	}

	_text := responsesSelectionText(_payload)
	if strings.TrimSpace(_text) == "" {
		_text = "/v1/responses"
	}

	return domain.ChatCompletionRequest{
		Model:                defaultString(stringValue(_payload["model"]), "AUTO"),
		Provider:             stringValue(_payload["provider"]),
		ProviderID:           stringValue(_payload["provider_id"]),
		Stream:               boolValue(_payload["stream"]),
		Messages:             []domain.ChatMessage{{Role: "user", Content: _text}},
		RequiredCapabilities: []string{"responses"},
	}, nil
}

// -------------------------------------------------------------------------------------
func responsesRouteSelectionRequest(_route proxy.ResponsesProxyRoute, _body []byte) (domain.ChatCompletionRequest, error) {
	_req, _err := responsesSelectionRequest(_body)
	if _err != nil {
		return domain.ChatCompletionRequest{}, _err
	}
	_path := normalizeProxyRoute(_route.Path)
	_req.Stream = false
	_req.RequiredCapabilities = []string{"responses"}
	if len(_req.Messages) == 0 {
		_req.Messages = []domain.ChatMessage{{Role: "user", Content: _path}}
	} else if _text, _ok := _req.Messages[0].Content.(string); !_ok || strings.TrimSpace(_text) == "" || _text == "/v1/responses" {
		_req.Messages[0].Content = _path
	}
	if strings.TrimSpace(_req.Model) == "" {
		_req.Model = "AUTO"
	}
	return _req, nil
}

// -------------------------------------------------------------------------------------
func responsesSelectionText(_payload map[string]interface{}) string {
	_parts := make([]string, 0, 4)
	for _, _key := range []string{"instructions", "prompt", "text"} {
		if _text := stringValue(_payload[_key]); _text != "" {
			_parts = append(_parts, _text)
		}
	}
	if _inputText := responsesInputText(_payload["input"]); _inputText != "" {
		_parts = append(_parts, _inputText)
	}
	return strings.Join(_parts, "\n")
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) handleLocalTokenCount(_w http.ResponseWriter, _body []byte) {
	var _payload interface{}
	if _err := json.Unmarshal(_body, &_payload); _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "request body is not valid JSON"))
		return
	}
	_encoded, _err := json.Marshal(_payload)
	if _err != nil {
		_h.writeJSON(_w, http.StatusBadRequest, domain.ErrorResponse("invalid_request_error", "request body cannot be counted"))
		return
	}
	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{"input_tokens": estimateLocalInputTokens(string(_encoded))})
}

// -------------------------------------------------------------------------------------
func estimateLocalInputTokens(_text string) int {
	_chineseChars := 0
	_otherChars := 0
	for _, _character := range _text {
		if unicode.In(_character, unicode.Han) {
			_chineseChars++
		} else {
			_otherChars++
		}
	}
	_tokens := int(math.Ceil(float64(_chineseChars)/1.5+float64(_otherChars)/4.0)) + 8
	if utf8.RuneCountInString(_text) > 0 && _tokens < 1 {
		return 1
	}
	return _tokens
}

// -------------------------------------------------------------------------------------
func responsesInputText(_value interface{}) string {
	switch _typed := _value.(type) {
	case string:
		return strings.TrimSpace(_typed)
	case []interface{}:
		_parts := make([]string, 0, len(_typed))
		for _, _item := range _typed {
			if _text := responsesInputText(_item); _text != "" {
				_parts = append(_parts, _text)
			}
		}
		return strings.Join(_parts, "\n")
	case map[string]interface{}:
		_parts := make([]string, 0)
		if _text := stringValue(_typed["text"]); _text != "" {
			_parts = append(_parts, _text)
		}
		if _content, _ok := _typed["content"]; _ok {
			if _text := responsesInputText(_content); _text != "" {
				_parts = append(_parts, _text)
			}
		}
		return strings.Join(_parts, "\n")
	default:
		return ""
	}
}

// -------------------------------------------------------------------------------------
func multimodalRequestMetaFromBody(_body []byte, _contentType string) (MultimodalRequestMeta, error) {
	_mediaType := ""
	_params := map[string]string{}
	if strings.TrimSpace(_contentType) != "" {
		if _parsedMediaType, _parsedParams, _err := mime.ParseMediaType(_contentType); _err == nil {
			_mediaType = strings.ToLower(strings.TrimSpace(_parsedMediaType))
			_params = _parsedParams
		}
	}

	if _mediaType == "multipart/form-data" {
		return multimodalRequestMetaFromMultipart(_body, _params["boundary"])
	}
	return multimodalRequestMetaFromJSON(_body)
}

// -------------------------------------------------------------------------------------
func multimodalRequestMetaFromJSON(_body []byte) (MultimodalRequestMeta, error) {
	if len(strings.TrimSpace(string(_body))) == 0 {
		return MultimodalRequestMeta{Model: "AUTO"}, nil
	}

	var _payload map[string]interface{}
	if _err := json.Unmarshal(_body, &_payload); _err != nil {
		return MultimodalRequestMeta{}, fmt.Errorf("request body is not a valid JSON payload")
	}

	return MultimodalRequestMeta{
		Model:      stringValue(_payload["model"]),
		Provider:   stringValue(_payload["provider"]),
		ProviderID: stringValue(_payload["provider_id"]),
		Text:       multimodalTextFromPayload(_payload),
		Stream:     boolValue(_payload["stream"]),
	}, nil
}

// -------------------------------------------------------------------------------------
func multimodalRequestMetaFromMultipart(_body []byte, _boundary string) (MultimodalRequestMeta, error) {
	if strings.TrimSpace(_boundary) == "" {
		return MultimodalRequestMeta{}, fmt.Errorf("multipart boundary is required")
	}

	_reader := multipart.NewReader(bytes.NewReader(_body), _boundary)
	_meta := MultimodalRequestMeta{}
	for {
		_part, _err := _reader.NextPart()
		if errors.Is(_err, io.EOF) {
			break
		}
		if _err != nil {
			return MultimodalRequestMeta{}, _err
		}
		if _part.FileName() != "" {
			_ = _part.Close()
			continue
		}
		_valueBytes, _readErr := io.ReadAll(io.LimitReader(_part, 1024*1024))
		_ = _part.Close()
		if _readErr != nil {
			return MultimodalRequestMeta{}, _readErr
		}
		_value := strings.TrimSpace(string(_valueBytes))
		switch _part.FormName() {
		case "model":
			_meta.Model = _value
		case "provider":
			_meta.Provider = _value
		case "provider_id":
			_meta.ProviderID = _value
		case "prompt", "input", "text":
			if _meta.Text == "" {
				_meta.Text = _value
			}
		case "stream":
			_meta.Stream = strings.EqualFold(_value, "true") || _value == "1"
		}
	}
	return _meta, nil
}

// -------------------------------------------------------------------------------------
func multimodalSelectionText(_spec MultimodalEndpointSpec, _meta MultimodalRequestMeta) string {
	_text := strings.TrimSpace(_meta.Text)
	if _text == "" {
		_text = _spec.Path
	}
	return strings.TrimSpace(_spec.TaskType + " " + _text)
}

// -------------------------------------------------------------------------------------
func multimodalTextFromPayload(_payload map[string]interface{}) string {
	for _, _key := range []string{"prompt", "input", "text"} {
		if _text := stringValue(_payload[_key]); strings.TrimSpace(_text) != "" {
			return _text
		}
	}
	if _messages, _ok := _payload["messages"].([]interface{}); _ok {
		_parts := make([]string, 0, len(_messages))
		for _, _item := range _messages {
			_message, _ok := _item.(map[string]interface{})
			if !_ok {
				continue
			}
			if _text := stringValue(_message["content"]); _text != "" {
				_parts = append(_parts, _text)
			}
		}
		return strings.Join(_parts, "\n")
	}
	return ""
}

// -------------------------------------------------------------------------------------
func stringValue(_value interface{}) string {
	switch _typed := _value.(type) {
	case string:
		return strings.TrimSpace(_typed)
	case []interface{}, map[string]interface{}:
		_raw, _err := json.Marshal(_typed)
		if _err == nil {
			return string(_raw)
		}
	}
	return ""
}

// -------------------------------------------------------------------------------------
func boolValue(_value interface{}) bool {
	_bool, _ok := _value.(bool)
	return _ok && _bool
}

// -------------------------------------------------------------------------------------
func providerIDFromRoute(_route string) string {
	_items := strings.Split(strings.Trim(_route, "/"), "/")
	if len(_items) == 0 {
		return ""
	}
	return strings.TrimSpace(_items[len(_items)-1])
}

// -------------------------------------------------------------------------------------
func benchmarkIDFromRoute(_route string) string {
	_items := strings.Split(strings.Trim(_route, "/"), "/")
	if len(_items) < 3 {
		return ""
	}
	return strings.TrimSpace(_items[len(_items)-1])
}

// -------------------------------------------------------------------------------------
func benchmarkIDFromCancelRoute(_route string) string {
	_items := strings.Split(strings.Trim(_route, "/"), "/")
	if len(_items) < 4 || _items[len(_items)-1] != "cancel" {
		return ""
	}
	return strings.TrimSpace(_items[len(_items)-2])
}

// -------------------------------------------------------------------------------------
func isProviderActionRoute(_route string, _action string) bool {
	return strings.HasPrefix(_route, "/api/provider-configs/") && strings.HasSuffix(_route, "/"+_action) ||
		strings.HasPrefix(_route, "/v1/provider-configs/") && strings.HasSuffix(_route, "/"+_action)
}

// -------------------------------------------------------------------------------------
func providerIDFromActionRoute(_route string, _action string) string {
	_items := strings.Split(strings.Trim(_route, "/"), "/")
	if len(_items) < 3 || _items[len(_items)-1] != _action {
		return ""
	}
	return strings.TrimSpace(_items[len(_items)-2])
}

// -------------------------------------------------------------------------------------
func isAPIKeyActionRoute(_route string, _action string) bool {
	_, _routeAction, _ok := apiKeyIDAndActionFromRoute(_route)
	return _ok && _routeAction == _action
}

// -------------------------------------------------------------------------------------
func isSettingsRoute(_route string, _parts ...string) bool {
	if len(_parts) == 0 {
		return false
	}
	_items := strings.Split(strings.Trim(_route, "/"), "/")
	for _idx, _item := range _items {
		if _item != "settings" {
			continue
		}
		if _idx+len(_parts) >= len(_items) {
			return false
		}
		_match := true
		for _partIdx, _part := range _parts {
			if _items[_idx+1+_partIdx] != _part {
				_match = false
				break
			}
		}
		if _match && _idx+1+len(_parts) == len(_items) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func apiKeyIDFromActionRoute(_route string, _action string) string {
	_id, _routeAction, _ok := apiKeyIDAndActionFromRoute(_route)
	if !_ok || _routeAction != _action {
		return ""
	}
	return _id
}

// -------------------------------------------------------------------------------------
func isAPIKeyUsageRoute(_route string) bool {
	_, _action, _ok := apiKeyIDAndActionFromRoute(_route)
	return _ok && _action == "usage"
}

// -------------------------------------------------------------------------------------
func isAPIKeyDensityRoute(_route string) bool {
	return _route == "/api/api-keys/density" || _route == "/v1/api-keys/density"
}

// -------------------------------------------------------------------------------------
func isAPIKeyUsageQueryRoute(_route string) bool {
	return _route == "/api/api-keys/usage" || _route == "/v1/api-keys/usage"
}

// -------------------------------------------------------------------------------------
func apiKeyIDFromUsageRoute(_route string) string {
	_id, _action, _ok := apiKeyIDAndActionFromRoute(_route)
	if !_ok || _action != "usage" {
		return ""
	}
	return _id
}

// -------------------------------------------------------------------------------------
func apiKeyIDFromRoute(_route string) string {
	_id, _, _ok := apiKeyIDAndActionFromRoute(_route)
	if !_ok {
		return ""
	}
	return _id
}

// -------------------------------------------------------------------------------------
func apiKeyIDAndActionFromRoute(_route string) (string, string, bool) {
	_items := strings.Split(strings.Trim(_route, "/"), "/")
	for _idx, _item := range _items {
		if _item != "api-keys" {
			continue
		}
		if _idx+1 >= len(_items) {
			return "", "", false
		}
		_id := strings.TrimSpace(_items[_idx+1])
		if _id == "" {
			return "", "", false
		}
		_action := ""
		if _idx+2 < len(_items) {
			_action = strings.TrimSpace(_items[_idx+2])
		}
		return _id, _action, true
	}
	return "", "", false
}

// -------------------------------------------------------------------------------------
func providerConfigToForm(_provider domain.LLMProviderConfig) ProviderForm {
	_model := ""
	_capabilities := []string{}
	if len(_provider.Models) > 0 {
		_model = _provider.Models[0].Name
		_capabilities = append([]string(nil), _provider.Models[0].Capabilities...)
	}
	_hasAPIKey := _provider.APIKey != "" || _provider.APIKeyEnv != ""
	_oauthConnected := false
	_oauthAccount := ""
	if strings.EqualFold(inferProviderKind(_provider), "openai-codex") {
		if _status, _err := codexauth.StatusFor(_provider.ID); _err == nil && _status.Status == "connected" {
			_oauthConnected = true
			_oauthAccount = defaultString(_status.AccountEmail, _status.AccountName)
		}
	}

	return ProviderForm{
		ID:              _provider.ID,
		Name:            _provider.Name,
		Kind:            inferProviderKind(_provider),
		Role:            defaultString(_provider.Role, "main"),
		APIKey:          "",
		APIKeyMasked:    maskedAPIKey(_provider),
		HasAPIKey:       _hasAPIKey,
		OAuth:           _oauthConnected,
		OAuthAccount:    _oauthAccount,
		Host:            _provider.BaseURL,
		ChatAPI:         _provider.ChatCompletionsPath,
		Model:           _model,
		Purpose:         defaultString(_provider.Purpose, "對話"),
		Scale:           defaultString(_provider.Scale, "中"),
		Responsibility:  _provider.Responsibility,
		ReasoningEffort: normalizeReasoningEffortForKind(inferProviderKind(_provider), _provider.ReasoningEffort),
		Capabilities:    normalizeCapabilities(_capabilities),
		Enabled:         _provider.Enabled,
		MaxConcurrent:   _provider.MaxConcurrent,
		Priority:        _provider.Priority,
	}
}

// -------------------------------------------------------------------------------------
func maskedAPIKey(_provider domain.LLMProviderConfig) string {
	if _provider.APIKey == "" && _provider.APIKeyEnv == "" {
		return ""
	}

	if _provider.APIKeyEnv != "" && _provider.APIKey == "" {
		return "•••••••• (" + _provider.APIKeyEnv + ")"
	}

	return "••••••••••••"
}

// -------------------------------------------------------------------------------------
func formToProviderConfig(_form ProviderForm) domain.LLMProviderConfig {
	_kind := defaultString(_form.Kind, "custom")
	_modelName := defaultString(_form.Model, "custom-model")
	if strings.EqualFold(inferProviderKind(domain.LLMProviderConfig{Kind: _kind}), "openai-codex") {
		_modelName = normalizeOpenAICodexModelID(_modelName)
	}
	_scale := defaultString(_form.Scale, "中")
	_purpose := defaultString(_form.Purpose, "對話")
	_capabilities := normalizeCapabilities(_form.Capabilities)
	if len(_capabilities) == 0 {
		_capabilities = capabilitiesForKindAndPurpose(_kind, _purpose)
	}

	return domain.LLMProviderConfig{
		ID:                  defaultString(_form.ID, generateProviderID(_kind)),
		Name:                defaultString(_form.Name, _kind),
		Kind:                _kind,
		Role:                defaultString(_form.Role, "main"),
		Type:                "openai_compatible",
		BaseURL:             strings.TrimRight(strings.TrimSpace(_form.Host), "/"),
		APIKey:              _form.APIKey,
		ChatCompletionsPath: defaultString(_form.ChatAPI, "/v1/chat/completions"),
		Enabled:             _form.Enabled,
		Weight:              10,
		Priority:            positiveInt(_form.Priority, 1),
		TimeoutSeconds:      domain.DefaultProviderTimeoutSeconds,
		MaxConcurrent:       positiveInt64(_form.MaxConcurrent, 4),
		Purpose:             _purpose,
		Scale:               _scale,
		Responsibility:      _form.Responsibility,
		ReasoningEffort:     normalizeReasoningEffortForKind(_kind, _form.ReasoningEffort),
		Models: []domain.LLMModelConfig{
			{
				Name:            _modelName,
				Aliases:         []string{"auto", _kind},
				MaxInputTokens:  maxInputTokensForKind(_kind),
				MaxOutputTokens: maxOutputTokensForKind(_kind),
				Capabilities:    _capabilities,
				CostTier:        costTierForScale(_scale),
				QualityTier:     qualityTierForScale(_scale),
			},
		},
	}
}

// -------------------------------------------------------------------------------------
func mergeProviderConfig(_old domain.LLMProviderConfig, _form ProviderForm) domain.LLMProviderConfig {
	_updated := formToProviderConfig(_form)

	_updated.APIKeyEnv = _old.APIKeyEnv
	if _updated.APIKey == "" {
		_updated.APIKey = _old.APIKey
	}
	if _old.Weight > 0 {
		_updated.Weight = _old.Weight
	}
	if _form.Priority <= 0 && _old.Priority > 0 {
		_updated.Priority = _old.Priority
	}
	if _old.TimeoutSeconds > 0 {
		_updated.TimeoutSeconds = _old.TimeoutSeconds
	}
	if _form.Role == "" {
		_updated.Role = defaultString(_old.Role, "main")
	}

	if len(_old.Models) > 0 && len(_updated.Models) > 0 {
		_matchedModel := false
		for _, _model := range _old.Models {
			if _model.Name == _updated.Models[0].Name {
				_updated.Models[0].MaxInputTokens = _model.MaxInputTokens
				_updated.Models[0].MaxOutputTokens = _model.MaxOutputTokens
				_updated.Models[0].CostTier = _model.CostTier
				_updated.Models[0].QualityTier = _model.QualityTier
				if len(_model.Aliases) > 0 {
					_updated.Models[0].Aliases = _model.Aliases
				}
				if len(_form.Capabilities) == 0 && len(_model.Capabilities) > 0 {
					_updated.Models[0].Capabilities = _model.Capabilities
				}
				_matchedModel = true
				break
			}
		}
		if !_matchedModel {
			if _old.Models[0].MaxInputTokens > 0 {
				_updated.Models[0].MaxInputTokens = _old.Models[0].MaxInputTokens
			}
			if _old.Models[0].MaxOutputTokens > 0 {
				_updated.Models[0].MaxOutputTokens = _old.Models[0].MaxOutputTokens
			}
			if _old.Models[0].CostTier > 0 {
				_updated.Models[0].CostTier = _old.Models[0].CostTier
			}
			if _old.Models[0].QualityTier > 0 {
				_updated.Models[0].QualityTier = _old.Models[0].QualityTier
			}
			if len(_form.Capabilities) == 0 && len(_old.Models[0].Capabilities) > 0 {
				_updated.Models[0].Capabilities = _old.Models[0].Capabilities
			}
			_aliases := append([]string(nil), internalProviderModelAliases(&_updated, _updated.Models[0].Aliases)...)
			for _, _model := range _old.Models {
				_aliases = append(_aliases, internalProviderModelAliases(&_updated, _model.Aliases)...)
			}
			_updated.Models[0].Aliases = uniqueSortedStrings(_aliases)
		}
	}
	if len(_updated.Models) > 0 && strings.EqualFold(_updated.Kind, "llamacpp") && _updated.Models[0].MaxInputTokens < maxInputTokensForKind(_updated.Kind) {
		_updated.Models[0].MaxInputTokens = maxInputTokensForKind(_updated.Kind)
	}

	return _updated
}

// -------------------------------------------------------------------------------------
func inferProviderKind(_provider domain.LLMProviderConfig) string {
	if _provider.Kind != "" {
		return _provider.Kind
	}

	_text := strings.ToLower(_provider.Name + " " + _provider.BaseURL + " " + _provider.ID)
	switch {
	case strings.Contains(_text, "codex"):
		return "openai-codex"
	case strings.Contains(_text, "openai"):
		return "openai"
	case strings.Contains(_text, "minimax"):
		return "minimax"
	case strings.Contains(_text, "ollama"):
		return "ollama"
	case strings.Contains(_text, "omlx") || strings.Contains(_text, "mlx"):
		return "omlx"
	case strings.Contains(_text, "vllm"):
		return "vllm"
	case strings.Contains(_text, "llama.cpp") || strings.Contains(_text, "llamacpp"):
		return "llamacpp"
	default:
		return "custom"
	}
}

// -------------------------------------------------------------------------------------
func capabilitiesForKindAndPurpose(_kind string, _purpose string) []string {
	_capabilities := capabilitiesForPurpose(_purpose)
	switch strings.ToLower(strings.TrimSpace(_kind)) {
	case "openai":
		_capabilities = append(_capabilities, "responses", "vision", "image_analysis", "image_generation", "image_edit", "image_variation", "audio_analysis", "transcription", "audio_translation", "tts", "tools", "function_calling", "json_mode", "long_context")
	case "openai-codex":
		_capabilities = append(_capabilities, "responses", "coding", "reasoning", "tools", "function_calling", "long_context", "vision", "image_analysis")
	case "ollama":
		_capabilities = append(_capabilities, "vision", "image_analysis")
	case "omlx":
		_capabilities = append(_capabilities, "tools", "function_calling", "json_mode", "json", "long_context")
	case "vllm":
		_capabilities = append(_capabilities, "tools", "function_calling", "json_mode", "long_context")
	}
	return uniqueSortedStrings(_capabilities)
}

// -------------------------------------------------------------------------------------
func normalizeCapabilities(_capabilities []string) []string {
	_normalized := make([]string, 0, len(_capabilities))
	_aliases := map[string]string{
		"image":            "vision",
		"image_analysis":   "image_analysis",
		"vision":           "vision",
		"function_call":    "function_calling",
		"function_calling": "function_calling",
		"json":             "json",
		"json_mode":        "json_mode",
		"audio":            "audio_analysis",
		"speech":           "tts",
		"text_to_speech":   "tts",
		"transcribe":       "transcription",
		"video":            "video_analysis",
	}
	for _, _capability := range _capabilities {
		_capability = strings.ToLower(strings.TrimSpace(_capability))
		if _capability == "" {
			continue
		}
		if _alias, _ok := _aliases[_capability]; _ok {
			_capability = _alias
		}
		_normalized = append(_normalized, _capability)
	}
	return uniqueSortedStrings(_normalized)
}

// -------------------------------------------------------------------------------------
func normalizeReasoningEffortForKind(_kind string, _effort string) string {
	_effort = strings.ToLower(strings.TrimSpace(_effort))
	if !strings.EqualFold(strings.TrimSpace(_kind), "openai-codex") {
		return _effort
	}
	switch _effort {
	case "":
		return "high"
	case "none", "minimal", "low", "medium", "high", "xhigh":
		return _effort
	default:
		return "high"
	}
}

// -------------------------------------------------------------------------------------
func capabilitiesForPurpose(_purpose string) []string {
	switch _purpose {
	case "文件轉換":
		return []string{"chat", "summarization", "extraction", "translation"}
	case "影像分析":
		return []string{"vision", "image_analysis", "extraction"}
	case "影像生成":
		return []string{"image_generation", "image_edit", "image_variation"}
	case "影片分析":
		return []string{"video_analysis", "extraction"}
	case "影片生成":
		return []string{"video_generation"}
	case "聲音分析":
		return []string{"audio_analysis", "transcription", "audio_translation", "extraction"}
	case "聲音生成":
		return []string{"audio_generation", "tts"}
	default:
		return []string{"chat", "reasoning", "responses"}
	}
}

// -------------------------------------------------------------------------------------
func maxInputTokensForKind(_kind string) int {
	return 1048576
}

// -------------------------------------------------------------------------------------
func maxOutputTokensForKind(_kind string) int {
	return 262144
}

// -------------------------------------------------------------------------------------
func qualityTierForScale(_scale string) int {
	switch _scale {
	case "大":
		return 8
	case "中":
		return 6
	case "小":
		return 4
	case "極小":
		return 2
	default:
		return 5
	}
}

// -------------------------------------------------------------------------------------
func costTierForScale(_scale string) int {
	switch _scale {
	case "大":
		return 5
	case "中":
		return 3
	case "小":
		return 2
	case "極小":
		return 1
	default:
		return 3
	}
}

// -------------------------------------------------------------------------------------
func defaultString(_value string, _fallback string) string {
	_value = strings.TrimSpace(_value)
	if _value == "" {
		return _fallback
	}
	return _value
}

// -------------------------------------------------------------------------------------
func positiveInt64(_value int64, _fallback int64) int64 {
	if _value <= 0 {
		return _fallback
	}
	return _value
}

// -------------------------------------------------------------------------------------
func positiveInt(_value int, _fallback int) int {
	if _value <= 0 {
		return _fallback
	}
	return _value
}

// -------------------------------------------------------------------------------------
func generateProviderID(_kind string) string {
	_kind = strings.ToLower(strings.TrimSpace(_kind))
	if _kind == "" {
		_kind = "provider"
	}

	var _buf [4]byte
	if _, _err := rand.Read(_buf[:]); _err == nil {
		return fmt.Sprintf("%s-%s", _kind, hex.EncodeToString(_buf[:]))
	}

	return fmt.Sprintf("%s-%d", _kind, time.Now().UnixNano())
}

// -------------------------------------------------------------------------------------
func (_h *HTTPAPI) writeJSON(_w http.ResponseWriter, _status int, _payload interface{}) {
	_w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_w.WriteHeader(_status)
	_ = json.NewEncoder(_w).Encode(_payload)
}

// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
const (
	defaultKeyDensityWindow = 5 * time.Minute
	maxKeyDensityWindow     = time.Hour
)

// -------------------------------------------------------------------------------------
// handleGetAPIKeyDensity 回傳所有金鑰的請求密集度（依複雜度分級）。
// 以金鑰清單為主體：沒有流量的金鑰也會列出（計數為 0），方便直接比較。
func (_h *HTTPAPI) handleGetAPIKeyDensity(_w http.ResponseWriter, _window string) {
	_duration := parseKeyDensityWindow(_window)
	_settings := _h.currentAdvancedSettings()
	_yieldThresholds := keyusage.YieldThresholds{
		LowMaxRatio: _settings.YieldLowMaxPercent / 100,
		MidMaxRatio: _settings.YieldMidMaxPercent / 100,
	}

	_densities := map[string]keyusage.RequestDensity{}
	for _, _density := range keyusage.DefaultRecorder().AllRequestDensityWithYieldThresholds(_duration, _yieldThresholds) {
		_densities[_density.KeyID] = _density
	}

	_keys, _err := auth.DefaultAPIKeyStore().List()
	if _err != nil {
		_h.writeJSON(_w, http.StatusInternalServerError, domain.ErrorResponse("load_failed", _err.Error()))
		return
	}

	_items := make([]map[string]interface{}, 0, len(_keys))
	_seen := map[string]bool{}
	_total := 0
	_totalTokens := 0
	for _, _key := range _keys {
		_density := _densities[_key.ID]
		_seen[_key.ID] = true
		_total += _density.Count
		_totalTokens += _density.Tokens
		_items = append(_items, keyDensityItem(_key.Name, _key.KeyType, _key.Enabled, _density))
	}
	// 已刪除但視窗內仍有樣本的金鑰也要列出，否則流量會憑空消失。
	for _id, _density := range _densities {
		if _seen[_id] {
			continue
		}
		_ = _id
		_total += _density.Count
		_totalTokens += _density.Tokens
		_items = append(_items, keyDensityItem("(已刪除)", "", false, _density))
	}

	// 以實際消耗排序：這份清單要回答的是「誰在燒配額」，
	// 請求數只是次要佐證（尚未完成的請求還沒有可信的消耗量）。
	sort.Slice(_items, func(_i int, _j int) bool {
		if _left, _right := _items[_i]["tokens"].(int), _items[_j]["tokens"].(int); _left != _right {
			return _left > _right
		}
		if _left, _right := _items[_i]["count"].(int), _items[_j]["count"].(int); _left != _right {
			return _left > _right
		}
		return _items[_i]["name"].(string) < _items[_j]["name"].(string)
	})

	_h.writeJSON(_w, http.StatusOK, map[string]interface{}{
		"window_seconds": _duration.Seconds(),
		"total_requests": _total,
		"total_tokens":   _totalTokens,
		"yield_thresholds": map[string]float64{
			"low_max_percent": _settings.YieldLowMaxPercent,
			"mid_max_percent": _settings.YieldMidMaxPercent,
		},
		"keys": _items,
	})
}

// -------------------------------------------------------------------------------------
// keyDensityItem 刻意不輸出任何金鑰識別資訊（ID、前綴、遮罩值）：
// 這份清單只用於觀察用量與頻率，名稱已足夠辨識。
func keyDensityItem(_name string, _keyType string, _enabled bool, _density keyusage.RequestDensity) map[string]interface{} {
	return map[string]interface{}{
		"name":                    _name,
		"key_type":                _keyType,
		"enabled":                 _enabled,
		"count":                   _density.Count,
		"per_minute":              _density.PerMinute,
		"completed_requests":      _density.CompletedRequests,
		"tokens":                  _density.Tokens,
		"tokens_per_minute":       _density.TokensPerMinute,
		"tokens_per_request":      _density.TokensPerRequest,
		"prompt_tokens":           _density.PromptTokens,
		"quality_tier_avg":        _density.QualityTierAvg,
		"output_ratio":            _density.OutputRatio,
		"output_ratio_median":     _density.OutputRatioMedian,
		"prose_ratio":             _density.ProseRatio,
		"prose_ratio_median":      _density.ProseRatioMedian,
		"prose_samples":           _density.ProseSamples,
		"reasoning_tokens":        _density.ReasoningTokens,
		"reasoning_ratio":         _density.ReasoningRatio,
		"reasoning_samples":       _density.ReasoningSamples,
		"continuation_count":      _density.ContinuationCount,
		"continuation_ratio":      _density.ContinuationRatio,
		"fingerprinted_requests":  _density.FingerprintedCount,
		"repeated_requests":       _density.RepeatedTaskCount,
		"repeated_task_ratio":     _density.RepeatedTaskRatio,
		"tool_call_count":         _density.ToolCallCount,
		"tool_calls_per_request":  _density.ToolCallsPerRequest,
		"tool_round_count":        _density.ToolRoundCount,
		"tool_rounds_per_request": _density.ToolRoundsPerRequest,
		"tool_output_tokens":      _density.ToolOutputTokens,
		"yield_low":               _density.YieldLow,
		"yield_mid":               _density.YieldMid,
		"yield_high":              _density.YieldHigh,
		"low":                     _density.Low,
		"mid":                     _density.Mid,
		"high":                    _density.High,
		"first_at":                _density.FirstAt,
		"last_at":                 _density.LastAt,
		"truncated":               _density.Truncated,
	}
}

// -------------------------------------------------------------------------------------
// parseKeyDensityWindow 接受秒數或 Go duration（例如 300、5m、1h）。
func parseKeyDensityWindow(_value string) time.Duration {
	_value = strings.TrimSpace(_value)
	if _value == "" {
		return defaultKeyDensityWindow
	}
	_duration := time.Duration(0)
	if _seconds, _err := strconv.Atoi(_value); _err == nil {
		_duration = time.Duration(_seconds) * time.Second
	} else if _parsed, _err := time.ParseDuration(_value); _err == nil {
		_duration = _parsed
	}
	if _duration <= 0 {
		return defaultKeyDensityWindow
	}
	if _duration > maxKeyDensityWindow {
		return maxKeyDensityWindow
	}
	return _duration
}

// -------------------------------------------------------------------------------------
// 綁定數快照的重算間隔：每個請求都掃一次 route map 太浪費，
// 而上限本來就不需要即時精確。
const conversationBindingRefreshInterval = 2 * time.Second

// -------------------------------------------------------------------------------------
// refreshConversationBindings 把目前的對話綁定數與上限同步給 balancer，
// 讓「已達上限的 provider 不再接新對話」在選擇階段生效。
func (_h *HTTPAPI) refreshConversationBindings() {
	_h.restoreQuotaCooldowns()
	if _h == nil || _h.Balancer == nil || _h.Client == nil {
		return
	}
	_h.bindingRefreshLock.Lock()
	if time.Since(_h.bindingRefreshedAt) < conversationBindingRefreshInterval {
		_h.bindingRefreshLock.Unlock()
		return
	}
	_h.bindingRefreshedAt = time.Now()
	_h.bindingRefreshLock.Unlock()

	_limit := _h.currentAdvancedSettings().MaxBindingsPerProvider
	_h.Balancer.SetConversationBindings(_h.Client.PromptCacheRouteCounts(), _limit)
}

// -------------------------------------------------------------------------------------
// advancedSettingsWarning 回傳設定之間互相衝突的提醒（不阻擋儲存）。
// 綁定上限高於某個 Provider 的最大併發時，該 Provider 被釘住的對話容易撞到併發上限，
// 進而被判定為不可用而解除黏著、失去推理脈絡。因為每個 Provider 的併發各自設定，
// 這裡以「啟用中 Provider 的最小併發」為準。
func (_h *HTTPAPI) advancedSettingsWarning(_settings domain.AdvancedSettingsConfig) string {
	if _h == nil || _h.Balancer == nil || _settings.MaxBindingsPerProvider <= 0 {
		return ""
	}

	_tightestName := ""
	_tightest := 0
	for _, _provider := range _h.Balancer.ConfigSnapshot().Providers {
		if !_provider.Enabled || _provider.MaxConcurrent <= 0 {
			continue
		}
		if _tightest == 0 || int(_provider.MaxConcurrent) < _tightest {
			_tightest = int(_provider.MaxConcurrent)
			_tightestName = defaultString(_provider.Name, _provider.ID)
		}
	}
	if _tightest == 0 || _settings.MaxBindingsPerProvider <= _tightest {
		return ""
	}
	return fmt.Sprintf(
		"綁定上限 %d 高於 Provider「%s」的最大併發 %d：該 Provider 被釘住的對話容易撞到併發上限而被迫降級（失去推理脈絡）。建議調降綁定上限，或提高該 Provider 的最大併發。",
		_settings.MaxBindingsPerProvider, _tightestName, _tightest,
	)
}
