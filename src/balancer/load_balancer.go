package balancer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"LoadBalanceProvider/src/analyzer"
	"LoadBalanceProvider/src/balancer/strategy"
	"LoadBalanceProvider/src/classifier"
	"LoadBalanceProvider/src/config"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/providerusage"
)

const minProviderUsageRemainingPercent = 5.0

// -------------------------------------------------------------------------------------
type LoadBalancer struct {
	_lock     sync.RWMutex
	Config    *domain.ProxyConfig
	Analyzer  *analyzer.RequestAnalyzer
	Cache     *classifier.Cache
	Providers []*ProviderRuntime

	// 對話綁定數快照。實際資料在 proxy 層（proxy 依賴 balancer，不能反向匯入），
	// 因此由 API 層在選擇前寫入。
	_bindingLock  sync.RWMutex
	_bindings     map[string]int
	_bindingLimit int
}

// -------------------------------------------------------------------------------------
type ProviderRuntime struct {
	modelCooldownLock        sync.Mutex
	modelCooldowns           map[string]time.Time
	Config                   *domain.LLMProviderConfig
	Active                   int64
	Successes                int64
	Failures                 int64
	ConsecutiveFailures      int64
	overloadLock             sync.Mutex
	consecutiveOverloads     int
	overloadUntil            time.Time
	CircuitOpenUntil         int64
	CapacityUnavailableUntil int64
	QuotaUnavailableUntil    int64
	_latencyLock             sync.Mutex
	LatencyEWMA50MS          float64
	LatencyEWMA95MS          float64
	LastDurationMS           float64
	ReactionEWMA             float64
	LastReactionMS           float64
	TokenSpeedEWMA           float64
	LastTokenSpeed           float64
	ClientDeliveryEWMA       float64
	LastClientDeliveryTPS    float64
	ProviderReportedTPSEWMA  float64
	LastProviderReportedTPS  float64
	LastCompletionTokens     int64
	TotalCompletionTokens    int64
	_usageLock               sync.Mutex
	Usage                    ProviderUsageSnapshot
	LastUsageProbeAt         int64
	AuthError                ProviderAuthErrorState
}

// -------------------------------------------------------------------------------------
type ProviderUsageSnapshot struct {
	UpdatedAt                   time.Time         `json:"updated_at,omitempty"`
	LimitRequests               string            `json:"limit_requests,omitempty"`
	RemainingRequests           string            `json:"remaining_requests,omitempty"`
	ResetRequests               string            `json:"reset_requests,omitempty"`
	LimitTokens                 string            `json:"limit_tokens,omitempty"`
	RemainingTokens             string            `json:"remaining_tokens,omitempty"`
	ResetTokens                 string            `json:"reset_tokens,omitempty"`
	RequestUsagePercent         float64           `json:"request_usage_percent,omitempty"`
	RequestRemainingPercent     float64           `json:"request_remaining_percent,omitempty"`
	TokenUsagePercent           float64           `json:"token_usage_percent,omitempty"`
	TokenRemainingPercent       float64           `json:"token_remaining_percent,omitempty"`
	CodexPrimaryUsedPercent     float64           `json:"codex_primary_used_percent,omitempty"`
	CodexPrimaryRemainPercent   float64           `json:"codex_primary_remaining_percent,omitempty"`
	CodexSecondaryUsedPercent   float64           `json:"codex_secondary_used_percent,omitempty"`
	CodexSecondaryRemainPercent float64           `json:"codex_secondary_remaining_percent,omitempty"`
	Headers                     map[string]string `json:"headers,omitempty"`
}

// -------------------------------------------------------------------------------------
type ProviderAuthErrorState struct {
	Active    bool      `json:"active"`
	Message   string    `json:"message,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// -------------------------------------------------------------------------------------
type ProviderSelection struct {
	Provider *ProviderRuntime
	Model    *domain.LLMModelConfig
	Score    float64
}

// -------------------------------------------------------------------------------------
type SelectionMeta struct {
	Strategy           string                   `json:"strategy"`
	Reason             string                   `json:"reason"`
	CandidateCount     int                      `json:"candidate_count"`
	RequestedModel     string                   `json:"requested_model"`
	SelectedProviderID string                   `json:"selected_provider_id"`
	SelectedProvider   string                   `json:"selected_provider"`
	SelectedModel      string                   `json:"selected_model"`
	RequestProfile     domain.RequestProfile    `json:"request_profile"`
	Candidates         []SelectionCandidateMeta `json:"candidates"`
}

// -------------------------------------------------------------------------------------
type NoAvailableProviderError struct {
	TemporaryOverload bool
	RetryAfter        time.Duration
}

// -------------------------------------------------------------------------------------
func (_e *NoAvailableProviderError) Error() string {
	if _e != nil && _e.TemporaryOverload {
		return "model providers are temporarily overloaded; please retry later"
	}
	return "no available provider can handle this request"
}

// -------------------------------------------------------------------------------------
func SelectionRetryAfterSeconds(_err error) int {
	var _selectionErr *NoAvailableProviderError
	if !errors.As(_err, &_selectionErr) || _selectionErr == nil || !_selectionErr.TemporaryOverload || _selectionErr.RetryAfter <= 0 {
		return 0
	}
	_seconds := int(math.Ceil(_selectionErr.RetryAfter.Seconds()))
	if _seconds < 1 {
		return 1
	}
	return _seconds
}

// -------------------------------------------------------------------------------------
type SelectionCandidateMeta struct {
	ProviderID    string  `json:"provider_id"`
	ProviderName  string  `json:"provider_name"`
	Model         string  `json:"model"`
	Score         float64 `json:"score"`
	Active        int64   `json:"active"`
	MaxConcurrent int64   `json:"max_concurrent"`
	UsagePercent  float64 `json:"usage_percent,omitempty"`
	RemainPercent float64 `json:"remaining_percent,omitempty"`
	LatencyP50MS  float64 `json:"latency_p50_ms"`
	LatencyP95MS  float64 `json:"latency_p95_ms"`
	CircuitOpen   bool    `json:"circuit_open"`
}

// -------------------------------------------------------------------------------------
func NewLoadBalancer(_config *domain.ProxyConfig) *LoadBalancer {
	if _config == nil {
		_config = config.DefaultProxyConfig()
	}

	config.ApplyDefaults(_config)

	_providers := make([]*ProviderRuntime, 0, len(_config.Providers))
	for _idx := range _config.Providers {
		_providers = append(_providers, &ProviderRuntime{Config: &_config.Providers[_idx]})
	}

	return &LoadBalancer{
		Config:    _config,
		Analyzer:  analyzer.New(),
		Cache:     classifier.NewCache(1000),
		Providers: _providers,
	}
}

// -------------------------------------------------------------------------------------
// SetConversationBindings 更新各 provider 目前的對話綁定數與上限。
// _limit <= 0 代表不啟用上限。
func (_b *LoadBalancer) SetConversationBindings(_counts map[string]int, _limit int) {
	if _b == nil {
		return
	}
	_b._bindingLock.Lock()
	defer _b._bindingLock.Unlock()
	_b._bindings = _counts
	_b._bindingLimit = _limit
}

// -------------------------------------------------------------------------------------
// providerAtBindingCap 回報該 provider 是否已達對話綁定上限。
func (_b *LoadBalancer) providerAtBindingCap(_providerID string) bool {
	if _b == nil {
		return false
	}
	_b._bindingLock.RLock()
	defer _b._bindingLock.RUnlock()
	if _b._bindingLimit <= 0 || len(_b._bindings) == 0 {
		return false
	}
	return _b._bindings[_providerID] >= _b._bindingLimit
}

// -------------------------------------------------------------------------------------
func (_b *LoadBalancer) ReloadConfig(_config *domain.ProxyConfig) {
	if _config == nil {
		_config = config.DefaultProxyConfig()
	}

	config.ApplyDefaults(_config)

	_b._lock.Lock()
	defer _b._lock.Unlock()

	_oldStats := map[string]*ProviderRuntime{}
	for _, _provider := range _b.Providers {
		_oldStats[_provider.Config.ID] = _provider
	}

	_providers := make([]*ProviderRuntime, 0, len(_config.Providers))
	for _idx := range _config.Providers {
		_runtime := &ProviderRuntime{Config: &_config.Providers[_idx]}
		if _old := _oldStats[_runtime.Config.ID]; _old != nil {
			_runtime.copyRuntimeState(_old)
		}
		_providers = append(_providers, _runtime)
	}

	_b.Config = _config
	_b.Providers = _providers
}

// -------------------------------------------------------------------------------------
func (_b *LoadBalancer) ConfigSnapshot() domain.ProxyConfig {
	_b._lock.RLock()
	defer _b._lock.RUnlock()

	_bytes, _err := json.Marshal(_b.Config)
	if _err != nil {
		return domain.ProxyConfig{}
	}

	var _snapshot domain.ProxyConfig
	_ = json.Unmarshal(_bytes, &_snapshot)
	return _snapshot
}

// -------------------------------------------------------------------------------------
func (_b *LoadBalancer) ProvidersSnapshot() []*ProviderRuntime {
	if _b == nil {
		return nil
	}
	_b._lock.RLock()
	defer _b._lock.RUnlock()

	_snapshot := make([]*ProviderRuntime, 0, len(_b.Providers))
	_snapshot = append(_snapshot, _b.Providers...)
	return _snapshot
}

// -------------------------------------------------------------------------------------
func (_b *LoadBalancer) Select(_req *domain.ChatCompletionRequest) (*ProviderRuntime, *domain.LLMModelConfig, domain.RequestProfile, SelectionMeta, error) {
	return _b.SelectExcluding(_req, nil)
}

// -------------------------------------------------------------------------------------
func (_b *LoadBalancer) SelectExcluding(_req *domain.ChatCompletionRequest, _excludedProviderIDs []string) (*ProviderRuntime, *domain.LLMModelConfig, domain.RequestProfile, SelectionMeta, error) {
	_profile := _b.classifyRequest(_req)
	_excluded := map[string]bool{}
	for _, _providerID := range _excludedProviderIDs {
		_providerID = strings.ToLower(strings.TrimSpace(_providerID))
		if _providerID != "" {
			_excluded[_providerID] = true
		}
	}

	_b._lock.RLock()
	defer _b._lock.RUnlock()

	_requestedModel, _modelFallbackReason := _b.effectiveRequestedModel(_req)
	_candidates, _fallbackReason := _b.collectCandidates(_req, _profile, _requestedModel, _excluded)

	if len(_candidates) == 0 {
		_selectionErr := _b.noAvailableProviderError(_req, _profile, _requestedModel, _excluded)
		return nil, nil, _profile, SelectionMeta{
			Strategy:       _b.selectionStrategy(),
			Reason:         _selectionErr.Error(),
			CandidateCount: 0,
			RequestedModel: _requestedModel,
			RequestProfile: _profile,
		}, _selectionErr
	}
	_selected, _meta := _b.selectWithStrategy(_candidates, _profile, _requestedModel)
	if _modelFallbackReason != "" {
		_meta.Reason = _modelFallbackReason + "; " + _meta.Reason
	}
	// 退讓過的選擇一定要留下痕跡：它代表某條規則被放寬了，
	// 而使用者看到的現象（請求落在見底的 provider 上）只有從這裡才追得出來。
	if _fallbackReason != "" {
		_meta.Reason = _fallbackReason + "; " + _meta.Reason
		log.Printf(
			"provider selection fell back: provider=%s model=%s pinned=%t reason=%s",
			_selected.Provider.Config.ID, _selected.Model.Name,
			strings.TrimSpace(_req.ProviderID) != "" || strings.TrimSpace(_req.Provider) != "",
			_fallbackReason,
		)
	}
	return _selected.Provider, _selected.Model, _profile, _meta, nil
}

// -------------------------------------------------------------------------------------
func (_b *LoadBalancer) noAvailableProviderError(_req *domain.ChatCompletionRequest, _profile domain.RequestProfile, _requestedModel string, _excludedProviderIDs map[string]bool) error {
	_now := time.Now()
	_matchingProviders := 0
	_capacityUnavailableProviders := 0
	_earliestRetry := time.Time{}

	for _, _provider := range _b.Providers {
		if _provider == nil || _provider.Config == nil {
			continue
		}
		if _excludedProviderIDs[strings.ToLower(strings.TrimSpace(_provider.Config.ID))] ||
			!_provider.Config.Enabled ||
			!providerMatchesRequest(_provider.Config, _req) ||
			strings.EqualFold(_provider.Config.Role, "classifier") ||
			strings.TrimSpace(_provider.Config.BaseURL) == "" ||
			!providerHasEligibleModel(_provider.Config, _profile, _requestedModel) {
			continue
		}

		if _provider.HasAuthError() || _provider.CircuitOpen(_now) {
			continue
		}
		for _, _model := range candidateModels(_provider.Config, _requestedModel) {
			if !modelMatchesSelectionConstraints(&_model, _profile, _requestedModel) {
				continue
			}
			_matchingProviders++
			_until := _provider.CooldownUntil(_model.Name)
			if _accountUntil := time.Unix(0, atomic.LoadInt64(&_provider.CapacityUnavailableUntil)); _accountUntil.After(_until) {
				_until = _accountUntil
			}
			// 併發滿載短暫等待後重查，仍受請求等待預算限制。
			if !_until.After(_now) && _provider.Config.MaxConcurrent > 0 && atomic.LoadInt64(&_provider.Active) >= _provider.Config.MaxConcurrent {
				_until = _now.Add(time.Second)
			}
			if !_until.After(_now) {
				continue
			}
			_capacityUnavailableProviders++
			if _earliestRetry.IsZero() || _until.Before(_earliestRetry) {
				_earliestRetry = _until
			}
		}
	}

	if _matchingProviders > 0 && _capacityUnavailableProviders == _matchingProviders {
		_retryAfter := time.Second
		if !_earliestRetry.IsZero() && _earliestRetry.After(_now) {
			_retryAfter = _earliestRetry.Sub(_now)
		}
		return &NoAvailableProviderError{TemporaryOverload: true, RetryAfter: _retryAfter}
	}
	return &NoAvailableProviderError{}
}

// -------------------------------------------------------------------------------------
func (_b *LoadBalancer) classifyRequest(_req *domain.ChatCompletionRequest) domain.RequestProfile {
	if _b.Cache == nil {
		_b.Cache = classifier.NewCache(1000)
	}

	_cacheKey := classifier.RequestCacheKey(_req)
	if _profile, _ok := _b.Cache.Get(_cacheKey); _ok {
		return _profile
	}

	_analyzer := _b.Analyzer
	if _analyzer == nil {
		_analyzer = analyzer.New()
	}

	_profile := _analyzer.Analyze(_req)
	if !classifier.ShouldUseLLM(_profile) {
		_b.Cache.Set(_cacheKey, _profile)
		return _profile
	}

	_provider, _ok := _b.classifierProviderConfig()
	if !_ok {
		_b.Cache.Set(_cacheKey, _profile)
		return _profile
	}

	_ctx, _cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer _cancel()

	_llmClassifier := classifier.NewLLM(_provider)
	if _classified, _used, _err := _llmClassifier.Classify(_ctx, _req, _profile); _err == nil && _used {
		_profile = _classified
	}

	_b.Cache.Set(_cacheKey, _profile)
	return _profile
}

// -------------------------------------------------------------------------------------
func (_b *LoadBalancer) classifierProviderConfig() (domain.LLMProviderConfig, bool) {
	_b._lock.RLock()
	defer _b._lock.RUnlock()

	for _, _provider := range _b.Providers {
		if _provider == nil || _provider.Config == nil || !_provider.Config.Enabled {
			continue
		}
		if strings.EqualFold(_provider.Config.Role, "classifier") {
			return *_provider.Config, true
		}
	}

	return domain.LLMProviderConfig{}, false
}

// -------------------------------------------------------------------------------------
// collectCandidates 回傳候選清單，以及「動用了哪一層退讓」。
// 退讓原因必須回報出來：正常情況下永遠是空字串，一旦不是，就代表這次選擇
// 是在放寬某條規則之後才成立的 —— 沒有這個訊息，事後完全無法分辨
// 「請求為什麼落在一個不該被選中的 provider 上」。
func (_b *LoadBalancer) collectCandidates(_req *domain.ChatCompletionRequest, _profile domain.RequestProfile, _requestedModel string, _excludedProviderIDs map[string]bool) ([]ProviderSelection, string) {
	_candidates := make([]ProviderSelection, 0)
	// 已達對話綁定上限的 provider 先放旁邊：只有在完全沒有其他候選時才動用，
	// 避免上限把請求逼成「沒有可用 provider」。
	_atCap := make([]ProviderSelection, 0)
	// 超過等級上限的候選同樣先放旁邊：降級是政策，不該讓請求變成無 provider 可用。
	// 沒有任何符合上限的模型時，寧可放行到較高等級，也不要回 503。
	_overTier := make([]ProviderSelection, 0)
	// 可用量見底的 provider 是最後手段。硬排除會在多數帳號同時見底時
	// 把流量全擠到少數幾個上，併發一滿就選不到 provider，使用者看到的是斷線。
	// 評分本來就會把低可用量的 provider 排到最後（usageScoreAdjustment），
	// 所以留著當候選不會影響正常情況下的選擇。
	_exhausted := make([]ProviderSelection, 0)
	// 明確指定 provider 的請求（對話黏著或金鑰強制路由）不受上限影響，
	// 否則既有對話會直接失去唯一的候選。
	_pinned := strings.TrimSpace(_req.ProviderID) != "" || strings.TrimSpace(_req.Provider) != ""
	_now := time.Now()
	for _, _provider := range _b.Providers {
		if _provider == nil || _provider.Config == nil {
			continue
		}
		if _excludedProviderIDs[strings.ToLower(strings.TrimSpace(_provider.Config.ID))] {
			continue
		}
		if !_provider.Config.Enabled {
			continue
		}
		if !providerMatchesRequest(_provider.Config, _req) {
			continue
		}
		if strings.EqualFold(_provider.Config.Role, "classifier") {
			continue
		}
		if strings.TrimSpace(_provider.Config.BaseURL) == "" {
			continue
		}
		if _provider.CircuitOpen(_now) {
			continue
		}
		if _provider.CapacityUnavailable(_now) {
			continue
		}
		if _provider.HasAuthError() {
			continue
		}

		if _provider.Config.MaxConcurrent > 0 && atomic.LoadInt64(&_provider.Active) >= _provider.Config.MaxConcurrent {
			continue
		}

		// 可用量見底不再直接跳過，改為降級成最後手段。
		_exhaustedUsage := _provider.UsageSnapshot().ExhaustedForSelection()

		_models := candidateModels(_provider.Config, _requestedModel)
		for _modelIdx := range _models {
			_model := &_models[_modelIdx]
			if _provider.ModelUnavailableUntil(_model.Name).After(_now) {
				continue
			}
			if !modelMatchesSelectionConstraints(_model, _profile, _requestedModel) {
				continue
			}

			_score := _b.scoreCandidate(_provider, _model, _profile, _requestedModel)
			_selection := ProviderSelection{
				Provider: _provider,
				Model:    _model,
				Score:    _score,
			}
			// 等級上限先於綁定上限判斷，_atCap 裡才不會混進超過上限的模型。
			if _req.MaxQualityTier > 0 && _model.QualityTier > _req.MaxQualityTier {
				_overTier = append(_overTier, _selection)
				continue
			}
			if _exhaustedUsage {
				_exhausted = append(_exhausted, _selection)
				continue
			}
			if !_pinned && _b.providerAtBindingCap(_provider.Config.ID) {
				_atCap = append(_atCap, _selection)
				continue
			}
			_candidates = append(_candidates, _selection)
		}
	}

	// 退讓順序由「後果嚴重度」決定：綁定上限只是分散策略，放寬沒有代價；
	// 等級上限放寬只是多花配額，請求仍會成功；可用量見底最可能直接被上游拒絕，
	// 所以擺在最後。
	if len(_candidates) == 0 {
		if len(_atCap) > 0 {
			return _atCap, "fallback: all candidates at binding cap"
		}
		if len(_overTier) > 0 {
			return _overTier, "fallback: no model within the quality tier cap"
		}
		if len(_exhausted) > 0 {
			return _exhausted, "fallback: only providers with exhausted quota remain"
		}
		return nil, ""
	}
	return _candidates, ""
}

// -------------------------------------------------------------------------------------
func providerHasEligibleModel(_provider *domain.LLMProviderConfig, _profile domain.RequestProfile, _requestedModel string) bool {
	_models := candidateModels(_provider, _requestedModel)
	for _idx := range _models {
		if modelMatchesSelectionConstraints(&_models[_idx], _profile, _requestedModel) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func modelMatchesSelectionConstraints(_model *domain.LLMModelConfig, _profile domain.RequestProfile, _requestedModel string) bool {
	if _model == nil {
		return false
	}
	if !_model.MatchName(_requestedModel) && _requestedModel != "" && !strings.EqualFold(_requestedModel, "auto") {
		return false
	}
	if !modelSatisfiesHardRequirements(_model, _profile.HardRequirements) {
		return false
	}
	return _profile.EstimatedInputTokens <= _model.MaxInputTokens && _profile.RequestedOutputTokens <= _model.MaxOutputTokens
}

// -------------------------------------------------------------------------------------
func (_b *LoadBalancer) effectiveRequestedModel(_req *domain.ChatCompletionRequest) (string, string) {
	if _req == nil {
		return "", ""
	}
	return strings.TrimSpace(_req.Model), ""
}

// -------------------------------------------------------------------------------------
func candidateModels(_provider *domain.LLMProviderConfig, _requestedModel string) []domain.LLMModelConfig {
	if _provider == nil {
		return []domain.LLMModelConfig{}
	}

	_models := append([]domain.LLMModelConfig(nil), _provider.Models...)
	if requestedModelIsExplicit(_requestedModel) {
		for _idx := range _models {
			if !_models[_idx].MatchName(_requestedModel) {
				continue
			}
			_passthroughModel := _models[_idx]
			_passthroughModel.Name = strings.TrimSpace(_requestedModel)
			_passthroughModel.Aliases = append([]string{_models[_idx].Name}, _passthroughModel.Aliases...)
			_passthroughModel.QualityTier = explicitModelQualityTier(_provider, _passthroughModel.Name, _passthroughModel.QualityTier)
			return []domain.LLMModelConfig{_passthroughModel}
		}
		if !providerAllowsExplicitModelPassthrough(_provider) {
			return []domain.LLMModelConfig{}
		}

		_passthroughModel := domain.LLMModelConfig{
			Name:            strings.TrimSpace(_requestedModel),
			Aliases:         []string{strings.TrimSpace(_requestedModel)},
			MaxInputTokens:  1048576,
			MaxOutputTokens: 262144,
			Capabilities:    []string{"chat", "responses", "reasoning", "summarization", "extraction", "translation", "json", "json_mode", "tools", "function_calling", "long_context"},
			CostTier:        2,
			QualityTier:     3,
		}
		if len(_models) > 0 {
			_passthroughModel = _models[0]
			_passthroughModel.Name = strings.TrimSpace(_requestedModel)
			_passthroughModel.Aliases = append([]string{strings.TrimSpace(_requestedModel)}, _passthroughModel.Aliases...)
		}
		_passthroughModel.QualityTier = explicitModelQualityTier(_provider, _passthroughModel.Name, _passthroughModel.QualityTier)
		return []domain.LLMModelConfig{_passthroughModel}
	}

	return _models
}

// -------------------------------------------------------------------------------------
func providerAllowsExplicitModelPassthrough(_provider *domain.LLMProviderConfig) bool {
	if _provider == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(_provider.Kind), "openai-codex") ||
		strings.EqualFold(strings.TrimSpace(_provider.Type), "openai-codex")
}

// -------------------------------------------------------------------------------------
// explicitModelQualityTier 讓 Codex 的實際模型變體決定監看等級。
// Provider 只保存一個主模型，其他 API 模型是 aliases；若直接沿用主模型等級，
// Luna／Terra 這類較低階變體就會被誤記成 Sol 的大模型等級。
func explicitModelQualityTier(_provider *domain.LLMProviderConfig, _modelName string, _fallback int) int {
	if !providerAllowsExplicitModelPassthrough(_provider) {
		return _fallback
	}

	_normalized := strings.ToLower(strings.TrimSpace(_modelName))
	_normalized = strings.NewReplacer("_", "-", " ", "-").Replace(_normalized)
	switch {
	case strings.HasSuffix(_normalized, "-sol"):
		return 8
	case strings.HasSuffix(_normalized, "-terra"):
		return 6
	case strings.HasSuffix(_normalized, "-luna"):
		return 4
	default:
		return _fallback
	}
}

// -------------------------------------------------------------------------------------
func requestedModelIsExplicit(_model string) bool {
	return strings.TrimSpace(_model) != "" && !strings.EqualFold(strings.TrimSpace(_model), "auto")
}

// -------------------------------------------------------------------------------------
func providerMatchesRequest(_provider *domain.LLMProviderConfig, _req *domain.ChatCompletionRequest) bool {
	if _provider == nil || _req == nil {
		return false
	}

	_requestedProvider := strings.TrimSpace(_req.ProviderID)
	if _requestedProvider == "" {
		_requestedProvider = strings.TrimSpace(_req.Provider)
	}
	if _requestedProvider == "" || strings.EqualFold(_requestedProvider, "auto") {
		return true
	}

	return strings.EqualFold(_provider.ID, _requestedProvider) ||
		strings.EqualFold(_provider.Name, _requestedProvider) ||
		strings.EqualFold(_provider.Kind, _requestedProvider)
}

// -------------------------------------------------------------------------------------
func (_b *LoadBalancer) selectWithStrategy(_candidates []ProviderSelection, _profile domain.RequestProfile, _requestedModel string) (ProviderSelection, SelectionMeta) {
	_strategy := strategy.Resolve(_b.selectionStrategy())
	_strategyCandidates := strategyCandidateMeta(_candidates)
	_selectedRef, _reason := _strategy.Select(_strategyCandidates, _profile, _requestedModel)
	_selected := _candidates[0]
	if _selectedRef.Index >= 0 && _selectedRef.Index < len(_candidates) {
		_selected = _candidates[_selectedRef.Index]
	}

	return _selected, SelectionMeta{
		Strategy:           _strategy.Name(),
		Reason:             _reason,
		CandidateCount:     len(_candidates),
		RequestedModel:     _requestedModel,
		SelectedProviderID: _selected.Provider.Config.ID,
		SelectedProvider:   _selected.Provider.Config.Name,
		SelectedModel:      _selected.Model.Name,
		RequestProfile:     _profile,
		Candidates:         selectionCandidateMeta(_candidates),
	}
}

// -------------------------------------------------------------------------------------
func (_b *LoadBalancer) selectionStrategy() string {
	if _b.Config == nil || strings.TrimSpace(_b.Config.SelectionStrategy) == "" {
		return "random"
	}
	return strings.ToLower(strings.TrimSpace(_b.Config.SelectionStrategy))
}

// -------------------------------------------------------------------------------------
func modelSatisfiesHardRequirements(_model *domain.LLMModelConfig, _requirements []string) bool {
	for _, _requirement := range _requirements {
		switch strings.ToLower(strings.TrimSpace(_requirement)) {
		case "":
			continue
		case "vision":
			if !_model.HasCapability("vision") && !_model.HasCapability("image_analysis") {
				return false
			}
		case "image_generation":
			if !_model.HasCapability("image_generation") {
				return false
			}
		case "image_edit":
			if !_model.HasCapability("image_edit") && !_model.HasCapability("image_generation") {
				return false
			}
		case "image_variation":
			if !_model.HasCapability("image_variation") && !_model.HasCapability("image_generation") {
				return false
			}
		case "audio_analysis":
			if !_model.HasCapability("audio_analysis") && !_model.HasCapability("transcription") {
				return false
			}
		case "transcription":
			if !_model.HasCapability("transcription") && !_model.HasCapability("audio_analysis") {
				return false
			}
		case "audio_translation":
			if !_model.HasCapability("audio_translation") && !_model.HasCapability("transcription") {
				return false
			}
		case "audio_generation", "tts":
			if !_model.HasCapability("audio_generation") && !_model.HasCapability("tts") {
				return false
			}
		case "video_analysis":
			if !_model.HasCapability("video_analysis") {
				return false
			}
		case "video_generation":
			if !_model.HasCapability("video_generation") {
				return false
			}
		case "responses":
			if !_model.HasCapability("responses") {
				return false
			}
		case "tools":
			if !_model.HasCapability("tools") && !_model.HasCapability("function_calling") {
				return false
			}
		case "json_mode":
			if !_model.HasCapability("json_mode") && !_model.HasCapability("json") {
				return false
			}
		case "long_context":
			if _model.MaxInputTokens <= 32000 {
				return false
			}
		}
	}
	return true
}

// -------------------------------------------------------------------------------------
func selectionCandidateMeta(_candidates []ProviderSelection) []SelectionCandidateMeta {
	_meta := make([]SelectionCandidateMeta, 0, len(_candidates))
	for _, _candidate := range _candidates {
		_latencyP50MS, _latencyP95MS := _candidate.Provider.LatencySnapshot()
		_usage := _candidate.Provider.UsageSnapshot()
		_meta = append(_meta, SelectionCandidateMeta{
			ProviderID:    _candidate.Provider.Config.ID,
			ProviderName:  _candidate.Provider.Config.Name,
			Model:         _candidate.Model.Name,
			Score:         _candidate.Score,
			Active:        atomic.LoadInt64(&_candidate.Provider.Active),
			MaxConcurrent: _candidate.Provider.Config.MaxConcurrent,
			UsagePercent:  _usage.OverallUsagePercent(),
			RemainPercent: _usage.OverallRemainingPercent(),
			LatencyP50MS:  _latencyP50MS,
			LatencyP95MS:  _latencyP95MS,
			CircuitOpen:   _candidate.Provider.CircuitOpen(time.Now()),
		})
	}
	return _meta
}

// -------------------------------------------------------------------------------------
func strategyCandidateMeta(_candidates []ProviderSelection) []strategy.ProviderSelection {
	_meta := make([]strategy.ProviderSelection, 0, len(_candidates))
	for _idx, _candidate := range _candidates {
		_meta = append(_meta, strategy.ProviderSelection{
			Index:           _idx,
			ProviderID:      _candidate.Provider.Config.ID,
			ProviderName:    _candidate.Provider.Config.Name,
			ProviderKind:    _candidate.Provider.Config.Kind,
			ProviderPurpose: _candidate.Provider.Config.Purpose,
			Model:           _candidate.Model.Name,
			Score:           _candidate.Score,
			Active:          atomic.LoadInt64(&_candidate.Provider.Active),
			MaxConcurrent:   _candidate.Provider.Config.MaxConcurrent,
		})
	}
	return _meta
}

// -------------------------------------------------------------------------------------
func (_b *LoadBalancer) scoreCandidate(_provider *ProviderRuntime, _model *domain.LLMModelConfig, _profile domain.RequestProfile, _requestedModel string) float64 {
	_active := float64(atomic.LoadInt64(&_provider.Active))
	_capacity := math.Max(float64(_provider.Config.MaxConcurrent), 1)
	_loadPenalty := (_active / _capacity) * 35
	_, _latencyP95MS := _provider.LatencySnapshot()
	_latencyPenalty := math.Min((_latencyP95MS/1000)*6, 30)
	_failurePenalty := math.Min(float64(atomic.LoadInt64(&_provider.ConsecutiveFailures))*10, 30)
	_successPenalty := math.Min(math.Sqrt(float64(atomic.LoadInt64(&_provider.Successes)))*1.2, 30)
	_usageAdjustment := usageScoreAdjustment(_provider.UsageSnapshot())

	_score := float64(_provider.Config.Weight*10) +
		float64(_model.QualityTier*12) -
		float64(_model.CostTier*5) -
		float64(_provider.Config.Priority*3) -
		_loadPenalty -
		_latencyPenalty -
		_failurePenalty -
		_successPenalty +
		_usageAdjustment

	if _model.HasCapability(_profile.TaskType) {
		_score += 25
	}

	if purposeMatchesTask(_provider.Config.Purpose, _profile.TaskType) {
		_score += 15
	}

	if _model.MatchName(_requestedModel) && _requestedModel != "" && !strings.EqualFold(_requestedModel, "auto") {
		_score += 20
	}

	_tokenNeed := _profile.EstimatedInputTokens + _profile.RequestedOutputTokens
	_tokenLimit := _model.MaxInputTokens + _model.MaxOutputTokens
	if _tokenLimit > 0 {
		_utilization := float64(_tokenNeed) / float64(_tokenLimit)
		switch {
		case _utilization < 0.35 && _profile.ComplexityScore <= 4:
			_score += 8
		case _utilization > 0.75:
			_score -= 12
		}
	}

	if _profile.ComplexityScore >= 7 {
		_score += float64(_model.QualityTier * 4)
		_score -= float64(_model.CostTier)
	}

	return _score
}

// -------------------------------------------------------------------------------------
func purposeMatchesTask(_purpose string, _taskType string) bool {
	_purpose = strings.TrimSpace(_purpose)
	_taskType = strings.TrimSpace(_taskType)
	if _purpose == "" || _taskType == "" {
		return false
	}

	_taskMap := map[string][]string{
		"對話":   {"chat", "reasoning", "creative"},
		"文件轉換": {"summarization", "extraction", "translation"},
		"影像分析": {"vision", "image_analysis", "extraction"},
		"影像生成": {"image_generation", "image_edit", "image_variation"},
		"影片分析": {"video_analysis", "extraction"},
		"影片生成": {"video_generation"},
		"聲音分析": {"audio_analysis", "transcription", "audio_translation", "extraction"},
		"聲音生成": {"audio_generation", "tts"},
	}

	for _, _task := range _taskMap[_purpose] {
		if strings.EqualFold(_task, _taskType) {
			return true
		}
	}

	return false
}

// -------------------------------------------------------------------------------------
func (_b *LoadBalancer) ProviderStatus() []map[string]interface{} {
	_b._lock.RLock()
	defer _b._lock.RUnlock()
	return _b.providerStatusLocked()
}

// DashboardSnapshot 在同一版本的設定下讀取狀態，避免複製完整模型 catalog 與金鑰設定。
func (_b *LoadBalancer) DashboardSnapshot() ([]domain.LLMProviderConfig, []map[string]interface{}) {
	_b._lock.RLock()
	defer _b._lock.RUnlock()
	_configs := make([]domain.LLMProviderConfig, 0, len(_b.Providers))
	for _, _provider := range _b.Providers {
		_source := _provider.Config
		_config := domain.LLMProviderConfig{
			ID: _source.ID, Name: _source.Name, Kind: _source.Kind,
			Enabled: _source.Enabled, Priority: _source.Priority,
			BaseURL: _source.BaseURL, ChatCompletionsPath: _source.ChatCompletionsPath,
		}
		if len(_source.Models) > 0 {
			_config.Models = []domain.LLMModelConfig{{Name: _source.Models[0].Name}}
		}
		_configs = append(_configs, _config)
	}
	_status := _b.providerStatusLocked()
	for _, _item := range _status {
		delete(_item, "models")
	}
	return _configs, _status
}

func (_b *LoadBalancer) providerStatusLocked() []map[string]interface{} {
	_status := make([]map[string]interface{}, 0, len(_b.Providers))
	for _, _provider := range _b.Providers {
		_latencyP50MS, _latencyP95MS, _lastDurationMS, _reactionMS, _lastReactionMS, _tokenSpeed, _lastTokenSpeed, _lastCompletionTokens := _provider.MetricsSnapshot()
		_clientDeliveryTPS, _lastClientDeliveryTPS := _provider.ClientDeliverySnapshot()
		_providerReportedTPS, _lastProviderReportedTPS := _provider.ProviderReportedTPSSnapshot()
		_usage := _provider.UsageSnapshot()
		_authError := _provider.AuthErrorSnapshot()
		_status = append(_status, map[string]interface{}{
			"id":                                _provider.Config.ID,
			"name":                              _provider.Config.Name,
			"role":                              _provider.Config.Role,
			"enabled":                           _provider.Config.Enabled,
			"active":                            atomic.LoadInt64(&_provider.Active),
			"active_requests":                   atomic.LoadInt64(&_provider.Active),
			"max_concurrent":                    _provider.Config.MaxConcurrent,
			"successes":                         atomic.LoadInt64(&_provider.Successes),
			"failures":                          atomic.LoadInt64(&_provider.Failures),
			"consecutive_failures":              atomic.LoadInt64(&_provider.ConsecutiveFailures),
			"circuit_open":                      _provider.CircuitOpen(time.Now()),
			"circuit_open_until":                atomic.LoadInt64(&_provider.CircuitOpenUntil),
			"latency_p50_ms":                    _latencyP50MS,
			"latency_p95_ms":                    _latencyP95MS,
			"reaction_time_ms":                  _reactionMS,
			"first_token_ms":                    _lastReactionMS,
			"last_reaction_time_ms":             _lastReactionMS,
			"last_duration_ms":                  _lastDurationMS,
			"processing_time_ms":                _lastDurationMS,
			"token_generation_speed":            _tokenSpeed,
			"generation_tokens_per_second":      _tokenSpeed,
			"tokens_per_second":                 _tokenSpeed,
			"output_tokens_per_second":          _tokenSpeed,
			"last_token_generation_speed":       _lastTokenSpeed,
			"provider_generation_tps":           _tokenSpeed,
			"client_delivery_tps":               _clientDeliveryTPS,
			"last_client_delivery_tps":          _lastClientDeliveryTPS,
			"provider_reported_generation_tps":  _providerReportedTPS,
			"last_provider_reported_tps":        _lastProviderReportedTPS,
			"last_completion_tokens":            _lastCompletionTokens,
			"total_completion_tokens":           atomic.LoadInt64(&_provider.TotalCompletionTokens),
			"cumulative_completion_tokens":      atomic.LoadInt64(&_provider.TotalCompletionTokens),
			"usage":                             _usage,
			"auth_error":                        _authError.Active,
			"auth_error_message":                _authError.Message,
			"auth_error_at":                     _authError.UpdatedAt,
			"usage_percent":                     _usage.OverallUsagePercent(),
			"remaining_percent":                 _usage.OverallRemainingPercent(),
			"request_usage_percent":             _usage.RequestUsagePercent,
			"request_remaining_percent":         _usage.RequestRemainingPercent,
			"token_usage_percent":               _usage.TokenUsagePercent,
			"token_remaining_percent":           _usage.TokenRemainingPercent,
			"codex_primary_used_percent":        _usage.CodexPrimaryUsedPercent,
			"codex_primary_remaining_percent":   _usage.CodexPrimaryRemainPercent,
			"codex_secondary_used_percent":      _usage.CodexSecondaryUsedPercent,
			"codex_secondary_remaining_percent": _usage.CodexSecondaryRemainPercent,
			"models":                            _provider.Config.Models,
		})
	}
	return _status
}

// -------------------------------------------------------------------------------------
func (_b *LoadBalancer) StatusText() string {
	_b._lock.RLock()
	defer _b._lock.RUnlock()

	if len(_b.Providers) == 0 {
		return "no providers configured"
	}

	_parts := make([]string, 0, len(_b.Providers))
	for _, _provider := range _b.Providers {
		_, _latencyP95MS, _lastDurationMS, _reactionMS, _, _tokenSpeed, _, _ := _provider.MetricsSnapshot()
		_parts = append(_parts, fmt.Sprintf("%s active=%d success=%d failure=%d reaction=%.0fms last=%.0fms p95=%.0fms tok/s=%.2f circuit=%t",
			_provider.Config.ID,
			atomic.LoadInt64(&_provider.Active),
			atomic.LoadInt64(&_provider.Successes),
			atomic.LoadInt64(&_provider.Failures),
			_reactionMS,
			_lastDurationMS,
			_latencyP95MS,
			_tokenSpeed,
			_provider.CircuitOpen(time.Now()),
		))
	}

	return strings.Join(_parts, "; ")
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) StartRequest() {
	atomic.AddInt64(&_p.Active, 1)
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) ActiveCount() int64 {
	return atomic.LoadInt64(&_p.Active)
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) SuccessCount() int64 {
	return atomic.LoadInt64(&_p.Successes)
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) FailureCount() int64 {
	return atomic.LoadInt64(&_p.Failures)
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) FinishRequest() {
	atomic.AddInt64(&_p.Active, -1)
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) MarkSuccess(_latency time.Duration) {
	_p.MarkSuccessWithMetrics(_latency, 0, 0, 0, 0)
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) MarkSuccessWithMetrics(_latency time.Duration, _completionTokens int, _reactionMS float64, _tokenSpeed float64, _clientDeliveryTPS float64) {
	_durationMS := float64(_latency) / float64(time.Millisecond)
	_tokenSpeed = NormalizeTokenRate(int64(_completionTokens), _durationMS, _tokenSpeed)
	_clientDeliveryTPS = NormalizeTokenRate(int64(_completionTokens), _durationMS, _clientDeliveryTPS)
	atomic.AddInt64(&_p.Successes, 1)
	atomic.StoreInt64(&_p.ConsecutiveFailures, 0)
	_p.overloadLock.Lock()
	// 冷卻期間完成的既有請求不代表來源已恢復，不能清除這一波退避。
	if !time.Now().Before(_p.overloadUntil) {
		_p.consecutiveOverloads = 0
		_p.overloadUntil = time.Time{}
	}
	_p.overloadLock.Unlock()
	atomic.StoreInt64(&_p.CircuitOpenUntil, 0)
	_p.ClearAuthError()
	_p.recordLatency(_latency)
	_p.recordReaction(_reactionMS)
	_p.recordTokenMetrics(_completionTokens, _tokenSpeed)
	_p.recordClientDeliveryTPS(_clientDeliveryTPS)
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) MarkFailure(_latency time.Duration) {
	atomic.AddInt64(&_p.Failures, 1)
	_failures := atomic.AddInt64(&_p.ConsecutiveFailures, 1)
	_p.recordLatency(_latency)
	if _failures >= 3 {
		atomic.StoreInt64(&_p.CircuitOpenUntil, time.Now().Add(30*time.Second).UnixNano())
	}
}

// -------------------------------------------------------------------------------------
// NextOverloadBackoff 共用連續過載次數；明確的上游等待時間不受本地上限限制。
func (_p *ProviderRuntime) NextOverloadBackoff(retryAfter time.Duration) time.Duration {
	_p.overloadLock.Lock()
	defer _p.overloadLock.Unlock()
	now := time.Now()
	if _p.overloadUntil.After(now) {
		// 同一窗口不重抽亂數或升級；僅明確的較長 Retry-After 可延長。
		if retryAfter > 0 && now.Add(retryAfter).After(_p.overloadUntil) {
			_p.overloadUntil = now.Add(retryAfter)
		}
		return _p.overloadUntil.Sub(now)
	}
	_p.consecutiveOverloads = min(_p.consecutiveOverloads+1, 4)
	delay := retryAfter
	if delay <= 0 {
		delay = (2*time.Second)<<uint(_p.consecutiveOverloads-1) + time.Duration(rand.Int64N(int64(time.Second)))
	}
	_p.overloadUntil = now.Add(delay)
	return delay
}

func (_p *ProviderRuntime) MarkTemporaryUnavailable(_latency time.Duration, _duration time.Duration) {
	if _duration <= 0 {
		_duration = 30 * time.Second
	}
	atomic.AddInt64(&_p.Failures, 1)
	_p.recordLatency(_latency)
	_until := time.Now().Add(_duration).UnixNano()
	for {
		_previous := atomic.LoadInt64(&_p.CapacityUnavailableUntil)
		if _previous >= _until || atomic.CompareAndSwapInt64(&_p.CapacityUnavailableUntil, _previous, _until) {
			break
		}
	}
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) CircuitOpen(_now time.Time) bool {
	_until := atomic.LoadInt64(&_p.CircuitOpenUntil)
	return _until > 0 && _now.UnixNano() < _until
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) CapacityUnavailable(_now time.Time) bool {
	_until := max(atomic.LoadInt64(&_p.CapacityUnavailableUntil), atomic.LoadInt64(&_p.QuotaUnavailableUntil))
	return _until > 0 && _now.UnixNano() < _until
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) LatencySnapshot() (float64, float64) {
	_p._latencyLock.Lock()
	defer _p._latencyLock.Unlock()
	return _p.LatencyEWMA50MS, _p.LatencyEWMA95MS
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) MetricsSnapshot() (float64, float64, float64, float64, float64, float64, float64, int64) {
	_p._latencyLock.Lock()
	defer _p._latencyLock.Unlock()
	return _p.LatencyEWMA50MS, _p.LatencyEWMA95MS, _p.LastDurationMS, _p.ReactionEWMA, _p.LastReactionMS, _p.TokenSpeedEWMA, _p.LastTokenSpeed, _p.LastCompletionTokens
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) ClientDeliverySnapshot() (float64, float64) {
	_p._latencyLock.Lock()
	defer _p._latencyLock.Unlock()
	return _p.ClientDeliveryEWMA, _p.LastClientDeliveryTPS
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) RecordUsageHeaders(_headers http.Header) {
	if _p == nil || len(_headers) == 0 {
		return
	}
	_values := providerUsageHeaders(_headers)
	if len(_values) == 0 {
		return
	}
	_snapshot := buildProviderUsageSnapshot(_values)
	_remaining, _known := _snapshot.KnownRemainingPercent()
	if !_known {
		return
	}
	_p._usageLock.Lock()
	// 不用不完整的新快照取代完整的舊快照，避免統計在不同額度窗口間跳動。
	if !_snapshot.coversUsage(_p.Usage) {
		_p._usageLock.Unlock()
		return
	}
	_p.Usage = _snapshot
	_p._usageLock.Unlock()
	if _p.Config == nil || !_snapshot.HasUsageInfo() {
		return
	}
	if _err := providerusage.DefaultRecorder().Record(
		_p.Config.ID,
		100-_remaining,
		_remaining,
		_snapshot.UpdatedAt,
	); _err != nil {
		log.Printf("provider usage history record failed: provider=%s error=%v", _p.Config.ID, _err)
	}
}

// -------------------------------------------------------------------------------------
// QuotaBelowPeerAverage 回傳指定 provider 的可用量是否比「其他可選 provider 的平均」
// 低超過 _tolerancePoints 個百分點，用來判斷對話黏著是否已造成配額失衡。
// 沒有配額資訊、或找不到可比對的同儕時回傳 false —— 資訊不足時寧可維持黏著。
// ProviderAvailableForSelection 回報指定 provider 此刻是否還能被選中。
// 對話黏著若把請求釘在一個當下不可選的 provider（滿載、熔斷、配額暫時不可用…），
// 選擇階段會找不到候選而直接回 503；此時寧可放棄黏著、降級重新負載平衡。
//
// 刻意「不」檢查配額見底：5% 是**新對話**的選擇保留量，不代表既有對話不能繼續用。
// 用門檻趕走既有對話的代價是每輪清掉 previous_response_id 與加密推理內容，
// 模型得重讀檔案、重新推導 —— 那些重複工作要花 token，配額掉得更快，
// 更多帳號見底、更多對話被趕走，是個正回饋。
//
// 改成證據式：維持黏著直到該 provider 真的拒絕這次請求（capacity 錯誤會觸發
// releaseContinuity 換帳號），或 QuotaBelowPeerAverage 判定失衡過大。
// 這裡只保留「確定不能用」的條件：停用、熔斷、容量冷卻中、認證失敗、併發已滿。
func (_b *LoadBalancer) ProviderAvailableForSelection(_providerID string) bool {
	_providerID = strings.TrimSpace(_providerID)
	if _b == nil || _providerID == "" {
		return false
	}

	_b._lock.RLock()
	defer _b._lock.RUnlock()

	_now := time.Now()
	for _, _provider := range _b.Providers {
		if _provider == nil || _provider.Config == nil {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(_provider.Config.ID), _providerID) {
			continue
		}
		return _provider.Config.Enabled &&
			!strings.EqualFold(_provider.Config.Role, "classifier") &&
			strings.TrimSpace(_provider.Config.BaseURL) != "" &&
			!_provider.CircuitOpen(_now) &&
			!_provider.CapacityUnavailable(_now) &&
			!_provider.HasAuthError() &&
			(_provider.Config.MaxConcurrent <= 0 || atomic.LoadInt64(&_provider.Active) < _provider.Config.MaxConcurrent)
	}
	return false
}

// -------------------------------------------------------------------------------------
func (_b *LoadBalancer) QuotaBelowPeerAverage(_providerID string, _tolerancePoints float64) bool {
	_providerID = strings.TrimSpace(_providerID)
	if _b == nil || _providerID == "" {
		return false
	}

	_b._lock.RLock()
	defer _b._lock.RUnlock()

	var _targetProvider *ProviderRuntime
	for _, _provider := range _b.Providers {
		if _provider != nil && _provider.Config != nil && strings.EqualFold(strings.TrimSpace(_provider.Config.ID), _providerID) {
			_targetProvider = _provider
			break
		}
	}
	if _targetProvider == nil || !_targetProvider.Config.Enabled {
		return false
	}
	_targetUsage := _targetProvider.UsageSnapshot()
	if !_targetUsage.HasUsageInfo() {
		return false
	}
	_target := _targetUsage.OverallRemainingPercent()
	_peerSum := 0.0
	_peerCount := 0
	_now := time.Now()
	for _, _provider := range _b.Providers {
		if _provider == nil || _provider.Config == nil || _provider == _targetProvider || !_provider.Config.Enabled {
			continue
		}
		if strings.EqualFold(_provider.Config.Role, "classifier") {
			continue
		}
		if !sameProviderQuotaFamily(_targetProvider.Config, _provider.Config) {
			continue
		}
		if _provider.CircuitOpen(_now) || _provider.CapacityUnavailable(_now) || _provider.HasAuthError() {
			continue
		}
		if _provider.Config.MaxConcurrent > 0 && atomic.LoadInt64(&_provider.Active) >= _provider.Config.MaxConcurrent {
			continue
		}
		_usage := _provider.UsageSnapshot()
		if !_usage.HasUsageInfo() || _usage.ExhaustedForSelection() {
			continue
		}
		_peerSum += _usage.OverallRemainingPercent()
		_peerCount++
	}

	if _peerCount == 0 {
		return false
	}
	return (_peerSum/float64(_peerCount))-_target > _tolerancePoints
}

// -------------------------------------------------------------------------------------
func sameProviderQuotaFamily(_left *domain.LLMProviderConfig, _right *domain.LLMProviderConfig) bool {
	if _left == nil || _right == nil {
		return false
	}
	_leftKind := strings.TrimSpace(_left.Kind)
	_rightKind := strings.TrimSpace(_right.Kind)
	if _leftKind != "" || _rightKind != "" {
		return _leftKind != "" && _rightKind != "" && strings.EqualFold(_leftKind, _rightKind)
	}
	_leftType := strings.TrimSpace(_left.Type)
	_rightType := strings.TrimSpace(_right.Type)
	if _leftType != "" || _rightType != "" {
		return _leftType != "" && _rightType != "" && strings.EqualFold(_leftType, _rightType)
	}
	return true
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) UsageSnapshot() ProviderUsageSnapshot {
	if _p == nil {
		return ProviderUsageSnapshot{}
	}
	_p._usageLock.Lock()
	defer _p._usageLock.Unlock()
	return cloneProviderUsageSnapshot(_p.Usage)
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) UsageStale(_now time.Time, _maxAge time.Duration) bool {
	if _p == nil {
		return false
	}
	_snapshot := _p.UsageSnapshot()
	if _snapshot.UpdatedAt.IsZero() {
		return true
	}
	return _now.Sub(_snapshot.UpdatedAt) >= _maxAge
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) ShouldProbeUsage(_now time.Time, _maxAge time.Duration) bool {
	if _p == nil || _p.HasAuthError() || !_p.UsageStale(_now, _maxAge) {
		return false
	}
	_lastProbe := atomic.LoadInt64(&_p.LastUsageProbeAt)
	if _lastProbe <= 0 {
		return true
	}
	return _now.Sub(time.Unix(0, _lastProbe)) >= _maxAge
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) MarkUsageProbeAttempt(_now time.Time) {
	if _p == nil {
		return
	}
	if _now.IsZero() {
		_now = time.Now()
	}
	atomic.StoreInt64(&_p.LastUsageProbeAt, _now.UnixNano())
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) MarkAuthError(_message string) {
	if _p == nil {
		return
	}
	_message = strings.TrimSpace(_message)
	if _message == "" {
		_message = "provider authentication token is invalid"
	}
	_p._usageLock.Lock()
	_p.AuthError = ProviderAuthErrorState{
		Active:    true,
		Message:   _message,
		UpdatedAt: time.Now(),
	}
	_p._usageLock.Unlock()
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) ClearAuthError() {
	if _p == nil {
		return
	}
	_p._usageLock.Lock()
	_p.AuthError = ProviderAuthErrorState{}
	_p._usageLock.Unlock()
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) HasAuthError() bool {
	if _p == nil {
		return false
	}
	_p._usageLock.Lock()
	defer _p._usageLock.Unlock()
	return _p.AuthError.Active
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) AuthErrorSnapshot() ProviderAuthErrorState {
	if _p == nil {
		return ProviderAuthErrorState{}
	}
	_p._usageLock.Lock()
	defer _p._usageLock.Unlock()
	return _p.AuthError
}

// -------------------------------------------------------------------------------------
func providerUsageHeaders(_headers http.Header) map[string]string {
	_values := map[string]string{}
	for _key, _items := range _headers {
		_normalized := strings.ToLower(strings.TrimSpace(_key))
		if !isProviderUsageHeader(_normalized) {
			continue
		}
		_value := strings.TrimSpace(strings.Join(_items, ", "))
		if _value != "" {
			_values[_normalized] = _value
		}
	}
	return _values
}

// -------------------------------------------------------------------------------------
func isProviderUsageHeader(_key string) bool {
	_key = strings.ToLower(strings.TrimSpace(_key))
	return strings.HasPrefix(_key, "x-ratelimit-") ||
		strings.HasPrefix(_key, "ratelimit-") ||
		strings.HasPrefix(_key, "x-codex-")
}

// -------------------------------------------------------------------------------------
func buildProviderUsageSnapshot(_headers map[string]string) ProviderUsageSnapshot {
	_snapshot := ProviderUsageSnapshot{
		UpdatedAt:         time.Now(),
		LimitRequests:     firstHeaderValue(_headers, "x-ratelimit-limit-requests", "ratelimit-limit-requests"),
		RemainingRequests: firstHeaderValue(_headers, "x-ratelimit-remaining-requests", "ratelimit-remaining-requests"),
		ResetRequests:     firstHeaderValue(_headers, "x-ratelimit-reset-requests", "ratelimit-reset-requests"),
		LimitTokens:       firstHeaderValue(_headers, "x-ratelimit-limit-tokens", "ratelimit-limit-tokens"),
		RemainingTokens:   firstHeaderValue(_headers, "x-ratelimit-remaining-tokens", "ratelimit-remaining-tokens"),
		ResetTokens:       firstHeaderValue(_headers, "x-ratelimit-reset-tokens", "ratelimit-reset-tokens"),
		Headers:           cloneStringMap(_headers),
	}
	_snapshot.RequestUsagePercent, _snapshot.RequestRemainingPercent = usagePercentFromLimitRemaining(_snapshot.LimitRequests, _snapshot.RemainingRequests)
	_snapshot.TokenUsagePercent, _snapshot.TokenRemainingPercent = usagePercentFromLimitRemaining(_snapshot.LimitTokens, _snapshot.RemainingTokens)
	_snapshot.CodexPrimaryUsedPercent, _snapshot.CodexPrimaryRemainPercent = codexUsagePercentFromHeader(_headers["x-codex-primary-used-percent"])
	_snapshot.CodexSecondaryUsedPercent, _snapshot.CodexSecondaryRemainPercent = codexUsagePercentFromHeader(_headers["x-codex-secondary-used-percent"])
	return _snapshot
}

// -------------------------------------------------------------------------------------
func usagePercentFromLimitRemaining(_limitText string, _remainingText string) (float64, float64) {
	_limit := numberFromText(_limitText)
	_remaining := numberFromText(_remainingText)
	if _limit <= 0 || _remaining < 0 {
		return 0, 0
	}
	_remainingPercent := clampPercent((_remaining / _limit) * 100)
	return clampPercent(100 - _remainingPercent), _remainingPercent
}

// -------------------------------------------------------------------------------------
func codexUsagePercentFromHeader(_text string) (float64, float64) {
	_used := numberFromText(_text)
	if _used < 0 {
		return 0, 0
	}
	_used = clampPercent(_used)
	return _used, clampPercent(100 - _used)
}

// -------------------------------------------------------------------------------------
func (_s ProviderUsageSnapshot) OverallUsagePercent() float64 {
	if remaining, ok := _s.KnownRemainingPercent(); ok {
		return 100 - remaining
	}
	return 0
}

// -------------------------------------------------------------------------------------
func (_s ProviderUsageSnapshot) HasUsageInfo() bool {
	_, known := _s.KnownRemainingPercent()
	return !_s.UpdatedAt.IsZero() && known
}

// -------------------------------------------------------------------------------------
func (_s ProviderUsageSnapshot) OverallRemainingPercent() float64 {
	if remaining, ok := _s.KnownRemainingPercent(); ok {
		return remaining
	}
	return 0
}

// -------------------------------------------------------------------------------------
func (_s ProviderUsageSnapshot) ExhaustedForSelection() bool {
	return _s.remainingKnownAndExhausted(_s.LimitRequests, _s.RemainingRequests, _s.RequestRemainingPercent) ||
		_s.remainingKnownAndExhausted(_s.LimitTokens, _s.RemainingTokens, _s.TokenRemainingPercent) ||
		_s.codexRemainingKnownAndExhausted("x-codex-primary-used-percent", _s.CodexPrimaryRemainPercent) ||
		_s.codexRemainingKnownAndExhausted("x-codex-secondary-used-percent", _s.CodexSecondaryRemainPercent)
}

// -------------------------------------------------------------------------------------
func (_s ProviderUsageSnapshot) remainingKnownAndExhausted(_limit string, _remaining string, _remainingPercent float64) bool {
	if numberFromText(_limit) <= 0 || numberFromText(_remaining) < 0 {
		return false
	}
	return _remainingPercent <= minProviderUsageRemainingPercent
}

// -------------------------------------------------------------------------------------
func (_s ProviderUsageSnapshot) codexRemainingKnownAndExhausted(_header string, _remainingPercent float64) bool {
	if len(_s.Headers) == 0 {
		return false
	}
	if _, _ok := _s.Headers[strings.ToLower(strings.TrimSpace(_header))]; !_ok {
		return false
	}
	return _remainingPercent <= minProviderUsageRemainingPercent
}

// -------------------------------------------------------------------------------------
func usageScoreAdjustment(_usage ProviderUsageSnapshot) float64 {
	if !_usage.HasUsageInfo() {
		return 0
	}
	_usedPercent := _usage.OverallUsagePercent()
	_remainingPercent := _usage.OverallRemainingPercent()
	_adjustment := (_remainingPercent - 50) * 1.4

	switch {
	case _usedPercent >= 95:
		_adjustment -= 120
	case _usedPercent >= 90:
		_adjustment -= 80
	case _usedPercent >= 80:
		_adjustment -= 45
	}

	return _adjustment
}

// -------------------------------------------------------------------------------------
func firstHeaderValue(_headers map[string]string, _keys ...string) string {
	for _, _key := range _keys {
		if _value := strings.TrimSpace(_headers[strings.ToLower(_key)]); _value != "" {
			return _value
		}
	}
	return ""
}

// -------------------------------------------------------------------------------------
func numberFromText(_text string) float64 {
	_text = strings.TrimSpace(strings.ToLower(_text))
	if _text == "" {
		return -1
	}
	_text = strings.TrimSuffix(_text, "%")
	_text = strings.ReplaceAll(_text, ",", "")
	_value, _err := strconv.ParseFloat(_text, 64)
	if _err != nil {
		return -1
	}
	return _value
}

// -------------------------------------------------------------------------------------
func clampPercent(_value float64) float64 {
	if _value < 0 {
		return 0
	}
	if _value > 100 {
		return 100
	}
	return _value
}

// -------------------------------------------------------------------------------------
func maxPositive(_values ...float64) float64 {
	_max := float64(0)
	for _, _value := range _values {
		if _value > _max {
			_max = _value
		}
	}
	return _max
}

// -------------------------------------------------------------------------------------
func cloneStringMap(_input map[string]string) map[string]string {
	if len(_input) == 0 {
		return nil
	}
	_output := make(map[string]string, len(_input))
	for _key, _value := range _input {
		_output[_key] = _value
	}
	return _output
}

// -------------------------------------------------------------------------------------
func cloneProviderUsageSnapshot(_snapshot ProviderUsageSnapshot) ProviderUsageSnapshot {
	_snapshot.Headers = cloneStringMap(_snapshot.Headers)
	return _snapshot
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) recordLatency(_latency time.Duration) {
	if _latency <= 0 {
		return
	}

	_sampleMS := float64(_latency.Milliseconds())
	_p._latencyLock.Lock()
	defer _p._latencyLock.Unlock()
	_p.LastDurationMS = _sampleMS

	if _p.LatencyEWMA50MS <= 0 {
		_p.LatencyEWMA50MS = _sampleMS
		_p.LatencyEWMA95MS = _sampleMS
		return
	}

	_p.LatencyEWMA50MS = ewma(_p.LatencyEWMA50MS, _sampleMS, 0.25)
	if _sampleMS > _p.LatencyEWMA95MS {
		_p.LatencyEWMA95MS = ewma(_p.LatencyEWMA95MS, _sampleMS, 0.35)
	} else {
		_p.LatencyEWMA95MS = ewma(_p.LatencyEWMA95MS, _sampleMS, 0.05)
	}
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) recordTokenMetrics(_completionTokens int, _tokenSpeed float64) {
	if _completionTokens <= 0 {
		return
	}

	atomic.AddInt64(&_p.TotalCompletionTokens, int64(_completionTokens))
	_p._latencyLock.Lock()
	defer _p._latencyLock.Unlock()

	_p.LastCompletionTokens = int64(_completionTokens)
	if _tokenSpeed <= 0 {
		return
	}
	_p.LastTokenSpeed = _tokenSpeed
	if _p.TokenSpeedEWMA <= 0 {
		_p.TokenSpeedEWMA = _tokenSpeed
		return
	}
	_p.TokenSpeedEWMA = ewma(_p.TokenSpeedEWMA, _tokenSpeed, tokenRateEWMAAlpha)
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) recordClientDeliveryTPS(_clientDeliveryTPS float64) {
	if _clientDeliveryTPS <= 0 {
		return
	}
	_p._latencyLock.Lock()
	defer _p._latencyLock.Unlock()

	_p.LastClientDeliveryTPS = _clientDeliveryTPS
	if _p.ClientDeliveryEWMA <= 0 {
		_p.ClientDeliveryEWMA = _clientDeliveryTPS
		return
	}
	_p.ClientDeliveryEWMA = ewma(_p.ClientDeliveryEWMA, _clientDeliveryTPS, tokenRateEWMAAlpha)
}

// -------------------------------------------------------------------------------------
// RecordProviderReportedTPS 保存 provider 自報的解碼速度（模型內部時鐘）。
// 儀表板的生成速度已改用代理牆鐘，這個值單獨保留供 provider 明細比較模型／硬體效能。
func (_p *ProviderRuntime) RecordProviderReportedTPS(_tps float64) {
	if _tps <= 0 {
		return
	}
	_p._latencyLock.Lock()
	defer _p._latencyLock.Unlock()

	_p.LastProviderReportedTPS = _tps
	if _p.ProviderReportedTPSEWMA <= 0 {
		_p.ProviderReportedTPSEWMA = _tps
		return
	}
	_p.ProviderReportedTPSEWMA = ewma(_p.ProviderReportedTPSEWMA, _tps, tokenRateEWMAAlpha)
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) ProviderReportedTPSSnapshot() (float64, float64) {
	_p._latencyLock.Lock()
	defer _p._latencyLock.Unlock()
	return _p.ProviderReportedTPSEWMA, _p.LastProviderReportedTPS
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) recordReaction(_reactionMS float64) {
	_p._latencyLock.Lock()
	defer _p._latencyLock.Unlock()

	if _reactionMS <= 0 {
		_p.LastReactionMS = 0
		return
	}

	_p.LastReactionMS = _reactionMS
	if _p.ReactionEWMA <= 0 {
		_p.ReactionEWMA = _reactionMS
		return
	}
	_p.ReactionEWMA = ewma(_p.ReactionEWMA, _reactionMS, 0.25)
}

// -------------------------------------------------------------------------------------
func (_p *ProviderRuntime) copyRuntimeState(_old *ProviderRuntime) {
	// 設定重載保留相同來源的冷卻，不讓儲存介面設定繞過退避。
	if _p.Config.BaseURL == _old.Config.BaseURL && _p.Config.APIKey == _old.Config.APIKey && _p.Config.APIKeyEnv == _old.Config.APIKeyEnv && _p.Config.Kind == _old.Config.Kind {
		_p.CapacityUnavailableUntil = atomic.LoadInt64(&_old.CapacityUnavailableUntil)
		_p.QuotaUnavailableUntil = atomic.LoadInt64(&_old.QuotaUnavailableUntil)
		_old.modelCooldownLock.Lock()
		_p.modelCooldowns = make(map[string]time.Time, len(_old.modelCooldowns))
		for k, v := range _old.modelCooldowns {
			_p.modelCooldowns[k] = v
		}
		_old.modelCooldownLock.Unlock()
		_old.overloadLock.Lock()
		_p.consecutiveOverloads, _p.overloadUntil = _old.consecutiveOverloads, _old.overloadUntil
		_old.overloadLock.Unlock()
	}
	_p.Successes = atomic.LoadInt64(&_old.Successes)
	_p.Failures = atomic.LoadInt64(&_old.Failures)
	_p.ConsecutiveFailures = atomic.LoadInt64(&_old.ConsecutiveFailures)
	_p.CircuitOpenUntil = atomic.LoadInt64(&_old.CircuitOpenUntil)

	_p50, _p95, _lastDurationMS, _reactionMS, _lastReactionMS, _tokenSpeed, _lastTokenSpeed, _lastCompletionTokens := _old.MetricsSnapshot()
	_p._latencyLock.Lock()
	defer _p._latencyLock.Unlock()
	_p.LatencyEWMA50MS = _p50
	_p.LatencyEWMA95MS = _p95
	_p.LastDurationMS = _lastDurationMS
	_p.ReactionEWMA = _reactionMS
	_p.LastReactionMS = _lastReactionMS
	_p.TokenSpeedEWMA = _tokenSpeed
	_p.LastTokenSpeed = _lastTokenSpeed
	_p.LastCompletionTokens = _lastCompletionTokens
	_p.ClientDeliveryEWMA, _p.LastClientDeliveryTPS = _old.ClientDeliverySnapshot()
	_p.ProviderReportedTPSEWMA, _p.LastProviderReportedTPS = _old.ProviderReportedTPSSnapshot()
	_p.Usage = _old.UsageSnapshot()
	_p.AuthError = _old.AuthErrorSnapshot()
	atomic.StoreInt64(&_p.LastUsageProbeAt, atomic.LoadInt64(&_old.LastUsageProbeAt))
	atomic.StoreInt64(&_p.TotalCompletionTokens, atomic.LoadInt64(&_old.TotalCompletionTokens))
}

// -------------------------------------------------------------------------------------
func ewma(_current float64, _sample float64, _alpha float64) float64 {
	return (_alpha * _sample) + ((1 - _alpha) * _current)
}

// -------------------------------------------------------------------------------------
