package domain

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const DefaultProviderTimeoutSeconds = 300

// -------------------------------------------------------------------------------------
type AgentConfig struct {
	ServiceName      string      `json:"service_name"`
	MarsCloudURL     string      `json:"mars_cloud_url"`
	MarsCloudAccount string      `json:"mars_cloud_account"`
	MarsCloudPass    string      `json:"mars_cloud_password"`
	MarsCloudProject string      `json:"mars_cloud_proj"`
	HTTPPort         int         `json:"http_port"`
	HTTPSPort        int         `json:"https_port"`
	DefaultAccount   string      `json:"default_account"`
	DefaultPassword  string      `json:"default_pwd"`
	RestartTime      []string    `json:"restart_time"`
	HTTPBasePaths    []string    `json:"http_base_paths"`
	LLMProxy         ProxyConfig `json:"llm_proxy"`
}

// -------------------------------------------------------------------------------------
type NotificationTargetConfig struct {
	URL     string `json:"url"`
	APIKey  string `json:"api_key,omitempty"`
	Payload string `json:"payload"`
}

// -------------------------------------------------------------------------------------
type GeneralSettingsConfig struct {
	ShowProviderModels bool `json:"show_provider_models"`
}

// -------------------------------------------------------------------------------------
type AdvancedSettingsConfig struct {
	ProviderRetryRounds                      int     `json:"provider_retry_rounds"`
	ProviderRetrySourcesPerRound             int     `json:"provider_retry_sources_per_round"`
	ProviderRetryWaitSeconds                 int     `json:"provider_retry_wait_seconds"`
	PersistQuotaCooldown                     bool    `json:"persist_quota_cooldown"`
	ConversationAffinityTTLMinutes           int     `json:"conversation_affinity_ttl_minutes"`
	ConversationAffinityQuotaTolerancePoints float64 `json:"conversation_affinity_quota_tolerance_points"`
	ResponseRouteMaxEntries                  int     `json:"response_route_max_entries"`
	ProviderCapacityCooldownSeconds          int     `json:"provider_capacity_cooldown_seconds"`
	ProviderServerErrorCooldownSeconds       int     `json:"provider_server_error_cooldown_seconds"`
	MaxBindingsPerProvider                   int     `json:"max_bindings_per_provider"`
	YieldLowMaxPercent                       float64 `json:"yield_low_max_percent"`
	YieldMidMaxPercent                       float64 `json:"yield_mid_max_percent"`
	// 低推理降級：高頻率且推理佔比極低時，暫時把該金鑰壓到低階模型。
	// 依據是「模型自己的難度評估」——推理量少代表這個工作量不需要強模型。
	LowReasoningDemotionEnabled          bool    `json:"low_reasoning_demotion_enabled"`
	LowReasoningDemotionRequestsPerMin   float64 `json:"low_reasoning_demotion_requests_per_minute"`
	LowReasoningDemotionReasoningPercent float64 `json:"low_reasoning_demotion_reasoning_percent"`
	LowReasoningDemotionTargetTier       int     `json:"low_reasoning_demotion_target_tier"`
	// 解除必須是時間驅動而非指標驅動：降級後的模型推理本來就少，
	// 若用指標決定何時解除，會永遠留在低階模型而觀察不到反事實。
	LowReasoningDemotionMinutes int `json:"low_reasoning_demotion_minutes"`
	// 啟動條件：今日配額消耗未達這個百分比之前完全不偵測。
	// 配額還很充裕時沒有必要限制任何人 —— 大量消耗本身不是問題。
	LowReasoningDemotionMinDailyUsagePercent float64 `json:"low_reasoning_demotion_min_daily_usage_percent"`
}

// -------------------------------------------------------------------------------------
// MCPSettingsConfig 控制標準 MCP Streamable HTTP 端點。
// MCP 沿用既有 API Key 驗證，但工具清單永遠排除金鑰管理能力。
type MCPSettingsConfig struct {
	Enabled        bool     `json:"enabled"`
	ReadOnly       bool     `json:"read_only"`
	AllowedOrigins []string `json:"allowed_origins"`
}

// -------------------------------------------------------------------------------------
type ProxyConfig struct {
	SelectionStrategy string              `json:"selection_strategy"`
	RetryCount        int                 `json:"retry_count"`
	Providers         []LLMProviderConfig `json:"providers"`
}

// -------------------------------------------------------------------------------------
type LLMProviderConfig struct {
	ID                  string           `json:"id"`
	Name                string           `json:"name"`
	Kind                string           `json:"kind,omitempty"`
	Type                string           `json:"type"`
	Role                string           `json:"role,omitempty"`
	BaseURL             string           `json:"base_url"`
	APIKey              string           `json:"api_key"`
	APIKeyEnv           string           `json:"api_key_env"`
	ChatCompletionsPath string           `json:"chat_completions_path"`
	Enabled             bool             `json:"enabled"`
	Weight              int              `json:"weight"`
	Priority            int              `json:"priority"`
	TimeoutSeconds      int              `json:"timeout_seconds"`
	MaxConcurrent       int64            `json:"max_concurrent"`
	Models              []LLMModelConfig `json:"models"`
	Purpose             string           `json:"purpose,omitempty"`
	Scale               string           `json:"scale,omitempty"`
	Responsibility      string           `json:"responsibility,omitempty"`
	ReasoningEffort     string           `json:"reasoning_effort,omitempty"`
}

// -------------------------------------------------------------------------------------
type LLMModelConfig struct {
	Name            string   `json:"name"`
	Aliases         []string `json:"aliases"`
	MaxInputTokens  int      `json:"max_input_tokens"`
	MaxOutputTokens int      `json:"max_output_tokens"`
	Capabilities    []string `json:"capabilities"`
	CostTier        int      `json:"cost_tier"`
	QualityTier     int      `json:"quality_tier"`
}

// -------------------------------------------------------------------------------------
type ChatCompletionRequest struct {
	// MaxQualityTier 是伺服器端加上的模型等級上限（0 表示不限）。
	// 標記 json:"-" 是刻意的：這是內部政策，不能讓用戶端自己設定，
	// 否則任何人都可以宣告自己不受降級限制。
	MaxQualityTier        int                    `json:"-"`
	Model                 string                 `json:"model"`
	Provider              string                 `json:"provider,omitempty"`
	ProviderID            string                 `json:"provider_id,omitempty"`
	Messages              []ChatMessage          `json:"messages"`
	Attachments           []ChatAttachment       `json:"attachments,omitempty"`
	Stream                bool                   `json:"stream"`
	MaxTokens             *int                   `json:"max_tokens,omitempty"`
	MaxTokensV2           *int                   `json:"max_completion_tokens,omitempty"`
	Tools                 []interface{}          `json:"tools,omitempty"`
	ToolChoice            interface{}            `json:"tool_choice,omitempty"`
	ParallelToolCalls     *bool                  `json:"parallel_tool_calls,omitempty"`
	ResponseFormat        map[string]interface{} `json:"response_format,omitempty"`
	Reasoning             map[string]interface{} `json:"reasoning,omitempty"`
	ReasoningEffort       string                 `json:"reasoning_effort,omitempty"`
	PromptCacheKey        string                 `json:"prompt_cache_key,omitempty"`
	BenchmarkTextFallback bool                   `json:"benchmark_text_completion_fallback,omitempty"`
	RequiredCapabilities  []string               `json:"-"`
}

// -------------------------------------------------------------------------------------
type ChatMessage struct {
	Role       string         `json:"role"`
	Content    interface{}    `json:"content"`
	ToolCalls  []ChatToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

// -------------------------------------------------------------------------------------
type ChatToolCall struct {
	ID       string           `json:"id,omitempty"`
	Type     string           `json:"type,omitempty"`
	Function ChatFunctionCall `json:"function"`
}

// -------------------------------------------------------------------------------------
type ChatFunctionCall struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

// -------------------------------------------------------------------------------------
type ChatAttachment struct {
	Name      string `json:"name,omitempty"`
	MIMEType  string `json:"mime_type,omitempty"`
	MediaType string `json:"media_type,omitempty"`
	Content   string `json:"content,omitempty"`
	FileData  string `json:"file_data,omitempty"`
	SizeBytes int    `json:"size_bytes,omitempty"`
}

// -------------------------------------------------------------------------------------
func (_a *ChatAttachment) UnmarshalJSON(_data []byte) error {
	var _raw map[string]interface{}
	if _err := json.Unmarshal(_data, &_raw); _err != nil {
		return _err
	}
	_a.Name = firstRawString(_raw, "name", "filename", "file_name", "fileName")
	_a.MIMEType = firstRawString(_raw, "mime_type", "mimeType", "mimetype", "content_type", "contentType")
	_a.MediaType = firstRawString(_raw, "media_type", "mediaType", "type")
	_a.Content = firstRawString(_raw, "content", "data", "data_url", "dataUrl", "url")
	_a.FileData = firstRawString(_raw, "file_data", "fileData", "base64")
	_a.SizeBytes = firstRawInt(_raw, "size_bytes", "sizeBytes", "size")
	return nil
}

// -------------------------------------------------------------------------------------
func firstRawString(_raw map[string]interface{}, _keys ...string) string {
	for _, _key := range _keys {
		_value, _ok := _raw[_key]
		if !_ok || _value == nil {
			continue
		}
		if _text, _ok := _value.(string); _ok {
			if strings.TrimSpace(_text) != "" {
				return strings.TrimSpace(_text)
			}
			continue
		}
		_text := strings.TrimSpace(fmt.Sprint(_value))
		if _text != "" {
			return _text
		}
	}
	return ""
}

// -------------------------------------------------------------------------------------
func firstRawInt(_raw map[string]interface{}, _keys ...string) int {
	for _, _key := range _keys {
		_value, _ok := _raw[_key]
		if !_ok || _value == nil {
			continue
		}
		switch _typed := _value.(type) {
		case int:
			return _typed
		case float64:
			return int(_typed)
		case string:
			var _number int
			if _, _err := fmt.Sscanf(strings.TrimSpace(_typed), "%d", &_number); _err == nil {
				return _number
			}
		}
	}
	return 0
}

// -------------------------------------------------------------------------------------
type RequestProfile struct {
	InputCharacters       int      `json:"input_characters"`
	EstimatedInputTokens  int      `json:"estimated_input_tokens"`
	RequestedOutputTokens int      `json:"requested_output_tokens"`
	MessageCount          int      `json:"message_count"`
	TaskType              string   `json:"task_type"`
	ComplexityScore       int      `json:"complexity_score"`
	Signals               []string `json:"signals"`
	HardRequirements      []string `json:"hard_requirements"`
}

// -------------------------------------------------------------------------------------
type ErrorPayload struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

// -------------------------------------------------------------------------------------
func ErrorResponse(_kind string, _message string) ErrorPayload {
	return ErrorPayload{Error: ErrorDetail{Type: _kind, Message: _message}}
}

// -------------------------------------------------------------------------------------
func (_m ChatMessage) Text() string {
	return contentText(_m.Content)
}

// -------------------------------------------------------------------------------------
func contentText(_content interface{}) string {
	switch _content := _content.(type) {
	case string:
		return redactMediaDataURLs(_content)
	case []interface{}:
		var _builder strings.Builder
		for _, _part := range _content {
			_text := contentText(_part)
			if strings.TrimSpace(_text) != "" {
				_builder.WriteString(_text)
				_builder.WriteString("\n")
			}
		}
		return _builder.String()
	case map[string]interface{}:
		if _text, _ok := _content["text"].(string); _ok {
			return redactMediaDataURLs(_text)
		}
		if _contentValue, _ok := _content["content"]; _ok {
			return contentText(_contentValue)
		}
		if mediaKindFromContentMap(_content) != "" {
			return "[" + mediaKindFromContentMap(_content) + "]"
		}
	}

	_raw, _err := json.Marshal(_content)
	if _err != nil {
		return ""
	}

	return redactMediaDataURLs(string(_raw))
}

// -------------------------------------------------------------------------------------
func mediaKindFromContentMap(_content map[string]interface{}) string {
	if _type := strings.ToLower(strings.TrimSpace(firstRawString(_content, "type"))); _type != "" {
		switch {
		case strings.Contains(_type, "image"):
			return "image"
		case strings.Contains(_type, "audio"):
			return "audio"
		case strings.Contains(_type, "video"):
			return "video"
		}
	}
	for _, _key := range []string{"image_url", "imageUrl", "input_image", "inputImage", "image", "file_data", "fileData"} {
		if _value, _ok := _content[_key]; _ok && _value != nil {
			_text := strings.ToLower(strings.TrimSpace(fmt.Sprint(_value)))
			if strings.Contains(strings.ToLower(_key), "image") || strings.HasPrefix(_text, "data:image/") {
				return "image"
			}
		}
	}
	return ""
}

// -------------------------------------------------------------------------------------
func redactMediaDataURLs(_text string) string {
	_text = strings.TrimSpace(_text)
	if _text == "" {
		return ""
	}
	_replacements := []struct {
		Prefix string
		Label  string
	}{
		{Prefix: "data:image/", Label: "[image]"},
		{Prefix: "data:audio/", Label: "[audio]"},
		{Prefix: "data:video/", Label: "[video]"},
	}
	_result := _text
	for _, _replacement := range _replacements {
		_result = redactDataURLPrefix(_result, _replacement.Prefix, _replacement.Label)
	}
	return _result
}

// -------------------------------------------------------------------------------------
func redactDataURLPrefix(_text string, _prefix string, _label string) string {
	var _builder strings.Builder
	_remaining := _text
	for {
		_idx := strings.Index(strings.ToLower(_remaining), _prefix)
		if _idx < 0 {
			_builder.WriteString(_remaining)
			return _builder.String()
		}
		_builder.WriteString(_remaining[:_idx])
		_builder.WriteString(_label)
		_tail := _remaining[_idx:]
		_end := len(_tail)
		for _pos, _rune := range _tail {
			if _pos == 0 {
				continue
			}
			if strings.ContainsRune(" \t\r\n\"'<>)]}", _rune) {
				_end = _pos
				break
			}
		}
		_remaining = _tail[_end:]
	}
}

// -------------------------------------------------------------------------------------
func (_p LLMProviderConfig) ChatURL() string {
	_base := strings.TrimRight(_p.BaseURL, "/")
	_path := strings.TrimSpace(_p.ChatCompletionsPath)
	if _path == "" {
		_path = "/v1/chat/completions"
	}
	if !strings.HasPrefix(_path, "/") {
		_path = "/" + _path
	}
	return _base + _path
}

// -------------------------------------------------------------------------------------
func (_m LLMModelConfig) MatchName(_name string) bool {
	if _name == "" || strings.EqualFold(_name, "auto") {
		return true
	}

	if strings.EqualFold(_m.Name, _name) {
		return true
	}

	for _, _alias := range _m.Aliases {
		if strings.EqualFold(_alias, _name) {
			return true
		}
	}

	return false
}

// -------------------------------------------------------------------------------------
func (_m LLMModelConfig) HasCapability(_capability string) bool {
	for _, _cap := range _m.Capabilities {
		if strings.EqualFold(_cap, _capability) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func (_m LLMModelConfig) String() string {
	return fmt.Sprintf("%s(input=%d output=%d)", _m.Name, _m.MaxInputTokens, _m.MaxOutputTokens)
}

// -------------------------------------------------------------------------------------
func RFC3339Now() string {
	return time.Now().Format(time.RFC3339)
}

// -------------------------------------------------------------------------------------
