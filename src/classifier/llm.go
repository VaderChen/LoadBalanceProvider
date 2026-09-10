package classifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/providerdispatch"
	"LoadBalanceProvider/src/security"
)

// -------------------------------------------------------------------------------------
type LLM struct {
	Provider   domain.LLMProviderConfig
	HTTPClient *http.Client
}

// -------------------------------------------------------------------------------------
func NewLLM(_provider domain.LLMProviderConfig) *LLM {
	return &LLM{
		Provider: _provider,
		HTTPClient: &http.Client{
			Timeout: 20 * time.Second,
		},
	}
}

// -------------------------------------------------------------------------------------
func (_l *LLM) Name() string {
	return "llm"
}

// -------------------------------------------------------------------------------------
func (_l *LLM) Classify(_ctx context.Context, _req *domain.ChatCompletionRequest, _fallback domain.RequestProfile) (domain.RequestProfile, bool, error) {
	if _l == nil || strings.TrimSpace(_l.Provider.BaseURL) == "" || len(_l.Provider.Models) == 0 {
		return _fallback, false, fmt.Errorf("classifier provider is not configured")
	}

	_payload := map[string]interface{}{
		"model":  _l.Provider.Models[0].Name,
		"stream": false,
		"messages": []map[string]string{
			{
				"role":    "system",
				"content": "Classify the user's request for routing. Return compact JSON only: {\"task_type\":\"chat|coding|reasoning|summarization|translation|creative|extraction|vision|image_analysis|image_generation|video_analysis|video_generation|audio_analysis|transcription|audio_generation|tts\",\"signals\":[\"...\"],\"confidence\":0.0}. Confidence must be a number from 0.0 to 1.0.",
			},
			{
				"role":    "user",
				"content": combineMessages(_req.Messages),
			},
		},
		"max_tokens": 128,
	}

	_body, _err := json.Marshal(_payload)
	if _err != nil {
		return _fallback, false, _err
	}

	_targetURL := _l.Provider.ChatURL()
	if _err := security.ValidateOutboundURL(_targetURL); _err != nil {
		return _fallback, false, _err
	}
	_httpReq, _err := http.NewRequestWithContext(_ctx, http.MethodPost, _targetURL, bytes.NewReader(_body))
	if _err != nil {
		return _fallback, false, _err
	}

	_httpReq.Header.Set("Content-Type", "application/json")
	_httpReq.Header.Set("Accept", "application/json")
	if _apiKey := providerAPIKey(_l.Provider); _apiKey != "" {
		_httpReq.Header.Set("Authorization", "Bearer "+_apiKey)
	}

	_client := _l.HTTPClient
	if _client == nil {
		_client = &http.Client{Timeout: 20 * time.Second}
	}

	if err := _l.Provider.CheckScheduledDowntime(); err != nil {
		return _fallback, false, err
	}
	_resp, _err := providerdispatch.Do(_client, _httpReq, &_l.Provider)
	if _err != nil {
		return _fallback, false, _err
	}
	defer _resp.Body.Close()

	_respBody, _err := io.ReadAll(io.LimitReader(_resp.Body, 2*1024*1024))
	if _err != nil {
		return _fallback, false, _err
	}
	if _resp.StatusCode < http.StatusOK || _resp.StatusCode >= http.StatusMultipleChoices {
		return _fallback, false, fmt.Errorf("classifier provider returned status %d", _resp.StatusCode)
	}

	_taskType, _signals, _confidence, _err := parseClassificationResponse(_respBody)
	if _err != nil {
		return _fallback, false, _err
	}

	_profile := _fallback
	if _taskType != "" {
		_profile.TaskType = _taskType
	}
	if len(_signals) > 0 {
		_profile.Signals = _signals
	}
	if !ShouldAcceptLLM(_profile, _confidence) {
		return _fallback, false, nil
	}
	return _profile, true, nil
}

// -------------------------------------------------------------------------------------
func parseClassificationResponse(_body []byte) (string, []string, float64, error) {
	var _payload map[string]interface{}
	if _err := json.Unmarshal(_body, &_payload); _err != nil {
		return "", nil, 0, _err
	}

	_content := ""
	if _choices, _ok := _payload["choices"].([]interface{}); _ok && len(_choices) > 0 {
		if _choice, _ok := _choices[0].(map[string]interface{}); _ok {
			if _message, _ok := _choice["message"].(map[string]interface{}); _ok {
				_content, _ = _message["content"].(string)
			}
		}
	}
	if _content == "" {
		_content, _ = _payload["content"].(string)
	}
	if _content == "" {
		return "", nil, 0, fmt.Errorf("classifier response content is empty")
	}

	_content = extractJSONObject(_content)
	var _classification struct {
		TaskType   string   `json:"task_type"`
		Signals    []string `json:"signals"`
		Confidence float64  `json:"confidence"`
	}
	if _err := json.Unmarshal([]byte(_content), &_classification); _err != nil {
		return "", nil, 0, _err
	}

	_taskType := strings.ToLower(strings.TrimSpace(_classification.TaskType))
	if !IsValidTask(_taskType) {
		return "", nil, 0, fmt.Errorf("invalid task_type: %s", _classification.TaskType)
	}

	return _taskType, uniqueStrings(_classification.Signals), _classification.Confidence, nil
}

// -------------------------------------------------------------------------------------
func extractJSONObject(_value string) string {
	_value = strings.TrimSpace(_value)
	_start := strings.Index(_value, "{")
	_end := strings.LastIndex(_value, "}")
	if _start >= 0 && _end > _start {
		return _value[_start : _end+1]
	}
	return _value
}

// -------------------------------------------------------------------------------------
func combineMessages(_messages []domain.ChatMessage) string {
	var _builder strings.Builder
	for _, _message := range _messages {
		_builder.WriteString(_message.Role)
		_builder.WriteString(": ")
		_builder.WriteString(_message.Text())
		_builder.WriteString("\n")
	}
	return _builder.String()
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
func uniqueStrings(_values []string) []string {
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
	return _result
}

// -------------------------------------------------------------------------------------
