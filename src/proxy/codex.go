package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/codexauth"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/security"
)

// -------------------------------------------------------------------------------------
const defaultOpenAICodexAPIResponsesURL = "https://api.openai.com/v1/responses"
const defaultOpenAICodexOAuthResponsesURL = "https://chatgpt.com/backend-api/codex/responses"
const defaultCodexClientVersion = "0.153.4"
const defaultCodexUpstreamUserAgent = "codex-tui/" + defaultCodexClientVersion + " (Mac OS; arm64)"
const defaultCodexUpstreamOriginator = "codex-tui"

// -------------------------------------------------------------------------------------
type codexResponsesRequest struct {
	Model        string                  `json:"model"`
	Instructions string                  `json:"instructions,omitempty"`
	Input        []codexResponsesMessage `json:"input"`
	Tools        []interface{}           `json:"tools,omitempty"`
	ToolChoice   interface{}             `json:"tool_choice,omitempty"`
	Parallel     *bool                   `json:"parallel_tool_calls,omitempty"`
	Reasoning    *codexReasoningConfig   `json:"reasoning,omitempty"`
	Include      []string                `json:"include,omitempty"`
	PromptCache  string                  `json:"prompt_cache_key,omitempty"`
	Stream       bool                    `json:"stream"`
	Store        bool                    `json:"store"`
}

// -------------------------------------------------------------------------------------
type codexReasoningConfig struct {
	Effort  string `json:"effort"`
	Summary string `json:"summary,omitempty"`
}

// -------------------------------------------------------------------------------------
type codexResponsesMessage struct {
	Type    string                      `json:"type"`
	Role    string                      `json:"role,omitempty"`
	Content []codexResponsesContentPart `json:"content,omitempty"`
	CallID  string                      `json:"call_id,omitempty"`
	Name    string                      `json:"name,omitempty"`
	Args    string                      `json:"arguments,omitempty"`
	Output  *string                     `json:"output,omitempty"`
}

// -------------------------------------------------------------------------------------
type codexResponsesContentPart struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	ImageURL string `json:"image_url,omitempty"`
	Detail   string `json:"detail,omitempty"`
}

// -------------------------------------------------------------------------------------
type codexCompleted struct {
	ID     string            `json:"id"`
	Model  string            `json:"model"`
	Output []codexOutputItem `json:"output"`
	Usage  codexUsage        `json:"usage"`
}

// -------------------------------------------------------------------------------------
type codexOutputItem struct {
	ID        string `json:"id,omitempty"`
	Type      string `json:"type"`
	Role      string `json:"role,omitempty"`
	CallID    string `json:"call_id,omitempty"`
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
	Content   []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content,omitempty"`
}

// -------------------------------------------------------------------------------------
type codexUsage struct {
	InputTokens         int            `json:"input_tokens"`
	OutputTokens        int            `json:"output_tokens"`
	TotalTokens         int            `json:"total_tokens"`
	OutputTokensDetails map[string]int `json:"output_tokens_details,omitempty"`
}

// -------------------------------------------------------------------------------------
func isOpenAICodexProvider(_provider *balancer.ProviderRuntime) bool {
	if _provider == nil || _provider.Config == nil {
		return false
	}
	return isOpenAICodexProviderConfig(_provider.Config)
}

// -------------------------------------------------------------------------------------
func isOpenAICodexProviderConfig(_provider *domain.LLMProviderConfig) bool {
	if _provider == nil {
		return false
	}
	_kind := strings.ToLower(strings.TrimSpace(_provider.Kind))
	_type := strings.ToLower(strings.TrimSpace(_provider.Type))
	return _kind == "openai-codex" || _type == "openai-codex"
}

// -------------------------------------------------------------------------------------
func (_c *Client) forwardOpenAICodexCompletion(_ctx context.Context, _w http.ResponseWriter, _srcReq *http.Request, _provider *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _chatReq *domain.ChatCompletionRequest, _profile domain.RequestProfile, _selectionMeta balancer.SelectionMeta) (ChatMetrics, error) {
	_started := time.Now()
	_upstreamModel := codexUpstreamModelName(_model.Name)
	_apiKey := providerAPIKey(_provider)
	_useAPIKey := _apiKey != ""
	_authToken := strings.TrimSpace(_apiKey)
	_accountID := ""
	if _authToken == "" {
		auth, err := codexauth.EnsureContext(_ctx, _provider.Config.ID)
		if err != nil {
			return ChatMetrics{}, fmt.Errorf("openai codex oauth unavailable: %w", err)
		}
		_authToken = strings.TrimSpace(auth.AccessToken)
		_accountID = strings.TrimSpace(auth.AccountID)
	}
	payload := buildCodexResponsesRequest(_chatReq, _upstreamModel, _provider.Config)
	if !_useAPIKey {
		payload.Stream = true
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return ChatMetrics{}, err
	}
	_targetURL := codexResponsesURL(*_provider.Config, _useAPIKey)
	if err := security.ValidateOutboundURL(_targetURL); err != nil {
		return ChatMetrics{}, err
	}
	req, err := http.NewRequestWithContext(_ctx, http.MethodPost, _targetURL, bytes.NewReader(body))
	if err != nil {
		return ChatMetrics{}, err
	}
	req.Header.Set("Authorization", "Bearer "+_authToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	applyCodexUpstreamHeaders(_srcReq, req, !_chatReq.BenchmarkTextFallback)
	if _accountID != "" {
		req.Header.Set("chatgpt-account-id", _accountID)
	}

	client := _c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 0}
	}
	resp, err := doCodexHTTPRequest(client, req, _provider, _useAPIKey)
	if err != nil {
		return ChatMetrics{}, err
	}
	defer resp.Body.Close()
	_provider.RecordUsageHeaders(resp.Header)

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
		failure := &ProviderStatusError{FailureDetails: FailureDetails{RetryAfter: retryAfterHeader(resp.Header)}, StatusCode: resp.StatusCode, Message: strings.TrimSpace(string(raw))}
		EnrichFailure(failure, string(raw))
		return ChatMetrics{}, failure
	}

	_outboundStream := _chatReq.Stream || !_useAPIKey
	writeProxyHeaders(_w, _provider, _model, _profile, _selectionMeta, _outboundStream)
	if _outboundStream {
		_w.WriteHeader(http.StatusOK)
		if err := flushResponse(_w); err != nil {
			return ChatMetrics{}, err
		}
		_idleReader := newStreamIdleTimeoutReader(resp.Body, providerStreamIdleTimeout(_provider))
		defer _idleReader.Stop()
		metrics, err := streamCodexResponsesAsChat(_w, _idleReader, _upstreamModel, _started)
		return metrics, retainStreamRetryAfter(err, resp.Header)
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32*1024*1024))
	if err != nil {
		return ChatMetrics{}, err
	}
	var completed codexCompleted
	if err := json.Unmarshal(raw, &completed); err != nil {
		return ChatMetrics{}, err
	}
	response := codexCompletedAsChat(completed, _upstreamModel)
	data, err := json.Marshal(response)
	if err != nil {
		return ChatMetrics{}, err
	}
	_w.Header().Set("Content-Type", "application/json")
	_w.WriteHeader(http.StatusOK)
	if _, err := _w.Write(data); err != nil {
		return ChatMetrics{}, err
	}
	metrics := responseMetrics(data)
	metrics.finalizeTiming(time.Since(_started))
	return metrics, nil
}

// -------------------------------------------------------------------------------------
func (_c *Client) forwardOpenAICodexResponses(_ctx context.Context, _w http.ResponseWriter, _srcReq *http.Request, _provider *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _rawBody []byte, _profile domain.RequestProfile, _selectionMeta balancer.SelectionMeta) (ChatMetrics, error) {
	return _c.forwardOpenAICodexResponsesRoute(_ctx, _w, _srcReq, _provider, _model, ResponsesProxyRoute{Method: http.MethodPost, Path: "/v1/responses"}, _rawBody, _profile, _selectionMeta)
}

// -------------------------------------------------------------------------------------
func (_c *Client) forwardOpenAICodexResponsesRoute(_ctx context.Context, _w http.ResponseWriter, _srcReq *http.Request, _provider *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _route ResponsesProxyRoute, _rawBody []byte, _profile domain.RequestProfile, _selectionMeta balancer.SelectionMeta) (ChatMetrics, error) {
	_started := time.Now()
	_apiKey := providerAPIKey(_provider)
	_useAPIKey := _apiKey != ""
	_authToken := strings.TrimSpace(_apiKey)
	_accountID := ""
	if _authToken == "" {
		auth, err := codexauth.EnsureContext(_ctx, _provider.Config.ID)
		if err != nil {
			return ChatMetrics{}, fmt.Errorf("openai codex oauth unavailable: %w", err)
		}
		_authToken = strings.TrimSpace(auth.AccessToken)
		_accountID = strings.TrimSpace(auth.AccountID)
	}
	_forceStream := !_useAPIKey && normalizeResponsesRoutePath(_route.Path) == "/v1/responses"
	_body, _stream, _contentType, _err := rewriteResponsesRouteRequestBody(_route, _rawBody, _srcReq.Header.Get("Content-Type"), codexUpstreamModelName(_model.Name), _forceStream)
	if _err != nil {
		return ChatMetrics{}, _err
	}
	_body, _err = applyCodexReasoningEffortToResponsesRoute(_route, _body, _contentType, _provider.Config)
	if _err != nil {
		return ChatMetrics{}, _err
	}

	_method := strings.ToUpper(strings.TrimSpace(_route.Method))
	if _method == "" {
		_method = http.MethodPost
	}
	_targetURL := codexResponsesRouteURL(*_provider.Config, _useAPIKey, _route)
	if err := security.ValidateOutboundURL(_targetURL); err != nil {
		return ChatMetrics{}, err
	}
	req, err := http.NewRequestWithContext(_ctx, _method, _targetURL, bytes.NewReader(_body))
	if err != nil {
		return ChatMetrics{}, err
	}
	req.Header.Set("Authorization", "Bearer "+_authToken)
	if strings.TrimSpace(_contentType) != "" {
		req.Header.Set("Content-Type", _contentType)
	}
	if _stream {
		req.Header.Set("Accept", "text/event-stream")
	} else if _accept := strings.TrimSpace(_srcReq.Header.Get("Accept")); _accept != "" {
		req.Header.Set("Accept", _accept)
	}
	applyCodexUpstreamHeaders(_srcReq, req, true)
	if _accountID != "" {
		req.Header.Set("chatgpt-account-id", _accountID)
	}

	client := _c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 0}
	}
	resp, err := doCodexHTTPRequest(client, req, _provider, _useAPIKey)
	if err != nil {
		return ChatMetrics{}, err
	}
	defer resp.Body.Close()
	_provider.RecordUsageHeaders(resp.Header)

	copyResponseHeaders(_w.Header(), resp.Header)
	writeProxyHeaders(_w, _provider, _model, _profile, _selectionMeta, _stream)
	_w.WriteHeader(resp.StatusCode)
	if _stream {
		if err := flushResponse(_w); err != nil {
			return ChatMetrics{}, err
		}
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		if _stream {
			_, _ = copyAndFlush(_w, resp.Body)
		} else {
			_, _ = io.Copy(_w, resp.Body)
		}
		return ChatMetrics{}, &ProviderStatusError{FailureDetails: FailureDetails{RetryAfter: retryAfterHeader(resp.Header)}, StatusCode: resp.StatusCode, ResponseForwarded: true}
	}

	if _stream {
		metrics, err := streamCopyWithProviderIdleTimeout(_w, resp.Body, _started, true, _c.responseRouteRecorder(_route, _provider, _model, _srcReq, _body), _provider, responsesStreamHeartbeat(), responsesRefusalTerminal, responsesStreamFailureTerminal)
		return metrics, retainStreamRetryAfter(err, resp.Header)
	}

	_respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return ChatMetrics{}, err
	}
	_c.recordResponseRouteFromBody(_route, _provider, _model, _srcReq, _body, _respBody)
	if _, err = _w.Write(_respBody); err != nil {
		return ChatMetrics{}, err
	}
	_metrics := responseMetrics(_respBody)
	_metrics.finalizeTiming(time.Since(_started))
	return _metrics, nil
}

// -------------------------------------------------------------------------------------
// Keep the original client identity. Compatibility defaults are only used when the
// caller did not provide Codex identity headers.
func applyCodexUpstreamHeaders(_srcReq *http.Request, _targetReq *http.Request, _preserveClientIdentity bool) {
	if _targetReq == nil {
		return
	}
	_targetReq.Header.Set("User-Agent", defaultCodexUpstreamUserAgent)
	_targetReq.Header.Set("Originator", defaultCodexUpstreamOriginator)
	_source := _srcReq
	if !_preserveClientIdentity && _srcReq != nil {
		_source = _srcReq.Clone(_srcReq.Context())
		_source.Header = _srcReq.Header.Clone()
		_source.Header.Del("User-Agent")
		_source.Header.Del("Originator")
	}
	copyProviderPassthroughHeaders(_source, _targetReq, "responses=experimental")
}

// -------------------------------------------------------------------------------------
func buildCodexResponsesRequest(_chatReq *domain.ChatCompletionRequest, _model string, _provider *domain.LLMProviderConfig) codexResponsesRequest {
	return buildCodexResponsesRequestWithDefaults(_chatReq, _model, _provider, true)
}

func buildCodexResponsesRequestWithDefaults(_chatReq *domain.ChatCompletionRequest, _model string, _provider *domain.LLMProviderConfig, _addInstructions bool) codexResponsesRequest {
	messages := []codexResponsesMessage{}
	instructions := []string{}
	hasImage := false
	for _, msg := range _chatReq.Messages {
		role := strings.ToLower(strings.TrimSpace(msg.Role))
		parts := codexContentPartsForRole(role, msg.Content)
		if codexPartsHaveImage(parts) {
			hasImage = true
		}
		switch role {
		case "system", "developer":
			if content := strings.TrimSpace(messageContentText(msg.Content)); content != "" {
				instructions = append(instructions, content)
			}
		case "assistant":
			if len(parts) > 0 {
				messages = append(messages, codexResponsesMessage{Type: "message", Role: "assistant", Content: parts})
			}
			for index, call := range msg.ToolCalls {
				name := strings.TrimSpace(call.Function.Name)
				if name == "" {
					continue
				}
				callID := strings.TrimSpace(call.ID)
				if callID == "" {
					callID = fmt.Sprintf("call_history_%d", index)
				}
				messages = append(messages, codexResponsesMessage{
					Type:   "function_call",
					CallID: callID,
					Name:   name,
					Args:   strings.TrimSpace(call.Function.Arguments),
				})
			}
		case "tool":
			callID := strings.TrimSpace(msg.ToolCallID)
			if callID != "" {
				// 工具結果即使為空字串，仍須送出必要的 output 欄位。
				output := messageContentText(msg.Content)
				messages = append(messages, codexResponsesMessage{
					Type:   "function_call_output",
					CallID: callID,
					Output: &output,
				})
			} else if len(parts) > 0 {
				messages = append(messages, codexResponsesMessage{Type: "message", Role: "user", Content: parts})
			}
		default:
			if len(parts) > 0 {
				messages = append(messages, codexResponsesMessage{Type: "message", Role: "user", Content: parts})
			}
		}
	}
	attachmentParts := codexImageContentPartsFromAttachments(_chatReq.Attachments)
	if len(attachmentParts) > 0 {
		hasImage = true
		messages = attachCodexPartsToLastUserMessage(messages, attachmentParts)
	}
	if len(messages) == 0 {
		messages = append(messages, codexResponsesMessage{Type: "message", Role: "user", Content: []codexResponsesContentPart{{Type: "input_text", Text: ""}}})
	}
	instructionText := strings.TrimSpace(strings.Join(instructions, "\n\n"))
	if _addInstructions && instructionText == "" {
		instructionText = "你是 OpenAI Codex 驅動的助理。請直接根據使用者輸入完成任務。"
	}
	if _addInstructions && hasImage {
		instructionText = strings.TrimSpace(instructionText + "\n\nCurrent user turn includes attached media. Analyze the media attached to the current user turn directly. Do not infer current media availability from previous assistant messages.")
	}
	return codexResponsesRequest{
		Model:        codexUpstreamModelName(_model),
		Instructions: instructionText,
		Input:        messages,
		Tools:        codexToolDefinitions(_chatReq.Tools),
		ToolChoice:   codexToolChoice(_chatReq.ToolChoice),
		Parallel:     _chatReq.ParallelToolCalls,
		Reasoning:    codexReasoningConfigForChatRequest(_chatReq, _provider),
		Include:      []string{"reasoning.encrypted_content"},
		PromptCache:  strings.TrimSpace(_chatReq.PromptCacheKey),
		Stream:       _chatReq.Stream,
		Store:        false,
	}
}

// -------------------------------------------------------------------------------------
// codexToolDefinitions 將 Chat Completions 的巢狀 function 格式攤平成 Responses 格式。
// 非 function 工具保持原狀，避免代理層擅自破壞後續新增的工具型別。
func codexToolDefinitions(_tools []interface{}) []interface{} {
	result := make([]interface{}, 0, len(_tools))
	for _, raw := range _tools {
		tool, ok := raw.(map[string]interface{})
		if !ok || !strings.EqualFold(strings.TrimSpace(stringFromAny(tool["type"])), "function") {
			result = append(result, raw)
			continue
		}
		function, nested := tool["function"].(map[string]interface{})
		if !nested {
			result = append(result, raw)
			continue
		}
		converted := map[string]interface{}{
			"type":       "function",
			"name":       stringFromAny(function["name"]),
			"parameters": function["parameters"],
		}
		if description := strings.TrimSpace(stringFromAny(function["description"])); description != "" {
			converted["description"] = description
		}
		if strict, exists := function["strict"]; exists {
			converted["strict"] = strict
		}
		result = append(result, converted)
	}
	return result
}

// -------------------------------------------------------------------------------------
func codexToolChoice(_choice interface{}) interface{} {
	choice, ok := _choice.(map[string]interface{})
	if !ok || !strings.EqualFold(strings.TrimSpace(stringFromAny(choice["type"])), "function") {
		return _choice
	}
	if function, nested := choice["function"].(map[string]interface{}); nested {
		return map[string]interface{}{"type": "function", "name": stringFromAny(function["name"])}
	}
	return _choice
}

// -------------------------------------------------------------------------------------
func codexUpstreamModelName(_model string) string {
	return strings.ToLower(strings.TrimSpace(_model))
}

// -------------------------------------------------------------------------------------
func applyCodexReasoningEffortToResponsesRoute(_route ResponsesProxyRoute, _body []byte, _contentType string, _provider *domain.LLMProviderConfig) ([]byte, error) {
	_method := strings.ToUpper(strings.TrimSpace(_route.Method))
	_path := normalizeResponsesRoutePath(_route.Path)
	if _method != http.MethodPost || (_path != "/v1/responses" && _path != "/v1/responses/input_tokens") {
		return _body, nil
	}
	if !strings.Contains(strings.ToLower(strings.TrimSpace(_contentType)), "application/json") {
		return _body, nil
	}
	_defaultReasoning := codexReasoningConfigForProvider(_provider)
	if _defaultReasoning == nil {
		return _body, nil
	}

	_payload := map[string]interface{}{}
	if len(strings.TrimSpace(string(_body))) > 0 {
		if _err := decodeJSONPreservingNumbers(_body, &_payload); _err != nil {
			return nil, _err
		}
	}
	_reasoningPayload, _ := _payload["reasoning"].(map[string]interface{})
	if _reasoningPayload == nil {
		_reasoningPayload = map[string]interface{}{}
	}
	if _, _exists := _reasoningPayload["effort"]; !_exists {
		_reasoningPayload["effort"] = _defaultReasoning.Effort
	}
	if _, _exists := _reasoningPayload["summary"]; !_exists {
		_reasoningPayload["summary"] = "auto"
	}
	_payload["reasoning"] = _reasoningPayload
	if _path == "/v1/responses" {
		_payload["store"] = false
		_payload["include"] = appendUniqueStringValue(_payload["include"], "reasoning.encrypted_content")
	}
	return json.Marshal(_payload)
}

// -------------------------------------------------------------------------------------
func appendUniqueStringValue(_value interface{}, _item string) []string {
	_items := []string{}
	_seen := map[string]bool{}
	_add := func(_text string) {
		_text = strings.TrimSpace(_text)
		if _text == "" || _seen[_text] {
			return
		}
		_seen[_text] = true
		_items = append(_items, _text)
	}
	switch _typed := _value.(type) {
	case []interface{}:
		for _, _entry := range _typed {
			_add(fmt.Sprint(_entry))
		}
	case []string:
		for _, _entry := range _typed {
			_add(_entry)
		}
	case string:
		_add(_typed)
	}
	_add(_item)
	return _items
}

// -------------------------------------------------------------------------------------
func codexReasoningConfigForProvider(_provider *domain.LLMProviderConfig) *codexReasoningConfig {
	_rawEffort := strings.TrimSpace(providerReasoningEffort(_provider))
	if _rawEffort == "" {
		_rawEffort = "high"
	}
	_effort := normalizeCodexReasoningEffort(_rawEffort)
	if _effort == "" {
		return nil
	}
	return &codexReasoningConfig{Effort: _effort, Summary: "auto"}
}

// -------------------------------------------------------------------------------------
func codexReasoningConfigForChatRequest(_chatReq *domain.ChatCompletionRequest, _provider *domain.LLMProviderConfig) *codexReasoningConfig {
	if _chatReq != nil {
		if _rawEffort := reasoningEffortFromMap(_chatReq.Reasoning); strings.TrimSpace(_rawEffort) != "" {
			_effort := normalizeCodexReasoningEffort(_rawEffort)
			return &codexReasoningConfig{Effort: _effort, Summary: "auto"}
		}
		if strings.TrimSpace(_chatReq.ReasoningEffort) != "" {
			_effort := normalizeCodexReasoningEffort(_chatReq.ReasoningEffort)
			return &codexReasoningConfig{Effort: _effort, Summary: "auto"}
		}
	}
	return codexReasoningConfigForProvider(_provider)
}

// -------------------------------------------------------------------------------------
func reasoningEffortFromMap(_reasoning map[string]interface{}) string {
	if len(_reasoning) == 0 {
		return ""
	}
	if _effort, _ok := _reasoning["effort"].(string); _ok {
		return _effort
	}
	return ""
}

// -------------------------------------------------------------------------------------
func providerReasoningEffort(_provider *domain.LLMProviderConfig) string {
	if _provider == nil {
		return "high"
	}
	return _provider.ReasoningEffort
}

// -------------------------------------------------------------------------------------
func normalizeCodexReasoningEffort(_effort string) string {
	switch strings.ToLower(strings.TrimSpace(_effort)) {
	case "":
		return ""
	case "none", "minimal", "low", "medium", "high", "xhigh":
		return strings.ToLower(strings.TrimSpace(_effort))
	default:
		return "high"
	}
}

// -------------------------------------------------------------------------------------
func codexContentParts(_content interface{}) []codexResponsesContentPart {
	return codexContentPartsForRole("user", _content)
}

// -------------------------------------------------------------------------------------
func codexContentPartsForRole(_role string, _content interface{}) []codexResponsesContentPart {
	return codexContentPartsWithTextType(_content, codexTextContentPartType(_role))
}

// -------------------------------------------------------------------------------------
func codexTextContentPartType(_role string) string {
	if strings.EqualFold(strings.TrimSpace(_role), "assistant") {
		return "output_text"
	}
	return "input_text"
}

// -------------------------------------------------------------------------------------
func codexContentPartsWithTextType(_content interface{}, _textType string) []codexResponsesContentPart {
	_parts := make([]codexResponsesContentPart, 0)
	switch _typed := _content.(type) {
	case string:
		if strings.TrimSpace(_typed) != "" {
			_parts = append(_parts, codexResponsesContentPart{Type: _textType, Text: _typed})
		}
	case []interface{}:
		for _, _item := range _typed {
			_parts = append(_parts, codexContentPartsWithTextType(_item, _textType)...)
		}
	case map[string]interface{}:
		_type := strings.ToLower(strings.TrimSpace(stringFromMap(_typed, "type")))
		switch {
		case _type == "text" || _type == "input_text" || _type == "output_text":
			if _text := stringFromMap(_typed, "text"); _text != "" {
				_parts = append(_parts, codexResponsesContentPart{Type: _textType, Text: _text})
			}
		case _type == "refusal" && _textType == "output_text":
			if _text := stringFromMap(_typed, "text"); _text != "" {
				_parts = append(_parts, codexResponsesContentPart{Type: "refusal", Text: _text})
			}
		case _type == "image_url" || _type == "input_image" || strings.Contains(_type, "image"):
			if _textType == "input_text" {
				if _url := imageURLFromContentPart(_typed); _url != "" {
					_parts = append(_parts, codexResponsesContentPart{Type: "input_image", ImageURL: _url, Detail: "auto"})
				}
			}
		default:
			if _text := stringFromMap(_typed, "text"); _text != "" {
				_parts = append(_parts, codexResponsesContentPart{Type: _textType, Text: _text})
			}
		}
	}
	return _parts
}

// -------------------------------------------------------------------------------------
func codexInputImageContentParts(_content interface{}) []codexResponsesContentPart {
	_parts := make([]codexResponsesContentPart, 0)
	switch _typed := _content.(type) {
	case []interface{}:
		for _, _item := range _typed {
			_parts = append(_parts, codexInputImageContentParts(_item)...)
		}
	case map[string]interface{}:
		_type := strings.ToLower(strings.TrimSpace(stringFromMap(_typed, "type")))
		if _type == "image_url" || _type == "input_image" || strings.Contains(_type, "image") {
			if _url := imageURLFromContentPart(_typed); _url != "" {
				_parts = append(_parts, codexResponsesContentPart{Type: "input_image", ImageURL: _url, Detail: "auto"})
			}
		}
	}
	return _parts
}

// -------------------------------------------------------------------------------------
func imageURLFromContentPart(_part map[string]interface{}) string {
	if _imageURL, _ok := _part["image_url"].(map[string]interface{}); _ok {
		return stringFromMap(_imageURL, "url")
	}
	if _imageURL, _ok := _part["imageUrl"].(map[string]interface{}); _ok {
		return stringFromMap(_imageURL, "url")
	}
	if _image, _ok := _part["image"].(map[string]interface{}); _ok {
		return stringFromMap(_image, "url")
	}
	if _url := stringFromMapAny(_part, "image_url", "imageUrl", "input_image", "inputImage"); strings.TrimSpace(_url) != "" {
		return _url
	}
	if _url := stringFromMap(_part, "image"); strings.TrimSpace(_url) != "" {
		return _url
	}
	return ""
}

// -------------------------------------------------------------------------------------
func codexImageContentPartsFromAttachments(_attachments []domain.ChatAttachment) []codexResponsesContentPart {
	_parts := make([]codexResponsesContentPart, 0, len(_attachments))
	for _, _attachment := range _attachments {
		_mediaType := strings.ToLower(strings.TrimSpace(_attachment.MediaType))
		_mimeType := strings.ToLower(strings.TrimSpace(_attachment.MIMEType))
		if _mediaType != "image" && !strings.HasPrefix(_mimeType, "image/") && !looksLikeImageFile(_attachment.Name) {
			continue
		}
		_raw := firstNonEmptyString(_attachment.FileData, _attachment.Content)
		_url := attachmentImageDataURL(map[string]interface{}{
			"name":      _attachment.Name,
			"mime_type": _attachment.MIMEType,
			"file_data": _raw,
		}, _raw)
		if _url == "" {
			continue
		}
		_parts = append(_parts, codexResponsesContentPart{Type: "input_image", ImageURL: _url, Detail: "auto"})
	}
	return _parts
}

// -------------------------------------------------------------------------------------
func attachCodexPartsToLastUserMessage(_messages []codexResponsesMessage, _parts []codexResponsesContentPart) []codexResponsesMessage {
	for _idx := len(_messages) - 1; _idx >= 0; _idx-- {
		if strings.EqualFold(strings.TrimSpace(_messages[_idx].Role), "user") {
			_messages[_idx].Content = append(_messages[_idx].Content, _parts...)
			return _messages
		}
	}
	return append(_messages, codexResponsesMessage{Type: "message", Role: "user", Content: _parts})
}

// -------------------------------------------------------------------------------------
func codexPartsHaveImage(_parts []codexResponsesContentPart) bool {
	for _, _part := range _parts {
		if strings.EqualFold(_part.Type, "input_image") {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func codexResponsesURL(_provider domain.LLMProviderConfig, _useAPIKey bool) string {
	_base := strings.TrimRight(strings.TrimSpace(_provider.BaseURL), "/")
	_path := strings.TrimSpace(_provider.ChatCompletionsPath)

	if !_useAPIKey && (_base == "" || strings.Contains(strings.ToLower(_base), "api.openai.com") || strings.EqualFold(_path, "/v1/responses")) {
		return defaultOpenAICodexOAuthResponsesURL
	}
	if _useAPIKey && (_base == "" || strings.Contains(strings.ToLower(_base), "chatgpt.com") || strings.EqualFold(_path, "/backend-api/codex/responses")) {
		return defaultOpenAICodexAPIResponsesURL
	}
	if _base == "" {
		if _useAPIKey {
			return defaultOpenAICodexAPIResponsesURL
		}
		return defaultOpenAICodexOAuthResponsesURL
	}
	return _provider.ChatURL()
}

// -------------------------------------------------------------------------------------
func codexResponsesRouteURL(_provider domain.LLMProviderConfig, _useAPIKey bool, _route ResponsesProxyRoute) string {
	_baseURL := codexResponsesURL(_provider, _useAPIKey)
	_path := normalizeResponsesRoutePath(_route.Path)
	_suffix := strings.TrimPrefix(_path, "/v1/responses")
	return appendQuery(appendPathBeforeQuery(_baseURL, _suffix), _route.Query)
}

// -------------------------------------------------------------------------------------
func appendPathBeforeQuery(_baseURL string, _suffix string) string {
	_baseURL = strings.TrimSpace(_baseURL)
	_suffix = strings.TrimSpace(_suffix)
	if _suffix == "" {
		return _baseURL
	}
	_query := ""
	if _idx := strings.Index(_baseURL, "?"); _idx >= 0 {
		_query = _baseURL[_idx:]
		_baseURL = _baseURL[:_idx]
	}
	return strings.TrimRight(_baseURL, "/") + _suffix + _query
}

// -------------------------------------------------------------------------------------
func normalizeResponsesRoutePath(_path string) string {
	_path = strings.TrimRight(strings.TrimSpace(_path), "/")
	if _path == "" {
		return "/v1/responses"
	}
	if strings.HasPrefix(_path, "/api/v1/") {
		_path = strings.TrimPrefix(_path, "/api")
	}
	if !strings.HasPrefix(_path, "/") {
		_path = "/" + _path
	}
	return _path
}

// -------------------------------------------------------------------------------------
func codexCompletedAsChat(_completed codexCompleted, _fallbackModel string) map[string]interface{} {
	model := firstNonEmpty(_completed.Model, _fallbackModel)
	content := codexCompletedText(_completed)
	toolCalls := codexCompletedToolCalls(_completed)
	finishReason := "stop"
	message := map[string]interface{}{
		"role":    "assistant",
		"content": content,
	}
	if len(toolCalls) > 0 {
		finishReason = "tool_calls"
		message["tool_calls"] = toolCalls
		if content == "" {
			message["content"] = nil
		}
	}
	return map[string]interface{}{
		"id":      firstNonEmpty(_completed.ID, fmt.Sprintf("codex-%d", time.Now().UnixNano())),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []map[string]interface{}{
			{
				"index":         0,
				"finish_reason": finishReason,
				"message":       message,
			},
		},
		"usage": codexUsageAsChat(_completed.Usage),
	}
}

// -------------------------------------------------------------------------------------
func codexCompletedToolCalls(_completed codexCompleted) []map[string]interface{} {
	result := []map[string]interface{}{}
	for _, output := range _completed.Output {
		if !strings.EqualFold(strings.TrimSpace(output.Type), "function_call") || strings.TrimSpace(output.Name) == "" {
			continue
		}
		callID := firstNonEmpty(output.CallID, output.ID)
		if callID == "" {
			callID = fmt.Sprintf("call_codex_%d", len(result))
		}
		result = append(result, map[string]interface{}{
			"index": len(result),
			"id":    callID,
			"type":  "function",
			"function": map[string]interface{}{
				"name":      strings.TrimSpace(output.Name),
				"arguments": output.Arguments,
			},
		})
	}
	return result
}

// -------------------------------------------------------------------------------------
func codexCompletedText(_completed codexCompleted) string {
	var builder strings.Builder
	for _, output := range _completed.Output {
		if output.Type != "message" {
			continue
		}
		for _, part := range output.Content {
			if part.Type == "output_text" && part.Text != "" {
				builder.WriteString(part.Text)
			}
		}
	}
	return builder.String()
}

// -------------------------------------------------------------------------------------
func codexUsageAsChat(_usage codexUsage) map[string]interface{} {
	return map[string]interface{}{
		"prompt_tokens":     _usage.InputTokens,
		"completion_tokens": _usage.OutputTokens,
		"total_tokens":      _usage.TotalTokens,
		"completion_tokens_details": map[string]interface{}{
			"reasoning_tokens": _usage.OutputTokensDetails["reasoning_tokens"],
		},
	}
}

// -------------------------------------------------------------------------------------
type codexChatStreamState struct {
	indexes      map[string]int
	emittedCalls map[string]bool
	arguments    map[string]string
	hasToolCalls bool
}

// -------------------------------------------------------------------------------------
func streamCodexResponsesAsChat(_w http.ResponseWriter, _body io.Reader, _model string, _started time.Time) (ChatMetrics, error) {
	metrics := ChatMetrics{}
	state := &codexChatStreamState{
		indexes:      map[string]int{},
		emittedCalls: map[string]bool{},
		arguments:    map[string]string{},
	}

	_buffer := make([]byte, 32*1024)
	_pending := ""
	_processEvent := func(_event string) (bool, error) {
		markStreamActivity(_body, _event)
		_name, _payload := codexSSEEventNameAndPayload(_event)
		if _payload == "" {
			return false, nil
		}
		if _payload == "[DONE]" {
			return true, nil
		}
		return writeCodexEventAsChat(_w, _name, _payload, _model, _started, &metrics, state)
	}

	for {
		_count, _err := _body.Read(_buffer)
		if _count > 0 {
			_pending += string(_buffer[:_count])
			for {
				_event, _remaining, _ok := nextSSEEvent(_pending)
				if !_ok {
					_pending = _remaining
					break
				}
				_pending = _remaining
				_terminal, _processErr := _processEvent(_event)
				if _processErr != nil {
					return metrics, _processErr
				}
				if _terminal {
					if _doneErr := writeOpenAIStreamDone(_w); _doneErr != nil {
						return metrics, _doneErr
					}
					metrics.finalizeTiming(time.Since(_started))
					metrics.finalizeClientDelivery()
					return metrics, nil
				}
			}
		}

		if _err != nil {
			if _err != io.EOF {
				return metrics, _err
			}
			break
		}
	}

	if strings.TrimSpace(_pending) != "" {
		_terminal, _err := _processEvent(_pending)
		if _err != nil {
			return metrics, _err
		}
		if _terminal {
			if _doneErr := writeOpenAIStreamDone(_w); _doneErr != nil {
				return metrics, _doneErr
			}
			metrics.finalizeTiming(time.Since(_started))
			metrics.finalizeClientDelivery()
			return metrics, nil
		}
	}
	if err := writeOpenAIStreamDone(_w); err != nil {
		return metrics, err
	}
	metrics.finalizeTiming(time.Since(_started))
	metrics.finalizeClientDelivery()
	return metrics, nil
}

// -------------------------------------------------------------------------------------
func codexSSEEventNameAndPayload(_event string) (string, string) {
	_eventName := ""
	_dataLines := make([]string, 0, 1)
	for _, _line := range strings.Split(strings.ReplaceAll(_event, "\r\n", "\n"), "\n") {
		_line = strings.TrimRight(_line, "\r")
		if strings.TrimSpace(_line) == "" || strings.HasPrefix(_line, ":") {
			continue
		}
		if strings.HasPrefix(_line, "event:") {
			_eventName = strings.TrimSpace(strings.TrimPrefix(_line, "event:"))
			continue
		}
		if strings.HasPrefix(_line, "data:") {
			_data := strings.TrimPrefix(_line, "data:")
			_data = strings.TrimPrefix(_data, " ")
			_dataLines = append(_dataLines, _data)
		}
	}
	return strings.TrimSpace(_eventName), strings.TrimSpace(strings.Join(_dataLines, "\n"))
}

// -------------------------------------------------------------------------------------
func writeCodexEventAsChat(_w http.ResponseWriter, _eventName string, _payload string, _model string, _started time.Time, _metrics *ChatMetrics, _state *codexChatStreamState) (bool, error) {
	var item map[string]interface{}
	if err := json.Unmarshal([]byte(_payload), &item); err != nil {
		return false, err
	}
	eventType := strings.TrimSpace(firstNonEmpty(stringFromAny(item["type"]), _eventName))
	switch eventType {
	case "response.output_text.delta":
		return false, writeCodexDelta(_w, _model, "content", stringFromAny(item["delta"]), _started, _metrics)
	case "response.reasoning_text.delta", "response.reasoning_summary_text.delta":
		return false, writeCodexDelta(_w, _model, "reasoning_content", stringFromAny(item["delta"]), _started, _metrics)
	case "response.output_item.added", "response.output_item.done":
		output, _ := item["item"].(map[string]interface{})
		if !strings.EqualFold(strings.TrimSpace(stringFromAny(output["type"])), "function_call") {
			return false, nil
		}
		return false, writeCodexFunctionItemAsChat(_w, _model, item, output, _started, _metrics, _state)
	case "response.function_call_arguments.delta":
		key := codexStreamCallKey(item, nil)
		index := codexStreamCallIndex(item, key, _state)
		delta := stringFromAny(item["delta"])
		if delta == "" {
			return false, nil
		}
		_state.hasToolCalls = true
		_state.arguments[key] += delta
		return false, writeCodexToolCallDelta(_w, _model, index, "", "", delta, _started, _metrics)
	case "response.function_call_arguments.done":
		key := codexStreamCallKey(item, nil)
		index := codexStreamCallIndex(item, key, _state)
		return false, writeCodexRemainingArguments(_w, _model, index, key, stringFromAny(item["arguments"]), _started, _metrics, _state)
	case "response.completed":
		if raw := item["response"]; raw != nil {
			data, _ := json.Marshal(raw)
			var completed codexCompleted
			if err := json.Unmarshal(data, &completed); err == nil {
				for _, call := range codexCompletedToolCalls(completed) {
					callID := stringFromAny(call["id"])
					if _state.emittedCalls[callID] {
						continue
					}
					function, _ := call["function"].(map[string]interface{})
					if err := writeCodexToolCallDelta(_w, _model, numberFieldOrZero(call, "index"), callID, stringFromAny(function["name"]), stringFromAny(function["arguments"]), _started, _metrics); err != nil {
						return false, err
					}
					_state.emittedCalls[callID] = true
					_state.hasToolCalls = true
				}
				finishReason := "stop"
				if _state.hasToolCalls {
					finishReason = "tool_calls"
				}
				chunk := map[string]interface{}{
					"choices": []map[string]interface{}{
						{
							"index":         0,
							"delta":         map[string]interface{}{},
							"finish_reason": finishReason,
						},
					},
					"model": firstNonEmpty(completed.Model, _model),
					"usage": codexUsageAsChat(completed.Usage),
				}
				return true, writeOpenAIStreamChunk(_w, chunk, false, _started, _metrics)
			}
		}
		return true, nil
	case "response.failed", "response.incomplete", "error":
		if msg := codexEventErrorMessage(item); msg != "" {
			return false, fmt.Errorf("openai codex stream failed: %s", msg)
		}
	}
	return false, nil
}

// -------------------------------------------------------------------------------------
func writeCodexFunctionItemAsChat(_w http.ResponseWriter, _model string, _event map[string]interface{}, _item map[string]interface{}, _started time.Time, _metrics *ChatMetrics, _state *codexChatStreamState) error {
	key := codexStreamCallKey(_event, _item)
	index := codexStreamCallIndex(_event, key, _state)
	callID := firstNonEmpty(stringFromAny(_item["call_id"]), stringFromAny(_item["id"]), key)
	name := strings.TrimSpace(stringFromAny(_item["name"]))
	if !_state.emittedCalls[key] && !_state.emittedCalls[callID] {
		if err := writeCodexToolCallDelta(_w, _model, index, callID, name, "", _started, _metrics); err != nil {
			return err
		}
		_state.emittedCalls[key] = true
		_state.emittedCalls[callID] = true
	}
	_state.hasToolCalls = true
	return writeCodexRemainingArguments(_w, _model, index, key, stringFromAny(_item["arguments"]), _started, _metrics, _state)
}

// -------------------------------------------------------------------------------------
func codexStreamCallKey(_event map[string]interface{}, _item map[string]interface{}) string {
	if _item != nil {
		if value := firstNonEmpty(stringFromAny(_item["id"]), stringFromAny(_item["call_id"])); value != "" {
			return value
		}
	}
	return firstNonEmpty(stringFromAny(_event["item_id"]), stringFromAny(_event["call_id"]), fmt.Sprintf("output_%d", numberFieldOrZero(_event, "output_index")))
}

// -------------------------------------------------------------------------------------
func codexStreamCallIndex(_event map[string]interface{}, _key string, _state *codexChatStreamState) int {
	if index, exists := _state.indexes[_key]; exists {
		return index
	}
	index := numberFieldOrZero(_event, "output_index")
	_state.indexes[_key] = index
	return index
}

// -------------------------------------------------------------------------------------
func writeCodexRemainingArguments(_w http.ResponseWriter, _model string, _index int, _key string, _arguments string, _started time.Time, _metrics *ChatMetrics, _state *codexChatStreamState) error {
	if _arguments == "" {
		return nil
	}
	emitted := _state.arguments[_key]
	if emitted == _arguments {
		return nil
	}
	remaining := strings.TrimPrefix(_arguments, emitted)
	if emitted != "" && remaining == _arguments {
		return nil
	}
	_state.arguments[_key] = emitted + remaining
	return writeCodexToolCallDelta(_w, _model, _index, "", "", remaining, _started, _metrics)
}

// -------------------------------------------------------------------------------------
func writeCodexToolCallDelta(_w http.ResponseWriter, _model string, _index int, _callID string, _name string, _arguments string, _started time.Time, _metrics *ChatMetrics) error {
	function := map[string]interface{}{"arguments": _arguments}
	if _name != "" {
		function["name"] = _name
	}
	call := map[string]interface{}{
		"index":    _index,
		"function": function,
	}
	if _callID != "" {
		call["id"] = _callID
		call["type"] = "function"
	}
	chunk := map[string]interface{}{
		"model": _model,
		"choices": []map[string]interface{}{{
			"index": 0,
			"delta": map[string]interface{}{"tool_calls": []map[string]interface{}{call}},
		}},
	}
	return writeOpenAIStreamChunk(_w, chunk, true, _started, _metrics)
}

// -------------------------------------------------------------------------------------
func writeCodexDelta(_w http.ResponseWriter, _model string, _field string, _delta string, _started time.Time, _metrics *ChatMetrics) error {
	if _delta == "" {
		return nil
	}
	chunk := map[string]interface{}{
		"model": _model,
		"choices": []map[string]interface{}{
			{
				"index": 0,
				"delta": map[string]interface{}{_field: _delta},
			},
		},
	}
	return writeOpenAIStreamChunk(_w, chunk, true, _started, _metrics)
}

// -------------------------------------------------------------------------------------
func writeOpenAIStreamChunk(_w http.ResponseWriter, _chunk map[string]interface{}, _content bool, _started time.Time, _metrics *ChatMetrics) error {
	data, err := json.Marshal(_chunk)
	if err != nil {
		return err
	}
	line := "data: " + string(data) + "\n\n"
	if _, err := _w.Write([]byte(line)); err != nil {
		return DownstreamError(err)
	}
	if err := flushResponse(_w); err != nil {
		return err
	}
	eventMetrics := streamEventMetrics(line)
	if _metrics.FirstResponseMS <= 0 && eventMetrics.ContentSeen {
		eventMetrics.FirstResponseMS = durationMilliseconds(time.Since(_started))
	}
	_metrics.merge(eventMetrics)
	if _content && eventMetrics.ContentSeen {
		_metrics.recordClientContentWrite(time.Since(_started))
	}
	return nil
}

// -------------------------------------------------------------------------------------
func writeOpenAIStreamDone(_w http.ResponseWriter) error {
	if _, err := _w.Write([]byte("data: [DONE]\n\n")); err != nil {
		return DownstreamError(err)
	}
	return flushResponse(_w)
}

// -------------------------------------------------------------------------------------
func codexEventErrorMessage(_item map[string]interface{}) string {
	for _, key := range []string{"error", "message"} {
		switch value := _item[key].(type) {
		case string:
			if strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value)
			}
		case map[string]interface{}:
			if msg := strings.TrimSpace(stringFromAny(value["message"])); msg != "" {
				return msg
			}
		}
	}
	return ""
}

// -------------------------------------------------------------------------------------
func stringFromAny(_value interface{}) string {
	switch typed := _value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	case nil:
		return ""
	default:
		return fmt.Sprint(typed)
	}
}

// -------------------------------------------------------------------------------------
func firstNonEmpty(_values ...string) string {
	for _, value := range _values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
