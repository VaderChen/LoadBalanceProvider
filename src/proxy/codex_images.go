package proxy

import (
	"bufio"
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

const (
	defaultCodexImageToolModel = "gpt-image-2"
	maxCodexImageResponseSize  = 128 * 1024 * 1024
)

// -------------------------------------------------------------------------------------
type openAIImageGenerationRequest struct {
	InputImages       []map[string]interface{} `json:"-"`
	Mask              map[string]interface{}   `json:"-"`
	Prompt            string                   `json:"prompt"`
	Model             string                   `json:"model,omitempty"`
	N                 int                      `json:"n,omitempty"`
	Size              string                   `json:"size,omitempty"`
	Quality           string                   `json:"quality,omitempty"`
	Background        string                   `json:"background,omitempty"`
	OutputFormat      string                   `json:"output_format,omitempty"`
	OutputCompression *int                     `json:"output_compression,omitempty"`
	Moderation        string                   `json:"moderation,omitempty"`
	ResponseFormat    string                   `json:"response_format,omitempty"`
}

// -------------------------------------------------------------------------------------
type codexImageResult struct {
	Base64        string
	RevisedPrompt string
	OutputFormat  string
	Size          string
	Width         int
	Height        int
	Bytes         int
}

// -------------------------------------------------------------------------------------
// 圖片雖以 JSON 交付，下游等待期間仍由上游 SSE 活動計時管理。
func UsesCodexImageStream(provider *balancer.ProviderRuntime, request *http.Request) bool {
	return isOpenAICodexProvider(provider) && (isOpenAIImageGenerationRoute(request) || isOpenAIImageEditRoute(request))
}

// -------------------------------------------------------------------------------------
func isOpenAIImageGenerationRoute(_request *http.Request) bool {
	if _request == nil || _request.URL == nil {
		return false
	}
	_path := strings.TrimRight(strings.TrimSpace(_request.URL.Path), "/")
	return strings.EqualFold(_path, "/v1/images/generations") || strings.EqualFold(_path, "/api/v1/images/generations")
}

// -------------------------------------------------------------------------------------
func (_c *Client) forwardOpenAICodexImageGeneration(_ctx context.Context, _w http.ResponseWriter, _srcReq *http.Request, _provider *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _rawBody []byte, _profile domain.RequestProfile, _selectionMeta balancer.SelectionMeta) error {
	var _imageRequest openAIImageGenerationRequest
	var _err error
	if isOpenAIImageEditRoute(_srcReq) {
		_imageRequest, _err = decodeCodexImageEdit(_rawBody, _srcReq.Header.Get("Content-Type"))
	} else {
		_imageRequest, _err = decodeOpenAIImageGenerationRequest(_rawBody)
	}
	if _err != nil {
		return &ProviderStatusError{StatusCode: http.StatusBadRequest, Message: _err.Error(), FailureDetails: FailureDetails{Code: "invalid_request_error"}}
	}

	_authToken, _accountID, _useAPIKey, _err := codexImageAuthorization(_provider)
	if _err != nil {
		return _err
	}
	_mainModel := codexImageMainModel(_provider)
	if _mainModel == "" {
		return &ProviderStatusError{StatusCode: http.StatusBadRequest, Message: "Provider 預設模型必須是有效的 Responses 主模型，不可為 AUTO 或圖片專用模型", FailureDetails: FailureDetails{Code: "invalid_request_error"}}
	}
	_targetURL := codexResponsesURL(*_provider.Config, _useAPIKey)
	if _err := security.ValidateOutboundURL(_targetURL); _err != nil {
		return _err
	}

	_results := make([]codexImageResult, 0, _imageRequest.N)
	_createdAt := time.Now().Unix()
	for _idx := 0; _idx < _imageRequest.N; _idx++ {
		_result, _created, _requestErr := _c.requestCodexGeneratedImage(_ctx, _srcReq, _provider, _targetURL, _authToken, _accountID, _mainModel, _imageRequest)
		if _requestErr != nil {
			return _requestErr
		}
		if _created > 0 {
			_createdAt = _created
		}
		_results = append(_results, _result)
	}

	_response, _err := buildOpenAIImageGenerationResponse(_results, _createdAt, _imageRequest.ResponseFormat)
	if _err != nil {
		return _err
	}
	writeProxyHeaders(_w, _provider, _model, _profile, _selectionMeta, false)
	_w.Header().Set("Content-Type", "application/json")
	_w.WriteHeader(http.StatusOK)
	_, _err = _w.Write(_response)
	return _err
}

// -------------------------------------------------------------------------------------
func decodeOpenAIImageGenerationRequest(_rawBody []byte) (openAIImageGenerationRequest, error) {
	_request := openAIImageGenerationRequest{}
	if _err := json.Unmarshal(_rawBody, &_request); _err != nil {
		return _request, fmt.Errorf("invalid image generation request: %w", _err)
	}
	_request.Prompt = strings.TrimSpace(_request.Prompt)
	if _request.Prompt == "" {
		return _request, fmt.Errorf("invalid image generation request: prompt is required")
	}
	if _request.N <= 0 {
		_request.N = 1
	}
	if _request.N > 10 {
		return _request, fmt.Errorf("invalid image generation request: n must not exceed 10")
	}
	if _err := normalizeCodexImageParameters(&_request); _err != nil {
		return _request, _err
	}
	_request.ResponseFormat = strings.ToLower(strings.TrimSpace(_request.ResponseFormat))
	if _request.ResponseFormat == "" {
		_request.ResponseFormat = "b64_json"
	}
	if _request.ResponseFormat != "b64_json" && _request.ResponseFormat != "url" {
		return _request, fmt.Errorf("invalid image generation request: response_format must be b64_json or url")
	}
	return _request, nil
}

// -------------------------------------------------------------------------------------
func normalizeCodexImageToolModel(_model string) string {
	_model = strings.ToLower(strings.TrimSpace(_model))
	if _model == "" || strings.EqualFold(_model, "auto") {
		return defaultCodexImageToolModel
	}
	switch _model {
	case "gpt-image-2-2k", "gpt-image-2-4k":
		return defaultCodexImageToolModel
	default:
		return _model
	}
}

// -------------------------------------------------------------------------------------
func codexImageAuthorization(_provider *balancer.ProviderRuntime) (string, string, bool, error) {
	_apiKey := strings.TrimSpace(providerAPIKey(_provider))
	if _apiKey != "" {
		return _apiKey, "", true, nil
	}
	if _provider == nil || _provider.Config == nil {
		return "", "", false, fmt.Errorf("openai codex provider is not configured")
	}
	_auth, _err := codexauth.Ensure(_provider.Config.ID)
	if _err != nil {
		return "", "", false, fmt.Errorf("openai codex oauth unavailable: %w", _err)
	}
	_token := strings.TrimSpace(_auth.AccessToken)
	if _token == "" {
		return "", "", false, fmt.Errorf("openai codex oauth access token is empty")
	}
	return _token, strings.TrimSpace(_auth.AccountID), false, nil
}

// -------------------------------------------------------------------------------------
func codexImageMainModel(_provider *balancer.ProviderRuntime) string {
	if _provider == nil || _provider.Config == nil || len(_provider.Config.Models) == 0 {
		return ""
	}
	_name := codexUpstreamModelName(_provider.Config.Models[0].Name)
	if strings.EqualFold(_name, "auto") || isCodexImageOnlyModel(_name) {
		return ""
	}
	return _name
}

// -------------------------------------------------------------------------------------
func isCodexImageOnlyModel(_model string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(_model)), "gpt-image-")
}

// -------------------------------------------------------------------------------------
func buildCodexImageResponsesRequest(_mainModel string, _request openAIImageGenerationRequest) ([]byte, error) {
	_tool := map[string]interface{}{
		"type":   "image_generation",
		"action": "generate",
		"model":  _request.Model,
	}
	content := []interface{}{map[string]interface{}{"type": "input_text", "text": _request.Prompt}}
	if len(_request.InputImages) > 0 {
		_tool["action"] = "edit"
		for _, image := range _request.InputImages {
			content = append(content, image)
		}
		if _request.Mask != nil {
			_tool["input_image_mask"] = _request.Mask
		}
	}
	for _name, _value := range map[string]string{
		"size":          _request.Size,
		"quality":       _request.Quality,
		"background":    _request.Background,
		"output_format": _request.OutputFormat,
		"moderation":    _request.Moderation,
	} {
		if _value = strings.TrimSpace(_value); _value != "" {
			_tool[_name] = _value
		}
	}
	if _request.OutputCompression != nil {
		_tool["output_compression"] = *_request.OutputCompression
	}

	_payload := map[string]interface{}{
		"model":               codexUpstreamModelName(_mainModel),
		"instructions":        "",
		"input":               []interface{}{map[string]interface{}{"type": "message", "role": "user", "content": content}},
		"tools":               []interface{}{_tool},
		"tool_choice":         map[string]interface{}{"type": "image_generation"},
		"parallel_tool_calls": true,
		"reasoning":           map[string]interface{}{"effort": "medium", "summary": "auto"},
		"include":             []string{"reasoning.encrypted_content"},
		"stream":              true,
		"store":               false,
	}
	return json.Marshal(_payload)
}

// -------------------------------------------------------------------------------------
func (_c *Client) requestCodexGeneratedImage(_ctx context.Context, _srcReq *http.Request, _provider *balancer.ProviderRuntime, _targetURL string, _authToken string, _accountID string, _mainModel string, _imageRequest openAIImageGenerationRequest) (codexImageResult, int64, error) {
	_body, _err := buildCodexImageResponsesRequest(_mainModel, _imageRequest)
	if _err != nil {
		return codexImageResult{}, 0, _err
	}
	_request, _err := http.NewRequestWithContext(_ctx, http.MethodPost, _targetURL, bytes.NewReader(_body))
	if _err != nil {
		return codexImageResult{}, 0, _err
	}
	_request.Header.Set("Authorization", "Bearer "+_authToken)
	_request.Header.Set("Content-Type", "application/json")
	_request.Header.Set("Accept", "text/event-stream")
	applyCodexUpstreamHeaders(_srcReq, _request, false)
	if _accountID != "" {
		_request.Header.Set("chatgpt-account-id", _accountID)
	}

	_client := _c.HTTPClient
	if _client == nil {
		_client = &http.Client{Timeout: 0}
	}
	if err := _provider.Config.CheckScheduledDowntime(); err != nil {
		return codexImageResult{}, 0, err
	}
	_response, _err := doProviderHTTPRequest(_client, _request, providerStreamIdleTimeout(_provider), _provider.Config)
	if _err != nil {
		return codexImageResult{}, 0, _err
	}
	defer _response.Body.Close()
	_provider.RecordUsageHeaders(_response.Header)
	if _response.StatusCode < http.StatusOK || _response.StatusCode >= http.StatusMultipleChoices {
		_raw, _ := io.ReadAll(io.LimitReader(_response.Body, 1024*1024))
		status := &ProviderStatusError{StatusCode: _response.StatusCode, Message: strings.TrimSpace(string(_raw)), FailureDetails: FailureDetails{RetryAfter: retryAfterHeader(_response.Header)}}
		EnrichFailure(status, string(_raw))
		return codexImageResult{}, 0, status
	}
	idle := newStreamIdleTimeoutReader(_response.Body, providerStreamIdleTimeout(_provider))
	defer idle.Stop()
	result, created, err := readCodexGeneratedImage(idle)
	return result, created, retainStreamRetryAfter(err, _response.Header)
}

// -------------------------------------------------------------------------------------
func readCodexGeneratedImage(_reader io.Reader) (codexImageResult, int64, error) {
	_buffered := bufio.NewReaderSize(io.LimitReader(_reader, maxCodexImageResponseSize), 64*1024)
	_pending := codexImageResult{}
	_createdAt := int64(0)
	var _event strings.Builder

	_consume := func() (bool, error) {
		rawEvent := _event.String()
		markStreamActivity(_reader, rawEvent)
		_eventName, _payloadText := codexSSEEventNameAndPayload(rawEvent)
		_event.Reset()
		if _payloadText == "" || _payloadText == "[DONE]" {
			return false, nil
		}
		_payload := map[string]interface{}{}
		if _err := json.Unmarshal([]byte(_payloadText), &_payload); _err != nil {
			return false, fmt.Errorf("invalid openai codex image event %q: %w", _eventName, _err)
		}
		_type := strings.TrimSpace(firstNonEmpty(stringFromAny(_payload["type"]), _eventName))
		switch _type {
		case "response.output_item.done":
			if _item, _ok := _payload["item"].(map[string]interface{}); _ok {
				if _result, _ok := codexImageResultFromOutputItem(_item); _ok {
					_pending = _result
				}
			}
		case "response.image_generation_call.partial_image":
			// OpenAI Images 的非串流回應只接受最終 image_generation_call.result；
			// partial image 不可冒充最終成品。
		case "response.completed":
			if _response, _ok := _payload["response"].(map[string]interface{}); _ok {
				if _created := int64FromAny(_response["created_at"]); _created > 0 {
					_createdAt = _created
				}
				if _result, _ok := codexImageResultFromResponse(_response); _ok {
					_pending = _result
				}
			}
			if strings.TrimSpace(_pending.Base64) == "" {
				return true, fmt.Errorf("openai codex image generation completed without image output")
			}
			return true, nil
		case "response.failed", "response.incomplete", "error":
			_message := codexImageEventErrorMessage(_payload)
			if _message == "" {
				_message = "upstream image generation failed"
			}
			return true, &ProviderStreamError{Message: _message, FailureDetails: failureDetailsFromEvent(rawEvent)}
		}
		return false, nil
	}

	for {
		_line, _readErr := _buffered.ReadString('\n')
		if _line != "" {
			_event.WriteString(_line)
			if _line == "\n" || _line == "\r\n" {
				_done, _consumeErr := _consume()
				if _consumeErr != nil {
					return codexImageResult{}, _createdAt, _consumeErr
				}
				if _done {
					return _pending, _createdAt, nil
				}
			}
		}
		if _readErr != nil {
			if _readErr != io.EOF {
				return codexImageResult{}, _createdAt, _readErr
			}
			if _event.Len() > 0 {
				_done, _consumeErr := _consume()
				if _consumeErr != nil {
					return codexImageResult{}, _createdAt, _consumeErr
				}
				if _done && strings.TrimSpace(_pending.Base64) != "" {
					return _pending, _createdAt, nil
				}
			}
			return codexImageResult{}, _createdAt, &ProviderStreamError{Message: "openai codex image stream ended before response.completed", TruncatedStream: true}
		}
	}
}

// -------------------------------------------------------------------------------------
func codexImageEventErrorMessage(_payload map[string]interface{}) string {
	if _response, _ok := _payload["response"].(map[string]interface{}); _ok {
		if _error, _ok := _response["error"].(map[string]interface{}); _ok {
			if _message := strings.TrimSpace(stringFromAny(_error["message"])); _message != "" {
				return _message
			}
		}
	}
	return codexEventErrorMessage(_payload)
}

// -------------------------------------------------------------------------------------
func codexImageResultFromResponse(_response map[string]interface{}) (codexImageResult, bool) {
	_output, _ := _response["output"].([]interface{})
	for _, _rawItem := range _output {
		if _item, _ok := _rawItem.(map[string]interface{}); _ok {
			if _result, _ok := codexImageResultFromOutputItem(_item); _ok {
				return _result, true
			}
		}
	}
	return codexImageResult{}, false
}

// -------------------------------------------------------------------------------------
func codexImageResultFromOutputItem(_item map[string]interface{}) (codexImageResult, bool) {
	if !strings.EqualFold(strings.TrimSpace(stringFromAny(_item["type"])), "image_generation_call") {
		return codexImageResult{}, false
	}
	_base64 := strings.TrimSpace(stringFromAny(_item["result"]))
	if _base64 == "" {
		return codexImageResult{}, false
	}
	return codexImageResult{
		Base64:        _base64,
		RevisedPrompt: strings.TrimSpace(stringFromAny(_item["revised_prompt"])),
		OutputFormat:  strings.TrimSpace(stringFromAny(_item["output_format"])),
		Size:          strings.TrimSpace(stringFromAny(_item["size"])),
		Width:         int(int64FromAny(_item["width"])),
		Height:        int(int64FromAny(_item["height"])),
		Bytes:         int(int64FromAny(_item["bytes"])),
	}, true
}

// -------------------------------------------------------------------------------------
func int64FromAny(_value interface{}) int64 {
	switch _typed := _value.(type) {
	case float64:
		return int64(_typed)
	case json.Number:
		_value, _ := _typed.Int64()
		return _value
	case int64:
		return _typed
	case int:
		return int64(_typed)
	default:
		return 0
	}
}

// -------------------------------------------------------------------------------------
func buildOpenAIImageGenerationResponse(_results []codexImageResult, _createdAt int64, _responseFormat string) ([]byte, error) {
	if _createdAt <= 0 {
		_createdAt = time.Now().Unix()
	}
	_data := make([]interface{}, 0, len(_results))
	for _, _result := range _results {
		_item := map[string]interface{}{}
		if strings.EqualFold(_responseFormat, "url") {
			_item["url"] = "data:" + codexImageMIMEType(_result.OutputFormat) + ";base64," + _result.Base64
		} else {
			_item["b64_json"] = _result.Base64
		}
		if _result.RevisedPrompt != "" {
			_item["revised_prompt"] = _result.RevisedPrompt
		}
		if _result.OutputFormat != "" {
			_item["output_format"] = _result.OutputFormat
		}
		if _result.Size != "" {
			_item["size"] = _result.Size
		}
		if _result.Width > 0 {
			_item["width"] = _result.Width
		}
		if _result.Height > 0 {
			_item["height"] = _result.Height
		}
		if _result.Bytes > 0 {
			_item["bytes"] = _result.Bytes
		}
		_data = append(_data, _item)
	}
	return json.Marshal(map[string]interface{}{"created": _createdAt, "data": _data})
}

// -------------------------------------------------------------------------------------
func codexImageMIMEType(_format string) string {
	switch strings.ToLower(strings.TrimSpace(_format)) {
	case "jpg", "jpeg":
		return "image/jpeg"
	case "webp":
		return "image/webp"
	default:
		return "image/png"
	}
}
