package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/codexauth"
	"LoadBalanceProvider/src/security"
)

// -------------------------------------------------------------------------------------
const openAICodexModelsManifestURL = "https://chatgpt.com/backend-api/codex/models"
const codexModelsManifestBodyLimit int64 = 8 * 1024 * 1024

// -------------------------------------------------------------------------------------
type CodexModelsManifest struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// -------------------------------------------------------------------------------------
type codexManifestModel struct {
	Slug       string `json:"slug"`
	Visibility string `json:"visibility,omitempty"`
}

// -------------------------------------------------------------------------------------
func CodexUsableModelNames(_body []byte) ([]string, error) {
	_root := struct {
		Models []codexManifestModel `json:"models"`
	}{}
	if _err := json.Unmarshal(_body, &_root); _err != nil {
		return nil, fmt.Errorf("decode Codex models manifest: %w", _err)
	}
	if len(_root.Models) == 0 {
		return nil, fmt.Errorf("Codex models manifest is empty")
	}

	_seen := make(map[string]bool, len(_root.Models))
	_models := make([]string, 0, len(_root.Models))
	for _, _model := range _root.Models {
		_slug := strings.TrimSpace(_model.Slug)
		_key := strings.ToLower(_slug)
		if _slug == "" || _key == "auto" || _seen[_key] {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(_model.Visibility), "hide") {
			continue
		}
		_seen[_key] = true
		_models = append(_models, _slug)
	}
	if len(_models) == 0 {
		return nil, fmt.Errorf("Codex models manifest has no usable API models")
	}
	return _models, nil
}

// -------------------------------------------------------------------------------------
func AddAutoModelToCodexManifest(_body []byte) ([]byte, error) {
	_root := map[string]json.RawMessage{}
	if _err := json.Unmarshal(_body, &_root); _err != nil {
		return nil, fmt.Errorf("decode Codex models manifest: %w", _err)
	}
	_models := []map[string]json.RawMessage{}
	if _err := json.Unmarshal(_root["models"], &_models); _err != nil || len(_models) == 0 {
		if _err == nil {
			_err = fmt.Errorf("models list is empty")
		}
		return nil, fmt.Errorf("decode Codex manifest models: %w", _err)
	}

	_minPriority := 0
	_hasPriority := false
	_customIndex := -1
	for _idx, _model := range _models {
		// 本代理目前只提供 HTTP/SSE，不宣告可用的 WebSocket 傳輸。
		_model["prefer_websockets"] = json.RawMessage("false")
		_isAuto := strings.EqualFold(manifestRawString(_model["slug"]), "AUTO")
		if _isAuto {
			_customIndex = _idx
		}
		var _priority int
		if !_isAuto && json.Unmarshal(_model["priority"], &_priority) == nil && (!_hasPriority || _priority < _minPriority) {
			_minPriority = _priority
			_hasPriority = true
		}
	}

	if _customIndex < 0 {
		_cloned, _err := json.Marshal(_models[0])
		if _err != nil {
			return nil, _err
		}
		_custom := map[string]json.RawMessage{}
		if _err := json.Unmarshal(_cloned, &_custom); _err != nil {
			return nil, _err
		}
		_models = append(_models, _custom)
		_customIndex = len(_models) - 1
	}

	_autoPriority := -1
	if _hasPriority {
		_autoPriority = _minPriority - 1
	}
	_custom := _models[_customIndex]
	for _key, _value := range map[string]interface{}{
		"slug":                    "AUTO",
		"prefer_websockets":       false,
		"display_name":            "AUTO",
		"description":             "由 Mars LLM Proxy 自動選擇 Provider 與預設模型。",
		"visibility":              "list",
		"supported_in_api":        true,
		"priority":                _autoPriority,
		"minimal_client_version":  "0.0.0",
		"default_reasoning_level": "high",
		"availability_nux":        nil,
		"upgrade":                 nil,
	} {
		_encoded, _err := json.Marshal(_value)
		if _err != nil {
			return nil, _err
		}
		_custom[_key] = _encoded
	}
	_orderedModels := make([]map[string]json.RawMessage, 0, len(_models))
	_orderedModels = append(_orderedModels, _custom)
	for _idx, _model := range _models {
		if _idx == _customIndex || strings.EqualFold(manifestRawString(_model["slug"]), "AUTO") {
			continue
		}
		_orderedModels = append(_orderedModels, _model)
	}
	_models = _orderedModels
	_encodedModels, _err := json.Marshal(_models)
	if _err != nil {
		return nil, _err
	}
	_root["models"] = _encodedModels
	return json.Marshal(_root)
}

// -------------------------------------------------------------------------------------
func manifestRawString(_value json.RawMessage) string {
	var _text string
	if len(_value) == 0 || json.Unmarshal(_value, &_text) != nil {
		return ""
	}
	return strings.TrimSpace(_text)
}

// -------------------------------------------------------------------------------------
func (_c *Client) FetchCodexModelsManifest(_ctx context.Context, _provider *balancer.ProviderRuntime, _source *http.Request) (CodexModelsManifest, error) {
	if !isOpenAICodexProvider(_provider) || _provider.Config == nil {
		return CodexModelsManifest{}, fmt.Errorf("provider is not an OpenAI Codex provider")
	}
	if providerAPIKey(_provider) != "" {
		return CodexModelsManifest{}, fmt.Errorf("Codex model manifest requires a ChatGPT OAuth provider")
	}

	_clientVersion := ""
	_ifNoneMatch := ""
	_userAgent := ""
	_originator := defaultCodexUpstreamOriginator
	if _source != nil {
		_clientVersion = strings.TrimSpace(_source.URL.Query().Get("client_version"))
		if _clientVersion == "" {
			_clientVersion = strings.TrimSpace(_source.Header.Get("Version"))
		}
		_ifNoneMatch = strings.TrimSpace(_source.Header.Get("If-None-Match"))
		if _value := strings.TrimSpace(_source.Header.Get("User-Agent")); _value != "" {
			_userAgent = _value
		}
		if _value := strings.TrimSpace(_source.Header.Get("Originator")); _value != "" {
			_originator = _value
		}
	}
	if _clientVersion == "" {
		// 管理介面沒有呼叫端版本，可由部署環境跟進 Codex 的模型可見性版本。
		_clientVersion = strings.TrimSpace(os.Getenv("MARS_CODEX_MODELS_CLIENT_VERSION"))
	}
	if _clientVersion == "" {
		_clientVersion = defaultCodexClientVersion
	}
	if _userAgent == "" {
		_userAgent = "codex-tui/" + _clientVersion + " (Mac OS; arm64)"
	}

	_auth, _err := codexauth.Ensure(_provider.Config.ID)
	if _err != nil {
		return CodexModelsManifest{}, fmt.Errorf("Codex OAuth unavailable: %w", _err)
	}

	_endpoint := openAICodexModelsManifestURL + "?client_version=" + url.QueryEscape(_clientVersion)
	if _err := security.ValidateOutboundURL(_endpoint); _err != nil {
		return CodexModelsManifest{}, _err
	}
	_requestContext, _cancel := context.WithTimeout(_ctx, 15*time.Second)
	defer _cancel()
	_request, _err := http.NewRequestWithContext(_requestContext, http.MethodGet, _endpoint, nil)
	if _err != nil {
		return CodexModelsManifest{}, _err
	}
	_request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(_auth.AccessToken))
	_request.Header.Set("Accept", "application/json")
	_request.Header.Set("User-Agent", _userAgent)
	_request.Header.Set("Originator", _originator)
	_request.Header.Set("Version", _clientVersion)
	if _ifNoneMatch != "" {
		_request.Header.Set("If-None-Match", _ifNoneMatch)
	}
	if _accountID := strings.TrimSpace(_auth.AccountID); _accountID != "" {
		_request.Header.Set("chatgpt-account-id", _accountID)
	}

	_httpClient := _c.HTTPClient
	if _httpClient == nil {
		_httpClient = &http.Client{Timeout: 0}
	}
	_response, _err := security.GuardedHTTPClient(_httpClient).Do(_request)
	if _err != nil {
		return CodexModelsManifest{}, _err
	}
	defer _response.Body.Close()

	_result := CodexModelsManifest{
		StatusCode: _response.StatusCode,
		Header:     _response.Header.Clone(),
	}
	if _response.StatusCode == http.StatusNotModified {
		return _result, nil
	}
	_result.Body, _err = io.ReadAll(io.LimitReader(_response.Body, codexModelsManifestBodyLimit))
	if _err != nil {
		return CodexModelsManifest{}, _err
	}
	if _response.StatusCode < http.StatusOK || _response.StatusCode >= http.StatusMultipleChoices {
		return _result, fmt.Errorf("Codex models manifest returned status %d: %s", _response.StatusCode, strings.TrimSpace(string(_result.Body)))
	}
	return _result, nil
}
