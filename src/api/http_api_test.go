package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"LoadBalanceProvider/src/auth"
	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/keyusage"
	"LoadBalanceProvider/src/providerusage"
	"LoadBalanceProvider/src/proxy"
)

// -------------------------------------------------------------------------------------
func TestCanAccessMCPRouteWithPermanentAPIOrMCPKey(t *testing.T) {
	_tests := []struct {
		name       string
		key        auth.APIKeyView
		route      string
		wantAccess bool
	}{
		{name: "api key can call MCP", key: auth.APIKeyView{KeyType: auth.APIKeyTypeChat}, route: "/mcp/", wantAccess: true},
		{name: "MCP key can call MCP", key: auth.APIKeyView{KeyType: auth.APIKeyTypeMCP}, route: "/mcp/", wantAccess: true},
		{name: "temporary login key cannot call MCP", key: auth.APIKeyView{KeyType: auth.APIKeyTypeSession, Temporary: true}, route: "/mcp/", wantAccess: false},
		{name: "MCP key cannot call general API", key: auth.APIKeyView{KeyType: auth.APIKeyTypeMCP}, route: "/v1/chat/completions", wantAccess: false},
	}
	for _, _test := range _tests {
		t.Run(_test.name, func(t *testing.T) {
			if _got := canAccessRoute(_test.key, false, http.MethodPost, _test.route); _got != _test.wantAccess {
				t.Fatalf("canAccessRoute() = %v, want %v", _got, _test.wantAccess)
			}
		})
	}
}

// -------------------------------------------------------------------------------------
func TestImageGenerateMCPToolDefaultsToBase64Result(t *testing.T) {
	for _, _spec := range mcpToolSpecs() {
		if _spec.Definition.Name != "image_gen" {
			continue
		}
		if _spec.Route.Path != "/v1/images/generations" || !_spec.Route.BodyFromArguments {
			t.Fatalf("image_gen route = %#v", _spec.Route)
		}
		if _spec.Route.BodyDefaults["response_format"] != "b64_json" {
			t.Fatalf("image_gen response_format default = %#v", _spec.Route.BodyDefaults["response_format"])
		}
		if _spec.Route.BodyForcedValues["response_format"] != "b64_json" {
			t.Fatalf("image_gen response_format forced value = %#v", _spec.Route.BodyForcedValues["response_format"])
		}
		if !_spec.Route.RichContent {
			t.Fatal("image_gen route must be declared as rich content")
		}
		if _spec.Definition.OutputSchema != nil {
			t.Fatalf("image_gen must not declare the HTTP output schema: %#v", _spec.Definition.OutputSchema)
		}
		return
	}
	t.Fatal("image_gen MCP tool was not registered")
}

// -------------------------------------------------------------------------------------
func TestMCPGeneratedImageContentCompactsLargePNG(t *testing.T) {
	_source := image.NewRGBA(image.Rect(0, 0, 640, 640))
	_seed := uint32(0x9e3779b9)
	_nextByte := func() uint8 {
		_seed ^= _seed << 13
		_seed ^= _seed >> 17
		_seed ^= _seed << 5
		return uint8(_seed)
	}
	for _y := 0; _y < 640; _y++ {
		for _x := 0; _x < 640; _x++ {
			_source.SetRGBA(_x, _y, color.RGBA{
				R: _nextByte(),
				G: _nextByte(),
				B: _nextByte(),
				A: 255,
			})
		}
	}
	var _png bytes.Buffer
	if _err := png.Encode(&_png, _source); _err != nil {
		t.Fatal(_err)
	}
	if _png.Len() <= mcpImagePreviewTotalMaxBytes {
		t.Fatalf("test PNG is too small: %d bytes", _png.Len())
	}

	_content, _sanitized, _ok := mcpGeneratedImageContent(map[string]interface{}{
		"data": []interface{}{map[string]interface{}{"b64_json": base64.StdEncoding.EncodeToString(_png.Bytes())}},
	})
	if !_ok || len(_content) != 1 {
		t.Fatalf("image content = %#v, sanitized = %#v", _content, _sanitized)
	}
	_encoded, _ := _content[0]["data"].(string)
	_preview, _err := base64.StdEncoding.DecodeString(_encoded)
	if _err != nil {
		t.Fatal(_err)
	}
	if len(_preview) > mcpImagePreviewTotalMaxBytes {
		t.Fatalf("preview size = %d, limit = %d", len(_preview), mcpImagePreviewTotalMaxBytes)
	}
	if _content[0]["mimeType"] != "image/jpeg" {
		t.Fatalf("preview MIME = %#v", _content[0]["mimeType"])
	}
	if _, _, _err := image.Decode(bytes.NewReader(_preview)); _err != nil {
		t.Fatalf("preview cannot be decoded: %v", _err)
	}
}

// -------------------------------------------------------------------------------------
func TestMCPImageResultDoesNotExposeCompetingStructuredContent(t *testing.T) {
	_source := image.NewRGBA(image.Rect(0, 0, 2, 2))
	_source.SetRGBA(0, 0, color.RGBA{R: 255, A: 255})
	var _png bytes.Buffer
	if _err := png.Encode(&_png, _source); _err != nil {
		t.Fatal(_err)
	}
	_body, _err := json.Marshal(map[string]interface{}{
		"created": 1,
		"data": []interface{}{
			map[string]interface{}{"b64_json": base64.StdEncoding.EncodeToString(_png.Bytes())},
		},
	})
	if _err != nil {
		t.Fatal(_err)
	}

	_result := mcpToolResultFromHTTP(http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, _body)
	_content, _ok := _result["content"].([]map[string]interface{})
	if !_ok || len(_content) != 1 || _content[0]["type"] != "image" {
		t.Fatalf("MCP image content = %#v", _result["content"])
	}
	if _, _exists := _result["structuredContent"]; _exists {
		t.Fatalf("rich media result must not include structuredContent: %#v", _result)
	}
	_meta, _ := _content[0]["_meta"].(map[string]interface{})
	if _meta["codex/imageDetail"] != "original" {
		t.Fatalf("MCP image content is missing Codex image metadata: %#v", _content[0])
	}
}

// -------------------------------------------------------------------------------------
func TestAPIKeyRoutingPolicyOverridesChatRequest(t *testing.T) {
	_request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	_request = _request.WithContext(context.WithValue(_request.Context(), requestAPIKeyContextKey{}, auth.APIKeyView{
		KeyType:         auth.APIKeyTypeChat,
		ProviderID:      "provider-2",
		Model:           "model-2",
		ReasoningEffort: "medium",
	}))
	_body, _err := applyAPIKeyRoutingPolicyToJSONRequest(_request, []byte(`{"model":"caller-model","provider_id":"caller-provider","reasoning_effort":"low","messages":[]}`), false)
	if _err != nil {
		t.Fatal(_err)
	}
	var _payload map[string]interface{}
	if _err := json.Unmarshal(_body, &_payload); _err != nil {
		t.Fatal(_err)
	}
	if _payload["provider_id"] != "provider-2" || _payload["model"] != "model-2" || _payload["reasoning_effort"] != "medium" {
		t.Fatalf("rewritten payload = %#v", _payload)
	}
}

// -------------------------------------------------------------------------------------
// AUTO 代表「不強制」：呼叫端自己送來的 provider / model / reasoning 必須原封不動保留。
func TestAPIKeyAutoRoutingPolicyPreservesCallerOverrides(t *testing.T) {
	_request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	_request = _request.WithContext(context.WithValue(_request.Context(), requestAPIKeyContextKey{}, auth.APIKeyView{
		KeyType:         auth.APIKeyTypeChat,
		ProviderID:      "AUTO",
		Model:           "AUTO",
		ReasoningEffort: "AUTO",
	}))
	_body, _err := applyAPIKeyRoutingPolicyToJSONRequest(_request, []byte(`{"model":"caller-model","provider_id":"caller-provider","reasoning":{"effort":"xhigh","summary":"auto"},"input":"hi"}`), true)
	if _err != nil {
		t.Fatal(_err)
	}
	var _payload map[string]interface{}
	if _err := json.Unmarshal(_body, &_payload); _err != nil {
		t.Fatal(_err)
	}
	if _payload["provider_id"] != "caller-provider" {
		t.Fatalf("provider_id = %#v, want caller-provider preserved", _payload["provider_id"])
	}
	if _payload["model"] != "caller-model" {
		t.Fatalf("model = %#v, want caller-model preserved", _payload["model"])
	}
	_reasoning, _ok := _payload["reasoning"].(map[string]interface{})
	if !_ok || _reasoning["summary"] != "auto" {
		t.Fatalf("reasoning summary was not preserved: %#v", _payload["reasoning"])
	}
	if _reasoning["effort"] != "xhigh" {
		t.Fatalf("reasoning effort = %#v, want xhigh preserved", _reasoning["effort"])
	}
}

// -------------------------------------------------------------------------------------
// 只綁 provider、模型維持 AUTO：provider 被強制，模型仍由呼叫端決定。
func TestAPIKeyRoutingPolicyPinsProviderButKeepsCallerModel(t *testing.T) {
	_request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	_request = _request.WithContext(context.WithValue(_request.Context(), requestAPIKeyContextKey{}, auth.APIKeyView{
		KeyType:         auth.APIKeyTypeChat,
		ProviderID:      "provider-2",
		Model:           "AUTO",
		ReasoningEffort: "AUTO",
	}))
	_body, _err := applyAPIKeyRoutingPolicyToJSONRequest(_request, []byte(`{"model":"caller-model","reasoning_effort":"low","messages":[]}`), false)
	if _err != nil {
		t.Fatal(_err)
	}
	var _payload map[string]interface{}
	if _err := json.Unmarshal(_body, &_payload); _err != nil {
		t.Fatal(_err)
	}
	if _payload["provider_id"] != "provider-2" {
		t.Fatalf("provider_id = %#v, want provider-2", _payload["provider_id"])
	}
	if _payload["model"] != "caller-model" {
		t.Fatalf("model = %#v, want caller-model preserved", _payload["model"])
	}
	if _payload["reasoning_effort"] != "low" {
		t.Fatalf("reasoning_effort = %#v, want low preserved", _payload["reasoning_effort"])
	}
}

// -------------------------------------------------------------------------------------
func TestAPIKeyModelExistsValidatesAgainstPinnedProvider(t *testing.T) {
	_handler := &HTTPAPI{Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{
		{ID: "provider-1", Enabled: true, Models: []domain.LLMModelConfig{{Name: "model-a", Aliases: []string{"alias-a"}}}},
		{ID: "provider-2", Enabled: true, Models: []domain.LLMModelConfig{{Name: "model-b"}}},
	}})}

	if !_handler.apiKeyModelExists("provider-1", "model-a") {
		t.Fatal("model-a should exist on provider-1")
	}
	if !_handler.apiKeyModelExists("provider-1", "alias-a") {
		t.Fatal("alias-a should match model-a on provider-1")
	}
	if _handler.apiKeyModelExists("provider-1", "model-b") {
		t.Fatal("model-b belongs to provider-2 and must not validate against provider-1")
	}
	if !_handler.apiKeyModelExists("AUTO", "model-b") {
		t.Fatal("model-b should validate when the provider is AUTO")
	}
	if _handler.apiKeyModelExists("AUTO", "does-not-exist") {
		t.Fatal("unknown model must not validate")
	}
}

// -------------------------------------------------------------------------------------
func TestPinnedRoutingUnavailableReasonExplainsDisabledProvider(t *testing.T) {
	_handler := &HTTPAPI{Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{
		{ID: "provider-1", Name: "Pinned One", Enabled: false, Models: []domain.LLMModelConfig{{Name: "model-a"}}},
	}})}

	_pinned := func(_providerID string, _model string) *http.Request {
		_request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		return _request.WithContext(context.WithValue(_request.Context(), requestAPIKeyContextKey{}, auth.APIKeyView{
			KeyType:    auth.APIKeyTypeChat,
			ProviderID: _providerID, Model: _model, ReasoningEffort: "AUTO",
		}))
	}

	if _reason := _handler.pinnedRoutingUnavailableReason(_pinned("provider-1", "AUTO")); !strings.Contains(_reason, "disabled") {
		t.Fatalf("disabled provider reason = %q", _reason)
	}
	if _reason := _handler.pinnedRoutingUnavailableReason(_pinned("ghost-provider", "AUTO")); !strings.Contains(_reason, "no longer exists") {
		t.Fatalf("missing provider reason = %q", _reason)
	}
	if _reason := _handler.pinnedRoutingUnavailableReason(_pinned("AUTO", "AUTO")); _reason != "" {
		t.Fatalf("AUTO policy should not produce a pinned reason, got %q", _reason)
	}
}

// 全 AUTO 的政策不得改寫 body：既省下重新序列化，也避免數值精度／空 body 被破壞。
func TestAutoRoutingPolicyLeavesBodyByteIdentical(t *testing.T) {
	_request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	_request = _request.WithContext(context.WithValue(_request.Context(), requestAPIKeyContextKey{}, auth.APIKeyView{
		KeyType:    auth.APIKeyTypeChat,
		ProviderID: "AUTO", Model: "AUTO", ReasoningEffort: "AUTO",
	}))

	_original := []byte(`{"model":"m","seed":9007199254740993,"temperature":1.0,"messages":[]}`)
	_rewritten, _err := applyAPIKeyRoutingPolicyToJSONRequest(_request, _original, false)
	if _err != nil {
		t.Fatal(_err)
	}
	if string(_rewritten) != string(_original) {
		t.Fatalf("AUTO policy must not touch the body:\n got %s\nwant %s", _rewritten, _original)
	}

	_empty, _err := applyAPIKeyRoutingPolicyToJSONRequest(_request, []byte(``), false)
	if _err != nil {
		t.Fatal(_err)
	}
	if string(_empty) != "" {
		t.Fatalf("empty body must stay empty, got %q", _empty)
	}
}

// -------------------------------------------------------------------------------------
// 需要改寫時，未涉及的大整數欄位仍須保持原值（UseNumber）。
func TestRoutingPolicyRewritePreservesLargeIntegers(t *testing.T) {
	_request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	_request = _request.WithContext(context.WithValue(_request.Context(), requestAPIKeyContextKey{}, auth.APIKeyView{
		KeyType:    auth.APIKeyTypeChat,
		ProviderID: "provider-2", Model: "AUTO", ReasoningEffort: "AUTO",
	}))

	_rewritten, _err := applyAPIKeyRoutingPolicyToJSONRequest(_request, []byte(`{"model":"m","seed":9007199254740993,"messages":[]}`), false)
	if _err != nil {
		t.Fatal(_err)
	}
	if !strings.Contains(string(_rewritten), `"seed":9007199254740993`) {
		t.Fatalf("large integer lost precision: %s", _rewritten)
	}
	if !strings.Contains(string(_rewritten), `"provider_id":"provider-2"`) {
		t.Fatalf("provider was not pinned: %s", _rewritten)
	}
}

// -------------------------------------------------------------------------------------
func TestTemporaryAPIKeyDoesNotOverrideRequest(t *testing.T) {
	_request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	_request = _request.WithContext(context.WithValue(_request.Context(), requestAPIKeyContextKey{}, auth.APIKeyView{
		Temporary:       true,
		ProviderID:      "provider-2",
		Model:           "model-2",
		ReasoningEffort: "high",
	}))
	_original := []byte(`{"model":"caller-model","messages":[]}`)
	_rewritten, _err := applyAPIKeyRoutingPolicyToJSONRequest(_request, _original, false)
	if _err != nil {
		t.Fatal(_err)
	}
	if string(_rewritten) != string(_original) {
		t.Fatalf("temporary key rewrote request: %s", _rewritten)
	}
}

// -------------------------------------------------------------------------------------
// 帶了明確憑證就完全不讀 Session Cookie，否則失效的 Chat Key 會被登入 session 掩蓋。
func TestRequestAPIKeysIgnoreSessionCookieWhenExplicitCredentialPresent(t *testing.T) {
	_request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	_request.Header.Set("Authorization", "Bearer lbp_chat")
	_request.Header.Set("X-API-Key", "lbp_alternate")
	_request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "lbp_session"})

	_keys := requestAPIKeys(_request)
	if len(_keys) != 2 {
		t.Fatalf("keys = %#v, want only the explicit credentials", _keys)
	}
	if _keys[0].Key != "lbp_chat" || _keys[0].FromCookie {
		t.Fatalf("first key = %#v, want explicit bearer", _keys[0])
	}
	for _, _candidate := range _keys {
		if _candidate.FromCookie {
			t.Fatalf("session cookie must not be considered: %#v", _keys)
		}
	}
}

// -------------------------------------------------------------------------------------
func TestRequestAPIKeysFallsBackToSessionCookieWithoutExplicitCredential(t *testing.T) {
	_request := httptest.NewRequest(http.MethodGet, "/api/api-keys", nil)
	_request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "lbp_session"})

	_keys := requestAPIKeys(_request)
	if len(_keys) != 1 || _keys[0].Key != "lbp_session" || !_keys[0].FromCookie {
		t.Fatalf("keys = %#v, want the session cookie", _keys)
	}
}

// Response 擁有者必須來自「已驗證」的金鑰身分，不得由未驗證的 Header/Cookie 推導。
func TestResponseRouteOwnerComesFromVerifiedKeyOnly(t *testing.T) {
	_spoofed := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	_spoofed.Header.Set("X-API-Key", "someone-elses-key")
	_spoofed.AddCookie(&http.Cookie{Name: "unrelated_cookie", Value: "not-a-credential"})
	if _owner := proxy.ResponseRouteOwner(_spoofed); _owner != "anonymous" {
		t.Fatalf("unverified headers must not establish ownership, got %q", _owner)
	}

	_verified := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	_verified.Header.Set("X-API-Key", "someone-elses-key")
	_verified = _verified.WithContext(proxy.WithResponseRouteOwner(_verified.Context(), "key:real-id"))
	if _owner := proxy.ResponseRouteOwner(_verified); _owner != "key:real-id" {
		t.Fatalf("owner = %q, want the verified key identity", _owner)
	}
}

// 沒有 previous_response_id 時完全不介入，維持原本的負載平衡。
func TestConversationAffinitySkippedWithoutPreviousResponse(t *testing.T) {
	_handler := &HTTPAPI{Client: proxy.NewClient(), Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{})}
	_request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	_selection := domain.ChatCompletionRequest{Model: "AUTO"}

	_pinned, _dropped := _handler.applyConversationAffinity(&_selection, []byte(`{"model":"AUTO","input":"hi"}`), _request)
	if _pinned || _dropped {
		t.Fatalf("pinned=%v dropped=%v, want both false", _pinned, _dropped)
	}
	if _selection.ProviderID != "" {
		t.Fatalf("provider must stay unpinned, got %q", _selection.ProviderID)
	}
}

// -------------------------------------------------------------------------------------
// 帶 previous_response_id 時，必須回到當初服務該回應的 provider 與模型。
func TestConversationAffinityPinsPreviousProvider(t *testing.T) {
	_client := proxy.NewClient()
	_handler := &HTTPAPI{Client: _client, Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{
		{ID: "provider-a", Enabled: true, BaseURL: "http://a"},
		{ID: "provider-b", Enabled: true, BaseURL: "http://b"},
	}})}

	_request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	_request = _request.WithContext(proxy.WithResponseRouteOwner(_request.Context(), "key:owner-1"))
	_client.RecordResponseSnapshotForOwner("resp_prev", "provider-a", "gpt-5.6-luna", proxy.ResponseRouteOwner(_request), nil, nil)

	_selection := domain.ChatCompletionRequest{Model: "AUTO"}
	_pinned, _dropped := _handler.applyConversationAffinity(&_selection, []byte(`{"model":"AUTO","previous_response_id":"resp_prev"}`), _request)
	if !_pinned || _dropped {
		t.Fatalf("pinned=%v dropped=%v, want pinned", _pinned, _dropped)
	}
	if _selection.ProviderID != "provider-a" {
		t.Fatalf("provider = %q, want provider-a", _selection.ProviderID)
	}
	if _selection.Model != "gpt-5.6-luna" {
		t.Fatalf("model = %q, want the model that served the previous response", _selection.Model)
	}
}

// -------------------------------------------------------------------------------------
// Codex 多輪工具呼叫常常不送 previous_response_id，只帶 prompt_cache_key，
// 這種請求同樣必須黏回原 provider。
func TestConversationAffinityPinsByPromptCacheKey(t *testing.T) {
	_client := proxy.NewClient()
	_handler := &HTTPAPI{Client: _client, Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{
		{ID: "provider-a", Enabled: true, BaseURL: "http://a"},
		{ID: "provider-b", Enabled: true, BaseURL: "http://b"},
	}})}

	_request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	_request = _request.WithContext(proxy.WithResponseRouteOwner(_request.Context(), "key:owner-1"))
	_client.RecordPromptCacheRoute(proxy.PromptCacheRouteID("conv-uuid"), "provider-a", "gpt-5.6-luna", proxy.ResponseRouteOwner(_request))

	_selection := domain.ChatCompletionRequest{Model: "AUTO"}
	_body := []byte(`{"model":"AUTO","prompt_cache_key":"conv-uuid","input":[{"type":"reasoning","encrypted_content":"x"}]}`)
	_pinned, _dropped := _handler.applyConversationAffinity(&_selection, _body, _request)
	if !_pinned || _dropped {
		t.Fatalf("pinned=%v dropped=%v, want pinned via prompt_cache_key", _pinned, _dropped)
	}
	if _selection.ProviderID != "provider-a" {
		t.Fatalf("provider = %q, want provider-a", _selection.ProviderID)
	}
}

// -------------------------------------------------------------------------------------
// 全新對話（沒有任何延續性內容）不該被判定為需要重設，避免無謂的 strip 與日誌噪音。
func TestConversationAffinityIgnoresBrandNewConversation(t *testing.T) {
	_handler := &HTTPAPI{Client: proxy.NewClient(), Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{})}
	_request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	_selection := domain.ChatCompletionRequest{Model: "AUTO"}
	_body := []byte(`{"model":"AUTO","prompt_cache_key":"brand-new","input":[{"type":"message","role":"user","content":"hi"}]}`)
	_pinned, _dropped := _handler.applyConversationAffinity(&_selection, _body, _request)
	if _pinned || _dropped {
		t.Fatalf("pinned=%v dropped=%v, want both false for a brand new conversation", _pinned, _dropped)
	}
}

// -------------------------------------------------------------------------------------
// 未知的 cache key 但帶著加密推理內容 → 必須重設延續性，不能原樣送給隨機 provider。
func TestConversationAffinityResetsUnknownCacheKeyWithEncryptedReasoning(t *testing.T) {
	_handler := &HTTPAPI{Client: proxy.NewClient(), Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{})}
	_request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	_selection := domain.ChatCompletionRequest{Model: "AUTO"}
	_body := []byte(`{"model":"AUTO","prompt_cache_key":"unknown","input":[{"type":"reasoning","encrypted_content":"x"}]}`)
	_pinned, _dropped := _handler.applyConversationAffinity(&_selection, _body, _request)
	if _pinned || !_dropped {
		t.Fatalf("pinned=%v dropped=%v, want continuity reset", _pinned, _dropped)
	}
}

// -------------------------------------------------------------------------------------
// 明確模型是呼叫端參數，黏著只固定原 provider，不得拿舊模型覆寫。
func TestConversationAffinityPreservesExplicitModel(t *testing.T) {
	_client := proxy.NewClient()
	_handler := &HTTPAPI{Client: _client, Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{
		{ID: "provider-a", Name: "Provider A", Kind: "openai-codex", Enabled: true},
	}})}
	_request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	_client.RecordResponseSnapshotForOwner("resp_explicit", "provider-a", "old-model", proxy.ResponseRouteOwner(_request), nil, nil)

	_selection := domain.ChatCompletionRequest{Model: "caller-model"}
	_pinned, _dropped := _handler.applyConversationAffinity(&_selection, []byte(`{"model":"caller-model","previous_response_id":"resp_explicit"}`), _request)
	if !_pinned || _dropped {
		t.Fatalf("pinned=%v dropped=%v, want pinned", _pinned, _dropped)
	}
	if _selection.ProviderID != "provider-a" || _selection.Model != "caller-model" {
		t.Fatalf("selection = provider %q model %q", _selection.ProviderID, _selection.Model)
	}
}

// -------------------------------------------------------------------------------------
// route 過期、淘汰或重啟遺失時仍可重平衡，但必須要求移除舊 continuity。
func TestConversationAffinityRouteMissResetsContinuity(t *testing.T) {
	_handler := &HTTPAPI{Client: proxy.NewClient(), Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{})}
	_request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	_selection := domain.ChatCompletionRequest{Model: "AUTO"}

	_pinned, _dropped := _handler.applyConversationAffinity(&_selection, []byte(`{"model":"AUTO","previous_response_id":"missing"}`), _request)
	if _pinned || !_dropped {
		t.Fatalf("pinned=%v dropped=%v, want continuity reset", _pinned, _dropped)
	}
}

// -------------------------------------------------------------------------------------
// 強制 provider 與 response route 不同時，不可把原帳號的 ID 轉送給新帳號。
func TestConversationAffinityForcedProviderConflictResetsContinuity(t *testing.T) {
	_client := proxy.NewClient()
	_handler := &HTTPAPI{Client: _client, Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{
		{ID: "provider-a", Name: "Provider A", Kind: "openai-codex", Enabled: true},
		{ID: "provider-b", Name: "Provider B", Kind: "openai-codex", Enabled: true},
	}})}
	_request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	_client.RecordResponseSnapshotForOwner("resp_conflict", "provider-a", "model-a", proxy.ResponseRouteOwner(_request), nil, nil)
	_selection := domain.ChatCompletionRequest{Model: "AUTO", ProviderID: "provider-b"}

	_pinned, _dropped := _handler.applyConversationAffinity(&_selection, []byte(`{"model":"AUTO","previous_response_id":"resp_conflict"}`), _request)
	if _pinned || !_dropped {
		t.Fatalf("pinned=%v dropped=%v, want continuity reset", _pinned, _dropped)
	}
	if _selection.ProviderID != "provider-b" {
		t.Fatalf("forced provider changed to %q", _selection.ProviderID)
	}
}

// -------------------------------------------------------------------------------------
func TestAdvancedSettingsHandlersPersistValues(t *testing.T) {
	_path := filepath.Join(t.TempDir(), "advanced_settings.json")
	_handler := &HTTPAPI{Client: proxy.NewClient(), AdvancedSettingsConfigPath: _path}
	_recorder := httptest.NewRecorder()
	_handler.handleSaveAdvancedSettings(_recorder, []byte(`{
		"conversationAffinityTTLMinutes":45,
		"conversationAffinityQuotaTolerancePoints":12.5,
		"responseRouteMaxEntries":3500,
		"providerCapacityCooldownSeconds":25,
		"maxBindingsPerProvider":7,
		"yieldLowMaxPercent":3.5,
		"yieldMidMaxPercent":24
	}`))
	if _recorder.Code != http.StatusOK {
		t.Fatalf("save status = %d body=%s", _recorder.Code, _recorder.Body.String())
	}

	_recorder = httptest.NewRecorder()
	_handler.handleGetAdvancedSettings(_recorder)
	if _recorder.Code != http.StatusOK {
		t.Fatalf("get status = %d body=%s", _recorder.Code, _recorder.Body.String())
	}
	var _payload struct {
		Advanced AdvancedSettingsForm `json:"advanced"`
	}
	if _err := json.Unmarshal(_recorder.Body.Bytes(), &_payload); _err != nil {
		t.Fatal(_err)
	}
	if _payload.Advanced.ConversationAffinityTTLMinutes != 45 ||
		_payload.Advanced.ConversationAffinityQuotaTolerancePoints != 12.5 ||
		_payload.Advanced.ResponseRouteMaxEntries != 3500 ||
		_payload.Advanced.ProviderCapacityCooldownSeconds != 25 ||
		_payload.Advanced.MaxBindingsPerProvider != 7 ||
		_payload.Advanced.YieldLowMaxPercent != 3.5 ||
		_payload.Advanced.YieldMidMaxPercent != 24 {
		t.Fatalf("advanced settings = %#v", _payload.Advanced)
	}
}

// -------------------------------------------------------------------------------------
// 部分更新不得把未提供的欄位重設為零值。tolerance 的 0 是合法值，
// 一旦被靜默寫入就會無聲關閉對話黏著。
func TestAdvancedSettingsPartialUpdateKeepsOtherValues(t *testing.T) {
	_path := filepath.Join(t.TempDir(), "advanced_settings.json")
	_handler := &HTTPAPI{Client: proxy.NewClient(), AdvancedSettingsConfigPath: _path}

	_recorder := httptest.NewRecorder()
	_handler.handleSaveAdvancedSettings(_recorder, []byte(`{
		"conversationAffinityTTLMinutes":45,
		"conversationAffinityQuotaTolerancePoints":12.5,
		"responseRouteMaxEntries":3500
	}`))
	if _recorder.Code != http.StatusOK {
		t.Fatalf("initial save status = %d body=%s", _recorder.Code, _recorder.Body.String())
	}

	// 只更新 TTL，其餘欄位必須保持不變。
	_recorder = httptest.NewRecorder()
	_handler.handleSaveAdvancedSettings(_recorder, []byte(`{"conversationAffinityTTLMinutes":90}`))
	if _recorder.Code != http.StatusOK {
		t.Fatalf("partial save status = %d body=%s", _recorder.Code, _recorder.Body.String())
	}

	var _payload struct {
		Advanced AdvancedSettingsForm `json:"advanced"`
	}
	if _err := json.Unmarshal(_recorder.Body.Bytes(), &_payload); _err != nil {
		t.Fatal(_err)
	}
	if _payload.Advanced.ConversationAffinityTTLMinutes != 90 {
		t.Fatalf("ttl = %d, want 90", _payload.Advanced.ConversationAffinityTTLMinutes)
	}
	if _payload.Advanced.ConversationAffinityQuotaTolerancePoints != 12.5 {
		t.Fatalf("tolerance = %v, want 12.5 preserved", _payload.Advanced.ConversationAffinityQuotaTolerancePoints)
	}
	if _payload.Advanced.ResponseRouteMaxEntries != 3500 {
		t.Fatalf("max entries = %d, want 3500 preserved", _payload.Advanced.ResponseRouteMaxEntries)
	}

	// 明確送 0 仍要生效（0 是合法的「完全不容忍」設定）。
	_recorder = httptest.NewRecorder()
	_handler.handleSaveAdvancedSettings(_recorder, []byte(`{"conversationAffinityQuotaTolerancePoints":0}`))
	if _recorder.Code != http.StatusOK {
		t.Fatalf("explicit zero status = %d body=%s", _recorder.Code, _recorder.Body.String())
	}
	if _err := json.Unmarshal(_recorder.Body.Bytes(), &_payload); _err != nil {
		t.Fatal(_err)
	}
	if _payload.Advanced.ConversationAffinityQuotaTolerancePoints != 0 {
		t.Fatalf("explicit zero tolerance was not applied: %#v", _payload.Advanced)
	}
}

// -------------------------------------------------------------------------------------
// 放棄黏著時，只有原帳號能解讀的延續性內容必須一併移除，否則新 provider 必定失敗。
func TestStripConversationContinuityRemovesEncryptedReasoning(t *testing.T) {
	_body := []byte(`{"model":"m","previous_response_id":"resp_prev","input":[` +
		`{"type":"message","role":"user","content":"hi"},` +
		`{"type":"reasoning","encrypted_content":"secret"},` +
		`{"type":"function_call_output","output":"done"}]}`)

	_stripped, _err := stripConversationContinuity(_body)
	if _err != nil {
		t.Fatal(_err)
	}
	_text := string(_stripped)
	if strings.Contains(_text, "previous_response_id") {
		t.Fatalf("previous_response_id must be removed: %s", _text)
	}
	if strings.Contains(_text, "encrypted_content") || strings.Contains(_text, "secret") {
		t.Fatalf("encrypted reasoning must be removed: %s", _text)
	}
	if !strings.Contains(_text, "function_call_output") || !strings.Contains(_text, `"hi"`) {
		t.Fatalf("ordinary conversation items must be kept: %s", _text)
	}
}

// -------------------------------------------------------------------------------------
func TestWriteSelectionUnavailableSetsRetryAfterOnlyForTemporaryOverload(t *testing.T) {
	_handler := &HTTPAPI{}
	_temporary := httptest.NewRecorder()
	_handler.writeSelectionUnavailable(_temporary, nil, &balancer.NoAvailableProviderError{
		TemporaryOverload: true,
		RetryAfter:        15 * time.Second,
	})
	if _temporary.Code != http.StatusServiceUnavailable || _temporary.Header().Get("Retry-After") != "15" {
		t.Fatalf("temporary response = status %d Retry-After %q", _temporary.Code, _temporary.Header().Get("Retry-After"))
	}

	_generic := httptest.NewRecorder()
	_handler.writeSelectionUnavailable(_generic, nil, &balancer.NoAvailableProviderError{})
	if _generic.Header().Get("Retry-After") != "" {
		t.Fatalf("generic Retry-After = %q, want empty", _generic.Header().Get("Retry-After"))
	}
}

// -------------------------------------------------------------------------------------
func TestProviderUsageHistoryIncludesOnlyEnabledProviders(t *testing.T) {
	_recorder := providerusage.NewRecorder(filepath.Join(t.TempDir(), "provider_usage"))
	_start := time.Date(2026, time.August, 10, 0, 0, 0, 0, time.Local)
	_end := time.Date(2026, time.August, 10, 23, 59, 0, 0, time.Local)
	if _err := _recorder.RecordDayStart("enabled-provider", 100, _start); _err != nil {
		t.Fatalf("record start: %v", _err)
	}
	if _err := _recorder.RecordDayEnd("enabled-provider", 65, _end); _err != nil {
		t.Fatalf("record end: %v", _err)
	}

	_balancer := balancer.NewLoadBalancer(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{
		{ID: "enabled-provider", Enabled: true},
		{ID: "disabled-provider", Enabled: false},
	}})
	_handler := &HTTPAPI{Balancer: _balancer, ProviderUsageRecorder: _recorder}
	_response := httptest.NewRecorder()

	_handler.handleGetProviderUsageHistory(_response, "2026-08")
	if _response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", _response.Code, _response.Body.String())
	}

	var _payload providerusage.MonthStats
	if _err := json.Unmarshal(_response.Body.Bytes(), &_payload); _err != nil {
		t.Fatalf("decode response: %v", _err)
	}
	if _payload.ProviderCount != 1 || _payload.ObservedDays != 1 || len(_payload.Days) != 1 {
		t.Fatalf("payload = %#v", _payload)
	}
	if _payload.Days[0].Date != "2026-08-10" || _payload.Days[0].UsagePercent != 35 || _payload.Days[0].RemainingPercent != 65 {
		t.Fatalf("day = %#v", _payload.Days[0])
	}
}

// -------------------------------------------------------------------------------------
func TestUpstreamContentRejectionDoesNotPenalizeProviderHealth(t *testing.T) {
	_provider := &balancer.ProviderRuntime{Config: &domain.LLMProviderConfig{ID: "provider-1", Enabled: true}}
	_err := &proxy.ProviderStreamError{
		Message:           "Request blocked by content policy",
		ResponseForwarded: true,
		UpstreamRejected:  true,
	}

	recordProviderForwardFailure(_provider, _err, nil, time.Second, defaultProviderCapacityCooldown, 30*time.Second)

	if _provider.Failures != 0 || _provider.ConsecutiveFailures != 0 {
		t.Fatalf("content rejection changed provider health: failures=%d consecutive=%d", _provider.Failures, _provider.ConsecutiveFailures)
	}
}

// -------------------------------------------------------------------------------------
func TestSetCORSHeadersAllowsPreflightWithExplicitTokenHeader(t *testing.T) {
	_req := httptest.NewRequest(http.MethodOptions, "/v1/chat/completions", nil)
	_req.Header.Set("Origin", "https://client.example")
	_req.Header.Set("Access-Control-Request-Headers", "content-type, authorization")
	_rec := httptest.NewRecorder()

	setCORSHeaders(_rec, _req)

	if _got := _rec.Header().Get("Access-Control-Allow-Origin"); _got != "https://client.example" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want origin", _got)
	}
	if _got := _rec.Header().Get("Access-Control-Allow-Credentials"); _got != "" {
		t.Fatalf("Access-Control-Allow-Credentials = %q, want empty", _got)
	}
}

// -------------------------------------------------------------------------------------
func TestSetCORSHeadersRejectsPreflightWithoutExplicitTokenHeader(t *testing.T) {
	_req := httptest.NewRequest(http.MethodOptions, "/api/session", nil)
	_req.Header.Set("Origin", "https://client.example")
	_req.Header.Set("Access-Control-Request-Headers", "content-type")
	_rec := httptest.NewRecorder()

	setCORSHeaders(_rec, _req)

	if _got := _rec.Header().Get("Access-Control-Allow-Origin"); _got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want empty", _got)
	}
}

// -------------------------------------------------------------------------------------
func TestSetCORSHeadersAllowsActualRequestWithExplicitAPIKey(t *testing.T) {
	_req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	_req.Header.Set("Origin", "https://client.example")
	_req.Header.Set("X-API-Key", "lbp_test")
	_rec := httptest.NewRecorder()

	setCORSHeaders(_rec, _req)

	if _got := _rec.Header().Get("Access-Control-Allow-Origin"); _got != "https://client.example" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want origin", _got)
	}
	if _got := _rec.Header().Get("Access-Control-Allow-Credentials"); _got != "" {
		t.Fatalf("Access-Control-Allow-Credentials = %q, want empty", _got)
	}
}

// -------------------------------------------------------------------------------------
func TestSetCORSHeadersRejectsCookieOnlyCrossOriginRequest(t *testing.T) {
	_req := httptest.NewRequest(http.MethodGet, "/api/session", nil)
	_req.Header.Set("Origin", "https://client.example")
	_req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "lbp_session"})
	_rec := httptest.NewRecorder()

	setCORSHeaders(_rec, _req)

	if _got := _rec.Header().Get("Access-Control-Allow-Origin"); _got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want empty", _got)
	}
	if _got := _rec.Header().Get("Access-Control-Allow-Credentials"); _got != "" {
		t.Fatalf("Access-Control-Allow-Credentials = %q, want empty", _got)
	}
}

// -------------------------------------------------------------------------------------
func TestNormalizedAPIRouteStripsPathProxyPrefix(t *testing.T) {
	_cases := map[string]string{
		"/api/session":                             "/api/session",
		"/v1/chat/completions":                     "/v1/chat/completions",
		"/netpass/lbp/api/session":                 "/api/session",
		"/netpass/lbp/v1/chat/completions":         "/v1/chat/completions",
		"/netpass/lbp/api/provider-configs/":       "/api/provider-configs",
		"/netpass/lbp/api/v1/chat/completions":     "/v1/chat/completions",
		"/api/v1/api/settings/general":             "/api/settings/general",
		"/netpass/lbp/api/v1/api/settings/general": "/api/settings/general",
	}

	for _path, _want := range _cases {
		if _got := normalizedAPIRoute(_path); _got != _want {
			t.Fatalf("normalizedAPIRoute(%q) = %q, want %q", _path, _got, _want)
		}
	}
}

// -------------------------------------------------------------------------------------
func TestModelsRouteAcceptsRootModelsPath(t *testing.T) {
	for _, _route := range []string{"/models", "/v1/models", "/api/v1/models"} {
		_normalized := normalizedAPIRoute(_route)
		if !isModelsRoute(_normalized) {
			t.Fatalf("isModelsRoute(%q) = false, want true", _route)
		}
		if !isChatCompatibleRoute(http.MethodGet, _normalized) {
			t.Fatalf("isChatCompatibleRoute(GET, %q) = false, want true", _route)
		}
	}
}

// -------------------------------------------------------------------------------------
func TestModelRetrieveRouteAcceptsOpenAICompatiblePaths(t *testing.T) {
	_cases := map[string]string{
		"/models/AUTO":           "AUTO",
		"/v1/models/gpt-5.5":     "gpt-5.5",
		"/api/v1/models/gpt%205": "gpt 5",
	}
	for _route, _want := range _cases {
		_normalized := normalizedAPIRoute(_route)
		if !isModelRetrieveRoute(_normalized) {
			t.Fatalf("isModelRetrieveRoute(%q) = false, want true", _route)
		}
		if !isChatCompatibleRoute(http.MethodGet, _normalized) {
			t.Fatalf("isChatCompatibleRoute(GET, %q) = false, want true", _route)
		}
		if _got := modelIDFromRetrieveRoute(_normalized); _got != _want {
			t.Fatalf("modelIDFromRetrieveRoute(%q) = %q, want %q", _route, _got, _want)
		}
	}
}

// -------------------------------------------------------------------------------------
func TestResponsesProxySubrouteAcceptsDeleteAndCompact(t *testing.T) {
	_cases := []struct {
		Method string
		Route  string
	}{
		{http.MethodDelete, "/v1/responses/resp_123"},
		{http.MethodPost, "/v1/responses/resp_123/compact"},
		{http.MethodDelete, "/api/v1/responses/resp_123"},
		{http.MethodPost, "/api/v1/responses/resp_123/compact"},
		{http.MethodPost, "/v1/responses/compact"},
		{http.MethodPost, "/api/v1/responses/compact"},
	}
	for _, _case := range _cases {
		_normalized := normalizedAPIRoute(_case.Route)
		if !isResponsesProxySubroute(_case.Method, _normalized) {
			t.Fatalf("isResponsesProxySubroute(%s, %q) = false, want true", _case.Method, _case.Route)
		}
	}
}

// -------------------------------------------------------------------------------------
func TestNormalizedAPIRouteMapsCodexBackendPaths(t *testing.T) {
	_cases := map[string]string{
		"/backend-api/codex/models":            "/v1/models",
		"/backend-api/codex/responses":         "/v1/responses",
		"/backend-api/codex/responses/compact": "/v1/responses/compact",
	}
	for _path, _want := range _cases {
		if _got := normalizedAPIRoute(_path); _got != _want {
			t.Fatalf("normalizedAPIRoute(%q) = %q, want %q", _path, _got, _want)
		}
	}
}

// -------------------------------------------------------------------------------------
func TestCodexModelsManifestRequestDetection(t *testing.T) {
	_withVersion := httptest.NewRequest(http.MethodGet, "/v1/models?client_version=0.129.0", nil)
	if !isCodexModelsManifestRequest(_withVersion) {
		t.Fatal("client_version request should use Codex manifest")
	}
	_direct := httptest.NewRequest(http.MethodGet, "/backend-api/codex/models", nil)
	if !isCodexModelsManifestRequest(_direct) {
		t.Fatal("direct Codex models request should use Codex manifest")
	}
	_openAI := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	if isCodexModelsManifestRequest(_openAI) {
		t.Fatal("plain OpenAI models request should keep list format")
	}
}

// -------------------------------------------------------------------------------------
func TestRequestETagMatchesCodexManifestETag(t *testing.T) {
	_etag := codexModelsManifestETag([]byte(`{"models":[]}`))
	if !requestETagMatches(_etag, _etag) {
		t.Fatal("exact ETag should match")
	}
	if !requestETagMatches(`W/`+_etag, _etag) {
		t.Fatal("weak ETag should match")
	}
	if requestETagMatches(`"different"`, _etag) {
		t.Fatal("different ETag must not match")
	}
}

// -------------------------------------------------------------------------------------
func TestLocalTokenCountRoutesAreChatCompatible(t *testing.T) {
	_route := "/v1/responses/input_tokens"
	if !isLocalTokenCountRoute(_route) {
		t.Fatalf("isLocalTokenCountRoute(%q) = false", _route)
	}
	if !isChatCompatibleRoute(http.MethodPost, _route) {
		t.Fatalf("isChatCompatibleRoute(POST, %q) = false", _route)
	}
	if _tokens := estimateLocalInputTokens("中文測試 hello"); _tokens <= 0 {
		t.Fatalf("estimated input tokens = %d, want positive", _tokens)
	}
}

// -------------------------------------------------------------------------------------
func TestAPIKeyUsageRouteMatchesStatsEndpoint(t *testing.T) {
	_cases := map[string]string{
		"/api/api-keys/key-1234567890abcdef/usage":           "/api/api-keys/key-1234567890abcdef/usage",
		"/v1/api-keys/key-1234567890abcdef/usage":            "/v1/api-keys/key-1234567890abcdef/usage",
		"/netpass/lbp/api/api-keys/key-abc/usage":            "/api/api-keys/key-abc/usage",
		"/netpass/lbp/api/v1/api/api-keys/key-abc/usage":     "/api/api-keys/key-abc/usage",
		"/proxy/a/b/api/v1/api/api-keys/key.with-dash/usage": "/api/api-keys/key.with-dash/usage",
	}

	for _path, _normalized := range _cases {
		_route := normalizedAPIRoute(_path)
		if _route != _normalized {
			t.Fatalf("normalizedAPIRoute(%q) = %q, want %q", _path, _route, _normalized)
		}
		if !isAPIKeyUsageRoute(_route) {
			t.Fatalf("isAPIKeyUsageRoute(%q) = false, want true", _route)
		}
		if _id := apiKeyIDFromUsageRoute(_route); _id == "" {
			t.Fatalf("apiKeyIDFromUsageRoute(%q) returned empty id", _route)
		}
	}
}

// -------------------------------------------------------------------------------------
func TestAPIKeyUsageQueryRouteMatchesStatsEndpoint(t *testing.T) {
	_cases := map[string]string{
		"/api/api-keys/usage":                     "/api/api-keys/usage",
		"/v1/api-keys/usage":                      "/v1/api-keys/usage",
		"/netpass/lbp/api/api-keys/usage":         "/api/api-keys/usage",
		"/proxy/a/b/api/v1/api/api-keys/usage":    "/api/api-keys/usage",
		"/proxy/a/b/api/v1/api/v1/api-keys/usage": "/v1/api-keys/usage",
	}

	for _path, _normalized := range _cases {
		_route := normalizedAPIRoute(_path)
		if _route != _normalized {
			t.Fatalf("normalizedAPIRoute(%q) = %q, want %q", _path, _route, _normalized)
		}
		if !isAPIKeyUsageQueryRoute(_route) {
			t.Fatalf("isAPIKeyUsageQueryRoute(%q) = false, want true", _route)
		}
	}
}

// -------------------------------------------------------------------------------------
func TestSettingsRouteMatchesPrefixedRoutes(t *testing.T) {
	_generalRoutes := []string{
		"/api/settings/general",
		"/v1/settings/general",
		"/api/v1/settings/general",
		"/v1/api/settings/general",
		"/netpass/lbp/api/v1/settings/general",
		"/netpass/lbp/api/v1/api/settings/general",
	}
	for _, _path := range _generalRoutes {
		_route := normalizedAPIRoute(_path)
		if !isSettingsRoute(_route, "general") {
			t.Fatalf("isSettingsRoute(%q => %q, general) = false, want true", _path, _route)
		}
	}

	_notificationTestRoutes := []string{
		"/api/settings/notification/test",
		"/v1/settings/notification/test",
		"/api/v1/settings/notification/test",
		"/netpass/lbp/api/v1/settings/notification/test",
	}
	for _, _path := range _notificationTestRoutes {
		_route := normalizedAPIRoute(_path)
		if !isSettingsRoute(_route, "notification", "test") {
			t.Fatalf("isSettingsRoute(%q => %q, notification/test) = false, want true", _path, _route)
		}
	}
}

// -------------------------------------------------------------------------------------
func TestAPIKeyRouteIDParsingSupportsPrefixedRoutes(t *testing.T) {
	_cases := map[string]string{
		"/api/api-keys/key-123":                        "key-123",
		"/v1/api-keys/key-123":                         "key-123",
		"/api/v1/api/api-keys/key-123":                 "key-123",
		"/netpass/lbp/api/api-keys/key-123/disable":    "key-123",
		"/netpass/lbp/api/api-keys/key-123/usage":      "key-123",
		"/proxy/a/b/api/v1/api/api-keys/key-123/usage": "key-123",
	}

	for _route, _want := range _cases {
		_normalized := normalizedAPIRoute(_route)
		if _got := apiKeyIDFromRoute(_normalized); _got != _want {
			t.Fatalf("apiKeyIDFromRoute(%q => %q) = %q, want %q", _route, _normalized, _got, _want)
		}
	}
}

// -------------------------------------------------------------------------------------
func TestObservedReactionMSDoesNotFallbackToDuration(t *testing.T) {
	if _got := observedReactionMS(proxy.ChatMetrics{}); _got != 0 {
		t.Fatalf("observedReactionMS without first token = %.0f, want 0", _got)
	}
	if _got := observedReactionMS(proxy.ChatMetrics{FirstResponseMS: 1234}); _got != 1234 {
		t.Fatalf("observedReactionMS with first token = %.0f, want 1234", _got)
	}
}

// -------------------------------------------------------------------------------------
func TestNormalizeOpenAICodexModelID(t *testing.T) {
	if _got := normalizeOpenAICodexModelID(" GPT-5.6-Sol "); _got != "gpt-5.6-sol" {
		t.Fatalf("normalizeOpenAICodexModelID = %q, want gpt-5.6-sol", _got)
	}
}

// -------------------------------------------------------------------------------------
func TestSyncProviderModelAliasesExposesFetchedModels(t *testing.T) {
	_provider := domain.LLMProviderConfig{
		ID:      "local",
		Name:    "Local",
		Kind:    "llamacpp",
		Enabled: true,
		Models: []domain.LLMModelConfig{
			{
				Name:    "gemma-default.gguf",
				Aliases: []string{"auto", "llamacpp", "stale-model.gguf"},
			},
		},
	}

	syncProviderModelAliases(&_provider, []string{"ggml-org/gemma-4-E4B-it-GGUF", "gemma-4-12B-it-Q4_K_M.gguf"})
	_names := providerExposedModelNames(&_provider)
	_want := map[string]bool{
		"ggml-org/gemma-4-E4B-it-GGUF": false,
		"gemma-4-12B-it-Q4_K_M.gguf":   false,
	}
	for _, _name := range _names {
		if _, _ok := _want[_name]; _ok {
			_want[_name] = true
		}
		if _name == "auto" || _name == "llamacpp" {
			t.Fatalf("providerExposedModelNames leaked internal alias %q", _name)
		}
		if _name == "stale-model.gguf" {
			t.Fatalf("providerExposedModelNames kept stale fetched alias %q", _name)
		}
		if _name == "gemma-default.gguf" {
			t.Fatalf("providerExposedModelNames kept stale primary model %q", _name)
		}
	}
	for _name, _seen := range _want {
		if !_seen {
			t.Fatalf("providerExposedModelNames missing %q from %v", _name, _names)
		}
	}
	if _provider.Models[0].Name != "gemma-4-12B-it-Q4_K_M.gguf" {
		t.Fatalf("primary model = %q, want first fetched public model", _provider.Models[0].Name)
	}
}

// -------------------------------------------------------------------------------------
func TestSyncProviderModelAliasesRemovesUnavailablePublicModels(t *testing.T) {
	_provider := domain.LLMProviderConfig{
		ID:      "codex",
		Name:    "Codex",
		Kind:    "openai-codex",
		Enabled: true,
		Models: []domain.LLMModelConfig{{
			Name:    "gpt-5.6-sol",
			Aliases: []string{"auto", "openai-codex", "gpt-5.6", "gpt-5.6-sol"},
		}},
	}

	syncProviderModelAliases(&_provider, []string{"gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.5"})

	for _, _alias := range _provider.Models[0].Aliases {
		if _alias == "gpt-5.6" {
			t.Fatalf("stale unavailable alias was retained: %#v", _provider.Models[0].Aliases)
		}
	}
	if _provider.Models[0].Name != "gpt-5.6-sol" {
		t.Fatalf("provider model = %q, want existing supported model", _provider.Models[0].Name)
	}
}

// -------------------------------------------------------------------------------------
func TestMergeProviderConfigDropsPreviousPublicModelAliases(t *testing.T) {
	_old := domain.LLMProviderConfig{
		ID:      "local",
		Name:    "Local",
		Kind:    "llamacpp",
		Enabled: true,
		Models: []domain.LLMModelConfig{
			{
				Name:    "previous-model.gguf",
				Aliases: []string{"auto", "llamacpp", "old-fetched-model.gguf"},
			},
		},
	}
	_form := ProviderForm{
		ID:      "local",
		Name:    "Local",
		Kind:    "llamacpp",
		Host:    "http://127.0.0.1:8080",
		ChatAPI: "/v1/chat/completions",
		Model:   "new-model.gguf",
		Enabled: true,
	}

	_updated := mergeProviderConfig(_old, _form)
	_names := providerExposedModelNames(&_updated)
	for _, _name := range _names {
		if _name == "previous-model.gguf" || _name == "old-fetched-model.gguf" {
			t.Fatalf("providerExposedModelNames kept stale model %q in %v", _name, _names)
		}
	}
	if len(_updated.Models) == 0 || _updated.Models[0].Name != "new-model.gguf" {
		t.Fatalf("updated model = %#v, want new-model.gguf", _updated.Models)
	}
}

// -------------------------------------------------------------------------------------
// 釘住的 provider 當下不可選時要降級重新負載平衡，而不是讓選擇階段回 503。
func TestConversationAffinityDropsWhenPinnedProviderUnavailable(t *testing.T) {
	_client := proxy.NewClient()
	_handler := &HTTPAPI{Client: _client, Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{
		{ID: "provider-a", Enabled: false, BaseURL: "http://a"},
		{ID: "provider-b", Enabled: true, BaseURL: "http://b"},
	}})}

	_request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	_request = _request.WithContext(proxy.WithResponseRouteOwner(_request.Context(), "key:owner-1"))
	_client.RecordPromptCacheRoute(proxy.PromptCacheRouteID("conv-uuid"), "provider-a", "gpt-5.6-luna", proxy.ResponseRouteOwner(_request))

	_selection := domain.ChatCompletionRequest{Model: "AUTO"}
	_body := []byte(`{"model":"AUTO","prompt_cache_key":"conv-uuid","input":[{"type":"reasoning","encrypted_content":"x"}]}`)
	_pinned, _dropped := _handler.applyConversationAffinity(&_selection, _body, _request)
	if _pinned || !_dropped {
		t.Fatalf("pinned=%v dropped=%v, want the pin dropped so another provider can serve", _pinned, _dropped)
	}
	if _selection.ProviderID != "" {
		t.Fatalf("provider must be left unpinned, got %q", _selection.ProviderID)
	}
}

// -------------------------------------------------------------------------------------
// 容量冷卻改為可調：未設定時沿用預設值，設定後即時生效。
func TestProviderCapacityCooldownIsConfigurable(t *testing.T) {
	_path := filepath.Join(t.TempDir(), "advanced_settings.json")
	_handler := &HTTPAPI{Client: proxy.NewClient(), AdvancedSettingsConfigPath: _path}

	if _got := _handler.providerCapacityCooldown(); _got != defaultProviderCapacityCooldown {
		t.Fatalf("default cooldown = %s, want %s", _got, defaultProviderCapacityCooldown)
	}

	_recorder := httptest.NewRecorder()
	_handler.handleSaveAdvancedSettings(_recorder, []byte(`{"providerCapacityCooldownSeconds":3}`))
	if _recorder.Code != http.StatusOK {
		t.Fatalf("save status = %d body=%s", _recorder.Code, _recorder.Body.String())
	}
	if _got := _handler.providerCapacityCooldown(); _got != 3*time.Second {
		t.Fatalf("configured cooldown = %s, want 3s", _got)
	}

	// 超出範圍必須被擋下，且不影響既有設定。
	_recorder = httptest.NewRecorder()
	_handler.handleSaveAdvancedSettings(_recorder, []byte(`{"providerCapacityCooldownSeconds":9999}`))
	if _recorder.Code != http.StatusBadRequest {
		t.Fatalf("out-of-range status = %d, want 400", _recorder.Code)
	}
	if _got := _handler.providerCapacityCooldown(); _got != 3*time.Second {
		t.Fatalf("cooldown after rejected save = %s, want 3s preserved", _got)
	}
}

// -------------------------------------------------------------------------------------
// 重試用盡時必須用「正常完成」的串流訊息收尾，而不是 502 ——
// 502 會讓客戶端判定串流中斷並顯示「正在重新連線」。
func TestExhaustedRetriesEndWithGracefulTerminalNot502(t *testing.T) {
	_handler := &HTTPAPI{}
	_recorder := httptest.NewRecorder()
	_deferred := newDeferredResponseWriter(_recorder, true)

	// 模擬上游先回了 500 並寫入部分內容，但都還沒送出給客戶端。
	_deferred.WriteHeader(http.StatusInternalServerError)
	_, _ = _deferred.Write([]byte(`{"error":"boom"}`))

	_ok := _handler.writeGracefulStreamTerminal(_recorder, _deferred, true,
		proxy.ResponsesRefusalTerminal, errors.New("An error occurred while processing your request."))
	if !_ok {
		t.Fatal("graceful terminal should be written when nothing was committed")
	}
	if _recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (a 502 would trigger a client reconnect)", _recorder.Code)
	}
	_body := _recorder.Body.String()
	if !strings.Contains(_body, "response.completed") {
		t.Fatalf("expected a completed stream, got %q", _body)
	}
	if !strings.Contains(_body, "An error occurred while processing your request.") {
		t.Fatalf("reason should be surfaced, got %q", _body)
	}
	if strings.Contains(_body, `{"error":"boom"}`) {
		t.Fatalf("the discarded attempt must not leak into the response: %q", _body)
	}
}

// -------------------------------------------------------------------------------------
// 已經送出內容後不得再改寫收尾。
func TestGracefulStreamTerminalSkippedAfterCommit(t *testing.T) {
	_handler := &HTTPAPI{}
	_recorder := httptest.NewRecorder()
	_deferred := newDeferredResponseWriter(_recorder, true)
	_deferred.WriteHeader(http.StatusOK)
	_, _ = _deferred.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"))
	_ = _deferred.Commit()

	if _handler.writeGracefulStreamTerminal(_recorder, _deferred, true, proxy.ResponsesRefusalTerminal, errors.New("boom")) {
		t.Fatal("must not rewrite a response that was already sent")
	}
}

// -------------------------------------------------------------------------------------
// 格式由上游決定，代理負責轉碼：不可渲染的格式（例如 WebP）必須被轉成
// 客戶端顯示得出來的格式，而不是原樣送出讓客戶端印一整包 base64 JSON。
func TestCompactMCPImageTranscodesUnrenderableFormat(t *testing.T) {
	_source := image.NewRGBA(image.Rect(0, 0, 32, 32))
	for _y := 0; _y < 32; _y++ {
		for _x := 0; _x < 32; _x++ {
			_source.SetRGBA(_x, _y, color.RGBA{R: uint8(_x * 8), G: uint8(_y * 8), B: 128, A: 255})
		}
	}
	var _encoded bytes.Buffer
	if _err := png.Encode(&_encoded, _source); _err != nil {
		t.Fatal(_err)
	}

	// 內容可解碼，但宣告成客戶端渲染不出來的 mimeType。
	_bytes, _mime, _ := compactMCPImage(_encoded.Bytes(), "image/webp", mcpImagePreviewTotalMaxBytes)
	if _mime != "image/png" {
		t.Fatalf("mime = %q, want image/png after transcoding", _mime)
	}
	if _, _, _err := image.Decode(bytes.NewReader(_bytes)); _err != nil {
		t.Fatalf("transcoded image cannot be decoded: %v", _err)
	}

	// 已經可渲染且在預算內的影像不得被重新編碼。
	_same, _sameMIME, _isPreview := compactMCPImage(_encoded.Bytes(), "image/png", mcpImagePreviewTotalMaxBytes)
	if _sameMIME != "image/png" || _isPreview || !bytes.Equal(_same, _encoded.Bytes()) {
		t.Fatalf("renderable image should pass through untouched: mime=%q preview=%v", _sameMIME, _isPreview)
	}
}

// -------------------------------------------------------------------------------------
func TestRenderableMCPImageMIMEClassification(t *testing.T) {
	for _, _mime := range []string{"image/png", "image/jpeg", "image/JPG", "image/gif"} {
		if !isRenderableMCPImageMIME(_mime) {
			t.Fatalf("%s should be renderable", _mime)
		}
	}
	for _, _mime := range []string{"image/webp", "image/avif", "image/heic", ""} {
		if isRenderableMCPImageMIME(_mime) {
			t.Fatalf("%s must not be treated as renderable", _mime)
		}
	}
}

// -------------------------------------------------------------------------------------
// 複雜度只在有已驗證金鑰時記錄，且分級正確。
func TestNoteKeyRequestComplexityRecordsPerKey(t *testing.T) {
	_root := filepath.Join(t.TempDir(), "usage")
	_previous := keyusage.SetDefaultRecorderForTest(keyusage.NewRecorder(_root))
	defer keyusage.SetDefaultRecorderForTest(_previous)

	_request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	_request = _request.WithContext(context.WithValue(_request.Context(), requestAPIKeyContextKey{}, auth.APIKeyView{
		ID: "key-observed", KeyType: auth.APIKeyTypeChat,
	}))

	noteKeyRequestComplexity(_request, domain.RequestProfile{ComplexityScore: 2})
	noteKeyRequestComplexity(_request, domain.RequestProfile{ComplexityScore: 2})
	noteKeyRequestComplexity(_request, domain.RequestProfile{ComplexityScore: 8})

	_density := keyusage.DefaultRecorder().RequestDensity("key-observed", time.Minute)
	if _density.Count != 3 || _density.Low.Count != 2 || _density.High.Count != 1 {
		t.Fatalf("density = %#v", _density)
	}

	// 沒有金鑰的請求不得產生紀錄
	_anonymous := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	noteKeyRequestComplexity(_anonymous, domain.RequestProfile{ComplexityScore: 1})
	if _other := keyusage.DefaultRecorder().RequestDensity("", time.Minute); _other.Count != 0 {
		t.Fatalf("anonymous request should not be recorded: %#v", _other)
	}
}

// -------------------------------------------------------------------------------------
// 密集度端點：列出全部金鑰（含沒有流量的），依請求數排序。
func TestAPIKeyDensityEndpointListsAllKeys(t *testing.T) {
	_usageRoot := filepath.Join(t.TempDir(), "usage")
	_previousRecorder := keyusage.SetDefaultRecorderForTest(keyusage.NewRecorder(_usageRoot))
	defer keyusage.SetDefaultRecorderForTest(_previousRecorder)

	_store := auth.NewAPIKeyStore(filepath.Join(t.TempDir(), "api_keys.json"))
	_previousStore := auth.SetDefaultStoreForTest(_store)
	defer auth.SetDefaultStoreForTest(_previousStore)

	_busy, _err := _store.Create("Busy Key")
	if _err != nil {
		t.Fatal(_err)
	}
	if _, _err := _store.Create("Idle Key"); _err != nil {
		t.Fatal(_err)
	}

	_now := time.Now()
	for _idx := 0; _idx < 4; _idx++ {
		_ = keyusage.DefaultRecorder().RecordComplexity(_busy.ID, _now, 2)
	}
	_ = keyusage.DefaultRecorder().RecordRequest(_busy.ID, _now, keyusage.RequestSample{
		Complexity:       9,
		Continuation:     true,
		Fingerprint:      "repeat",
		ToolCalls:        1,
		ToolRounds:       1,
		ToolOutputTokens: 40,
	})

	_handler := &HTTPAPI{}
	_recorder := httptest.NewRecorder()
	_handler.handleGetAPIKeyDensity(_recorder, "5m")
	if _recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", _recorder.Code, _recorder.Body.String())
	}

	var _payload struct {
		WindowSeconds float64 `json:"window_seconds"`
		TotalRequests int     `json:"total_requests"`
		Keys          []struct {
			Name  string `json:"name"`
			Count int    `json:"count"`
			Low   struct {
				Count int `json:"count"`
			} `json:"low"`
			High struct {
				Count int `json:"count"`
			} `json:"high"`
			ToolCallCount     int     `json:"tool_call_count"`
			ContinuationRatio float64 `json:"continuation_ratio"`
			RepeatedTaskRatio float64 `json:"repeated_task_ratio"`
		} `json:"keys"`
	}
	if _err := json.Unmarshal(_recorder.Body.Bytes(), &_payload); _err != nil {
		t.Fatal(_err)
	}
	if _payload.WindowSeconds != 300 || _payload.TotalRequests != 5 {
		t.Fatalf("window=%v total=%d", _payload.WindowSeconds, _payload.TotalRequests)
	}
	if len(_payload.Keys) != 2 {
		t.Fatalf("keys = %#v, want both the busy and the idle key", _payload.Keys)
	}
	if _payload.Keys[0].Name != "Busy Key" || _payload.Keys[0].Count != 5 {
		t.Fatalf("busiest key should sort first: %#v", _payload.Keys[0])
	}
	if _payload.Keys[0].Low.Count != 4 || _payload.Keys[0].High.Count != 1 {
		t.Fatalf("tier split = %#v", _payload.Keys[0])
	}
	if _payload.Keys[0].ToolCallCount != 1 || _payload.Keys[0].ContinuationRatio != 0.2 {
		t.Fatalf("activity telemetry = %#v", _payload.Keys[0])
	}
	if _payload.Keys[1].Count != 0 {
		t.Fatalf("idle key should be listed with zero count: %#v", _payload.Keys[1])
	}

	// 這份清單不得洩漏任何金鑰識別資訊。
	_body := _recorder.Body.String()
	for _, _leak := range []string{_busy.ID, _busy.Prefix, "masked_key", "\"id\"", "prefix"} {
		if _leak != "" && strings.Contains(_body, _leak) {
			t.Fatalf("density response must not expose %q: %s", _leak, _body)
		}
	}
}

// -------------------------------------------------------------------------------------
// 這是管理端點：Chat 金鑰不得存取。
func TestAPIKeyDensityRouteIsAdminOnly(t *testing.T) {
	if !isAPIKeyDensityRoute("/api/api-keys/density") || !isAPIKeyDensityRoute("/v1/api-keys/density") {
		t.Fatal("density route should be recognised")
	}
	_chatKey := auth.APIKeyView{KeyType: auth.APIKeyTypeChat}
	if canAccessRoute(_chatKey, false, http.MethodGet, "/api/api-keys/density") {
		t.Fatal("chat keys must not reach the density endpoint")
	}
	_session := auth.APIKeyView{Temporary: true}
	if !canAccessRoute(_session, true, http.MethodGet, "/api/api-keys/density") {
		t.Fatal("web session should reach the density endpoint")
	}
}

// -------------------------------------------------------------------------------------
func TestParseKeyDensityWindow(t *testing.T) {
	for _input, _want := range map[string]time.Duration{
		"":      defaultKeyDensityWindow,
		"bogus": defaultKeyDensityWindow,
		"0":     defaultKeyDensityWindow,
		"300":   5 * time.Minute,
		"5m":    5 * time.Minute,
		"24h":   maxKeyDensityWindow,
		"-10":   defaultKeyDensityWindow,
		"1h30m": maxKeyDensityWindow,
		"90s":   90 * time.Second,
	} {
		if _got := parseKeyDensityWindow(_input); _got != _want {
			t.Fatalf("window %q → %s, want %s", _input, _got, _want)
		}
	}
}

// -------------------------------------------------------------------------------------
// 金鑰強制指定的 provider 是管理員政策，不得為了避開故障而擅自更換；
// 對話黏著造成的綁定則可以解除。
func TestConversationPinReleasableOnlyWhenNotKeyForced(t *testing.T) {
	_handler := &HTTPAPI{}

	_forced := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	_forced = _forced.WithContext(context.WithValue(_forced.Context(), requestAPIKeyContextKey{}, auth.APIKeyView{
		KeyType: auth.APIKeyTypeChat, ProviderID: "provider-a", Model: "AUTO", ReasoningEffort: "AUTO",
	}))
	if _handler.conversationPinIsReleasable(_forced) {
		t.Fatal("a key-forced provider must not be swapped away")
	}

	_auto := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	_auto = _auto.WithContext(context.WithValue(_auto.Context(), requestAPIKeyContextKey{}, auth.APIKeyView{
		KeyType: auth.APIKeyTypeChat, ProviderID: "AUTO", Model: "AUTO", ReasoningEffort: "AUTO",
	}))
	if !_handler.conversationPinIsReleasable(_auto) {
		t.Fatal("an affinity pin should be releasable")
	}

	if !_handler.conversationPinIsReleasable(httptest.NewRequest(http.MethodPost, "/v1/responses", nil)) {
		t.Fatal("requests without a key policy should be releasable")
	}
}

// -------------------------------------------------------------------------------------
// 綁定上限高於某個 Provider 的最大併發時要提醒，但不得阻擋儲存。
func TestAdvancedSettingsWarnsWhenBindingCapExceedsConcurrency(t *testing.T) {
	_handler := &HTTPAPI{
		Client:                     proxy.NewClient(),
		AdvancedSettingsConfigPath: filepath.Join(t.TempDir(), "advanced_settings.json"),
		Balancer: balancer.NewLoadBalancer(&domain.ProxyConfig{Providers: []domain.LLMProviderConfig{
			{ID: "roomy", Name: "Roomy", Enabled: true, MaxConcurrent: 16},
			{ID: "tight", Name: "Tight One", Enabled: true, MaxConcurrent: 4},
			{ID: "off", Name: "Disabled", Enabled: false, MaxConcurrent: 1},
		}}),
	}

	_recorder := httptest.NewRecorder()
	_handler.handleSaveAdvancedSettings(_recorder, []byte(`{"maxBindingsPerProvider":8}`))
	if _recorder.Code != http.StatusOK {
		t.Fatalf("warning must not block saving: status=%d body=%s", _recorder.Code, _recorder.Body.String())
	}

	var _payload struct {
		Warning  string               `json:"warning"`
		Advanced AdvancedSettingsForm `json:"advanced"`
	}
	if _err := json.Unmarshal(_recorder.Body.Bytes(), &_payload); _err != nil {
		t.Fatal(_err)
	}
	if _payload.Advanced.MaxBindingsPerProvider != 8 {
		t.Fatalf("value should still be saved: %#v", _payload.Advanced)
	}
	// 以「啟用中最小併發」為準，停用的 Provider 不列入
	if !strings.Contains(_payload.Warning, "Tight One") || !strings.Contains(_payload.Warning, "4") {
		t.Fatalf("warning should name the tightest enabled provider: %q", _payload.Warning)
	}

	// 調到不超過最小併發就不該再警告
	_recorder = httptest.NewRecorder()
	_handler.handleSaveAdvancedSettings(_recorder, []byte(`{"maxBindingsPerProvider":4}`))
	if _err := json.Unmarshal(_recorder.Body.Bytes(), &_payload); _err != nil {
		t.Fatal(_err)
	}
	if _payload.Warning != "" {
		t.Fatalf("no warning expected at or below the tightest concurrency: %q", _payload.Warning)
	}
}

// -------------------------------------------------------------------------------------
// 截斷的串流必須被判定為「可在送出第一個 token 前重試」，否則會直接放棄，
// 使用者看到的就是 stream closed before response.completed。
func TestTruncatedStreamIsRetryableBeforeFirstToken(t *testing.T) {
	_deferred := newDeferredResponseWriter(httptest.NewRecorder(), true)
	_err := &proxy.ProviderStreamError{
		Message:         "provider stream closed before sending a terminal event",
		TruncatedStream: true,
	}
	if !providerFailureCanRetryBeforeFirstToken(_err, _deferred) {
		t.Fatalf("a truncated stream with nothing forwarded must be retryable")
	}

	// 已經送出內容就不能重試，會重複輸出。
	_committed := newDeferredResponseWriter(httptest.NewRecorder(), true)
	if _, _writeErr := _committed.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")); _writeErr != nil {
		t.Fatal(_writeErr)
	}
	if !_committed.Committed() {
		t.Fatalf("a forwardable event should have committed the writer")
	}
	if providerFailureCanRetryBeforeFirstToken(_err, _committed) {
		t.Fatalf("a committed stream must not be retried")
	}
}

// -------------------------------------------------------------------------------------
// 串流請求選不到 provider 時不能回 HTTP 錯誤：客戶端會判定連線失敗並自動重連。
// 所有 provider 都不可用時，那會讓每一輪都變成一次「正在重新連線」。
func TestStreamSelectionFailureEndsGracefully(t *testing.T) {
	_recorder := httptest.NewRecorder()
	_deferred := newDeferredResponseWriter(_recorder, true)
	_handler := &HTTPAPI{}
	_terminal := func(_message string) []byte {
		return []byte("data: {\"type\":\"response.completed\",\"reason\":\"" + _message + "\"}\n\ndata: [DONE]\n\n")
	}

	if !_handler.writeGracefulStreamTerminal(_recorder, _deferred, true, _terminal, errors.New("no provider available")) {
		t.Fatalf("a streaming request should end with a terminal event")
	}
	if _recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (an error status triggers a client reconnect)", _recorder.Code)
	}
	_body := _recorder.Body.String()
	if !strings.Contains(_body, "response.completed") || !strings.Contains(_body, "[DONE]") {
		t.Fatalf("terminal event missing: %q", _body)
	}
	if !strings.Contains(_body, "no provider available") {
		t.Fatalf("failure reason should reach the user: %q", _body)
	}

	// 非串流請求維持原本的 JSON 錯誤。
	_plain := httptest.NewRecorder()
	if _handler.writeGracefulStreamTerminal(_plain, newDeferredResponseWriter(_plain, false), false, _terminal, errors.New("x")) {
		t.Fatalf("non-streaming requests must keep their JSON error")
	}
}

// -------------------------------------------------------------------------------------
// 換帳號重試的空檔沒有任何上游串流，內建心跳照不到；
// 客戶端等不到第一個 byte 就會斷線重連，而重連要把整個 context 重送一次。
func TestRetryKeepaliveKeepsClientAlive(t *testing.T) {
	_recorder := httptest.NewRecorder()
	_writer := newDeferredResponseWriter(_recorder, true)
	_ctx, _cancel := context.WithCancel(context.Background())
	defer _cancel()

	_stop := startRetryKeepalive(_ctx, _writer, true, proxy.ResponsesStreamHeartbeat(), nil)
	_deadline := time.Now().Add(providerRetryKeepaliveInterval * 3)
	for time.Now().Before(_deadline) && !_writer.Committed() {
		time.Sleep(50 * time.Millisecond)
	}
	_stop()

	if !_writer.Committed() {
		t.Fatalf("the keepalive should have reached the client within %v", providerRetryKeepaliveInterval*3)
	}
	if _writer.ContentWritten() {
		t.Fatalf("a keepalive must not count as delivered content — retrying has to stay possible")
	}
	if !strings.Contains(_recorder.Body.String(), "response.ping") {
		t.Fatalf("expected a ping event, got %q", _recorder.Body.String())
	}
}

// -------------------------------------------------------------------------------------
// 非串流請求不能被塞入 SSE 心跳。
func TestRetryKeepaliveSkipsNonStreamingRequests(t *testing.T) {
	_recorder := httptest.NewRecorder()
	_writer := newDeferredResponseWriter(_recorder, false)
	_stop := startRetryKeepalive(context.Background(), _writer, false, proxy.ResponsesStreamHeartbeat(), nil)
	time.Sleep(20 * time.Millisecond)
	_stop()
	if _writer.Committed() || _recorder.Body.Len() != 0 {
		t.Fatalf("a non-streaming response must stay untouched: %q", _recorder.Body.String())
	}
}
