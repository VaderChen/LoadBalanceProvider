package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/security"
)

// -------------------------------------------------------------------------------------
type Client struct {
	HTTPClient               *http.Client
	ResponseRoutes           sync.Map
	responseRouteCount       int64
	responseRouteLastSweepAt int64
	responseRouteTTLNanos    atomic.Int64
	responseRouteMaxEntries  atomic.Int64
	usageRefreshRunning      atomic.Bool
}

const (
	defaultResponseRouteTTL             = 30 * time.Minute
	responseRouteSweepInterval          = time.Minute
	defaultMaxResponseRoutes            = 2000
	streamEventBufferSize               = 128
	minimumReliableStreamMetricWindowMS = 100.0
	// 串流速率的最小可信樣本：chunk 數與時間窗都不足時不更新統計，
	// 避免短回應把儀表板數字拉得忽高忽低。
	minimumDeliverySampleChunks = 8
	// 生成窗至少要由這麼多個「帶內容的上游事件」構成。事件數太少代表整段輸出
	// 是一次沖出來的，量到的是 socket flush 而不是生成速率 —— 這種情況下窗再長
	// 也不可信，所以事件數的門檻比毫秒下限更能擋住荒謬數字。
	minimumGenerationSampleEvents     = 8
	minimumDeliveryWindowMS           = 200.0
	downstreamStreamHeartbeatInterval = 15 * time.Second
)

var (
	errProviderStreamIdleTimeout      = errors.New("provider stream idle timeout")
	errResponsesStreamMissingTerminal = errors.New("upstream response stream closed before a Responses terminal event")
)

// -------------------------------------------------------------------------------------
type ProviderStatusError struct {
	FailureDetails
	Message           string
	StatusCode        int
	ResponseForwarded bool
}

// -------------------------------------------------------------------------------------
type ProviderStreamError struct {
	FailureDetails
	Message           string
	RetryableCapacity bool
	ResponseForwarded bool
	UpstreamRejected  bool
	// TruncatedStream 表示上游沒送 terminal 事件就把串流關掉。
	// 這種故障用錯誤訊息比對認不出來（訊息是我們自己產的、狀態碼是 200），
	// 所以用旗標明確標示，讓「還沒送出內容就可以重試」的判斷認得它。
	TruncatedStream bool
}

// -------------------------------------------------------------------------------------
type ChatMetrics struct {
	CompletionTokens   int  `json:"completion_tokens"`
	EstimatedTokens    bool `json:"estimated_tokens"`
	FirstResponseMS    float64
	GenerationDuration float64
	GenerationTPS      float64
	ClientDeliveryTPS  float64
	ClientFirstWriteMS float64
	ClientLastWriteMS  float64
	ClientContentItems int
	StreamedHanChars   int
	StreamedOtherChars int
	// 以下把串流出來的字元依用途拆開。解析器本來就在看每個事件，
	// 這裡只是把既有結果分類存放，不是新增解析。
	//   Prose     ＝ 真的給人看的輸出
	//   Reasoning ＝ 推理內容／摘要
	//   Tool      ＝ 工具呼叫的參數
	// 一個只發工具呼叫的 turn，Prose 會是 0 —— 這比數工具數量更直接，
	// 而且完全不依賴工具名稱，任何客戶端都適用。
	ProseHanChars       int
	ProseOtherChars     int
	ReasoningHanChars   int
	ReasoningOtherChars int
	ToolHanChars        int
	ToolOtherChars      int
	ToolCallCount       int
	// ReportedReasoningTokens 是上游回報的推理量，精確值。
	// 字元估算取代不了它：多數 provider 根本不串流推理內容，
	// 就算開了摘要，摘要也比實際推理量小一個數量級。
	ReportedReasoningTokens int
	// ReasoningReported 表示上游「有回報這個欄位」，與值是不是 0 無關。
	// 不回報推理的模型會讓推理量恆為 0，那是「量不到」不是「不需要」——
	// 兩者混在一起會讓依推理量做的判斷全面誤判。
	ReasoningReported bool
	TotalResponseMS   float64
	ContentSeen       bool
	// ProviderContentEvents 是帶內容的上游事件數，用來判斷生成窗可不可信。
	ProviderContentEvents int
	ProviderTiming        bool
	TerminalSeen          bool
}

// -------------------------------------------------------------------------------------
type ResponsesProxyRoute struct {
	Method string
	Path   string
	Query  string
}

// -------------------------------------------------------------------------------------
type ResponseRouteTarget struct {
	ProviderID string
	Model      string
	Owner      string
	Input      interface{}
	Response   map[string]interface{}
	CreatedAt  time.Time
}

// -------------------------------------------------------------------------------------
type streamEvent struct {
	FailureDetails
	Body              string
	Suppress          bool
	HasContent        bool
	TerminalError     string
	RetryableCapacity bool
}

// -------------------------------------------------------------------------------------
type streamResult struct {
	Metrics  ChatMetrics
	DoneSeen bool
	Err      error
}

// streamFailureTerminal serializes a transport-level stream failure in the
// downstream wire format. Chat streams leave it nil and keep their [DONE]
// compatibility fallback; Responses streams must terminate with an error event.
type streamFailureTerminal func(error) []byte

// streamIdleTimeoutReader expires only when no complete SSE event arrives.
// Active long-running streams are not constrained by a total request deadline.
type streamIdleTimeoutReader struct {
	reader        io.ReadCloser
	timeout       time.Duration
	lastActivity  atomic.Int64
	lastEventType atomic.Value
	timedOut      atomic.Bool
	done          chan struct{}
	stopOnce      sync.Once
}

// -------------------------------------------------------------------------------------
func (_e *ProviderStatusError) Error() string {
	if _e.Message != "" {
		return fmt.Sprintf("provider returned status %d: %s", _e.StatusCode, _e.Message)
	}
	return fmt.Sprintf("provider returned status %d", _e.StatusCode)
}

// -------------------------------------------------------------------------------------
func (_e *ProviderStreamError) Error() string {
	if _e == nil || strings.TrimSpace(_e.Message) == "" {
		return "provider stream failed"
	}
	return _e.Message
}

// -------------------------------------------------------------------------------------
// IsTruncatedStreamError 表示上游沒送 terminal 事件就關掉串流。
// 一個字都還沒送出去的話，這種故障換一個 provider 重試對使用者是無痕的。
func IsTruncatedStreamError(_err error) bool {
	if _err == nil {
		return false
	}
	var _streamErr *ProviderStreamError
	if errors.As(_err, &_streamErr) {
		return _streamErr.TruncatedStream
	}
	return false
}

// -------------------------------------------------------------------------------------
func ResponseAlreadyForwarded(_err error) bool {
	if _err == nil {
		return false
	}
	var _providerStatusErr *ProviderStatusError
	if errors.As(_err, &_providerStatusErr) {
		return _providerStatusErr.ResponseForwarded
	}
	var _providerStreamErr *ProviderStreamError
	if errors.As(_err, &_providerStreamErr) {
		return _providerStreamErr.ResponseForwarded
	}
	return false
}

// -------------------------------------------------------------------------------------
func IsRetryableCapacityError(_err error) bool {
	if _err == nil {
		return false
	}
	var _providerStreamErr *ProviderStreamError
	if errors.As(_err, &_providerStreamErr) && _providerStreamErr.RetryableCapacity {
		return true
	}
	return providerErrorTextIsRetryableCapacity(_err.Error())
}

// -------------------------------------------------------------------------------------
func IsUpstreamRequestRejected(_err error) bool {
	var _providerStreamErr *ProviderStreamError
	return errors.As(_err, &_providerStreamErr) && _providerStreamErr.UpstreamRejected
}

// -------------------------------------------------------------------------------------
func NewClient() *Client {
	_client := &Client{
		HTTPClient: &http.Client{
			Timeout: 0,
		},
	}
	_client.ConfigureResponseRouteCache(defaultResponseRouteTTL, defaultMaxResponseRoutes)
	return _client
}

// -------------------------------------------------------------------------------------
func (_c *Client) ForwardChatCompletion(_ctx context.Context, _w http.ResponseWriter, _srcReq *http.Request, _provider *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _chatReq *domain.ChatCompletionRequest, _rawBody []byte, _profile domain.RequestProfile, _selectionMeta balancer.SelectionMeta) (ChatMetrics, error) {
	if isOpenAICodexProvider(_provider) {
		return _c.forwardOpenAICodexCompletion(_ctx, _w, _srcReq, _provider, _model, _chatReq, _profile, _selectionMeta)
	}

	_requestStarted := time.Now()
	_body, _forwardStreamUsage, _allowTextFallback, _err := rewriteRequestModel(_rawBody, _model.Name, _chatReq.Stream)
	if _err != nil {
		return ChatMetrics{}, _err
	}

	_resp, _err := _c.sendProviderRequest(_ctx, _srcReq, _provider, _model, _chatReq, _profile, _selectionMeta, _provider.Config.ChatURL(), _body)
	if _err != nil {
		return ChatMetrics{}, _err
	}
	defer _resp.Body.Close()
	_provider.RecordUsageHeaders(_resp.Header)

	if _resp.StatusCode < http.StatusOK || _resp.StatusCode >= http.StatusMultipleChoices {
		_errorBody, _ := io.ReadAll(io.LimitReader(_resp.Body, 8*1024*1024))
		if _allowTextFallback && isChatTemplateError(_errorBody) {
			_resp.Body.Close()
			_fallbackBody, _fallbackErr := rewriteTextCompletionRequest(_rawBody, _model.Name, _chatReq.Stream)
			if _fallbackErr == nil {
				_resp, _err = _c.sendProviderRequest(_ctx, _srcReq, _provider, _model, _chatReq, _profile, _selectionMeta, textCompletionURL(_provider.Config), _fallbackBody)
				if _err != nil {
					return ChatMetrics{}, _err
				}
				defer _resp.Body.Close()
				_provider.RecordUsageHeaders(_resp.Header)
				if _resp.StatusCode < http.StatusOK || _resp.StatusCode >= http.StatusMultipleChoices {
					_errorBody, _ = io.ReadAll(io.LimitReader(_resp.Body, 8*1024*1024))
				}
			}
		}
		if _resp.StatusCode < http.StatusOK || _resp.StatusCode >= http.StatusMultipleChoices {
			copyResponseHeaders(_w.Header(), _resp.Header)
			writeProxyHeaders(_w, _provider, _model, _profile, _selectionMeta, _chatReq.Stream)
			_w.WriteHeader(_resp.StatusCode)
			if len(_errorBody) > 0 {
				_, _ = _w.Write(_errorBody)
			} else {
				_, _ = io.Copy(_w, _resp.Body)
			}
			if _chatReq.Stream {
				flushResponse(_w)
			}
			return ChatMetrics{}, &ProviderStatusError{FailureDetails: FailureDetails{RetryAfter: retryAfterHeader(_resp.Header)}, StatusCode: _resp.StatusCode, ResponseForwarded: true}
		}
	}

	copyResponseHeaders(_w.Header(), _resp.Header)
	writeProxyHeaders(_w, _provider, _model, _profile, _selectionMeta, _chatReq.Stream)
	_w.WriteHeader(_resp.StatusCode)
	if _chatReq.Stream {
		flushResponse(_w)
	}

	if _chatReq.Stream {
		return streamCopyWithProviderIdleTimeout(_w, _resp.Body, _requestStarted, _forwardStreamUsage, nil, _provider, chatStreamHeartbeat(), chatRefusalTerminal, nil)
	}

	_respBody, _err := io.ReadAll(_resp.Body)
	if _err != nil {
		return ChatMetrics{}, _err
	}
	if _, _err = _w.Write(_respBody); _err != nil {
		return ChatMetrics{}, _err
	}

	_metrics := responseMetrics(_respBody)
	_metrics.finalizeTiming(time.Since(_requestStarted))
	return _metrics, nil
}

// -------------------------------------------------------------------------------------
func (_c *Client) ForwardMultimodal(_ctx context.Context, _w http.ResponseWriter, _srcReq *http.Request, _provider *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _targetURL string, _rawBody []byte, _stream bool, _profile domain.RequestProfile, _selectionMeta balancer.SelectionMeta) error {
	if isOpenAICodexProvider(_provider) && isOpenAIImageGenerationRoute(_srcReq) {
		return _c.forwardOpenAICodexImageGeneration(_ctx, _w, _srcReq, _provider, _model, _rawBody, _profile, _selectionMeta)
	}

	_body, _contentType, _err := rewriteMultimodalRequestBody(_rawBody, _srcReq.Header.Get("Content-Type"), _model.Name)
	if _err != nil {
		return _err
	}

	_resp, _err := _c.sendRawProviderRequest(_ctx, _srcReq, _provider, _model, _profile, _selectionMeta, _targetURL, _body, _contentType, _stream)
	if _err != nil {
		return _err
	}
	defer _resp.Body.Close()
	_provider.RecordUsageHeaders(_resp.Header)

	copyResponseHeaders(_w.Header(), _resp.Header)
	writeProxyHeaders(_w, _provider, _model, _profile, _selectionMeta, _stream)
	_w.WriteHeader(_resp.StatusCode)
	if _stream {
		flushResponse(_w)
	}

	var _copyErr error
	if _stream {
		_, _copyErr = copyAndFlush(_w, _resp.Body)
	} else {
		_, _copyErr = io.Copy(_w, _resp.Body)
	}
	if _resp.StatusCode < http.StatusOK || _resp.StatusCode >= http.StatusMultipleChoices {
		return &ProviderStatusError{FailureDetails: FailureDetails{RetryAfter: retryAfterHeader(_resp.Header)}, StatusCode: _resp.StatusCode, ResponseForwarded: true}
	}
	return _copyErr
}

// -------------------------------------------------------------------------------------
func (_c *Client) ForwardResponses(_ctx context.Context, _w http.ResponseWriter, _srcReq *http.Request, _provider *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _rawBody []byte, _profile domain.RequestProfile, _selectionMeta balancer.SelectionMeta) (ChatMetrics, error) {
	return _c.ForwardResponsesRoute(_ctx, _w, _srcReq, _provider, _model, ResponsesProxyRoute{Method: http.MethodPost, Path: "/v1/responses"}, _rawBody, _profile, _selectionMeta)
}

// -------------------------------------------------------------------------------------
func (_c *Client) ForwardResponsesRoute(_ctx context.Context, _w http.ResponseWriter, _srcReq *http.Request, _provider *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _route ResponsesProxyRoute, _rawBody []byte, _profile domain.RequestProfile, _selectionMeta balancer.SelectionMeta) (ChatMetrics, error) {
	if isOpenAICodexProvider(_provider) {
		return _c.forwardOpenAICodexResponsesRoute(_ctx, _w, _srcReq, _provider, _model, _route, _rawBody, _profile, _selectionMeta)
	}

	_requestStarted := time.Now()
	_body, _stream, _contentType, _err := rewriteResponsesRouteRequestBody(_route, _rawBody, _srcReq.Header.Get("Content-Type"), _model.Name, false)
	if _err != nil {
		return ChatMetrics{}, _err
	}

	_resp, _err := _c.sendRawProviderRouteRequest(_ctx, _srcReq, _provider, _model, _profile, _selectionMeta, responsesRouteURL(_provider.Config, _route), _route.Method, _body, _contentType, _stream)
	if _err != nil {
		return ChatMetrics{}, _err
	}
	defer _resp.Body.Close()
	_provider.RecordUsageHeaders(_resp.Header)

	copyResponseHeaders(_w.Header(), _resp.Header)
	writeProxyHeaders(_w, _provider, _model, _profile, _selectionMeta, _stream)
	_w.WriteHeader(_resp.StatusCode)
	if _stream {
		flushResponse(_w)
	}

	if _resp.StatusCode < http.StatusOK || _resp.StatusCode >= http.StatusMultipleChoices {
		if _stream {
			_, _ = copyAndFlush(_w, _resp.Body)
		} else {
			_, _ = io.Copy(_w, _resp.Body)
		}
		return ChatMetrics{}, &ProviderStatusError{FailureDetails: FailureDetails{RetryAfter: retryAfterHeader(_resp.Header)}, StatusCode: _resp.StatusCode, ResponseForwarded: true}
	}

	if _stream {
		return streamCopyWithProviderIdleTimeout(_w, _resp.Body, _requestStarted, true, _c.responseRouteRecorder(_route, _provider, _model, _srcReq, _body), _provider, responsesStreamHeartbeat(), responsesRefusalTerminal, responsesStreamFailureTerminal)
	}

	_respBody, _err := io.ReadAll(_resp.Body)
	if _err != nil {
		return ChatMetrics{}, _err
	}
	_c.recordResponseRouteFromBody(_route, _provider, _model, _srcReq, _body, _respBody)
	if _, _err = _w.Write(_respBody); _err != nil {
		return ChatMetrics{}, _err
	}

	_metrics := responseMetrics(_respBody)
	_metrics.finalizeTiming(time.Since(_requestStarted))
	return _metrics, nil
}

// -------------------------------------------------------------------------------------
func (_c *Client) sendProviderRequest(_ctx context.Context, _srcReq *http.Request, _provider *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _chatReq *domain.ChatCompletionRequest, _profile domain.RequestProfile, _selectionMeta balancer.SelectionMeta, _url string, _body []byte) (*http.Response, error) {
	if _err := security.ValidateOutboundURL(_url); _err != nil {
		return nil, _err
	}
	_targetReq, _err := http.NewRequestWithContext(_ctx, http.MethodPost, _url, bytes.NewReader(_body))
	if _err != nil {
		return nil, _err
	}

	_targetReq.Header.Set("Content-Type", "application/json")
	if _chatReq.Stream {
		_targetReq.Header.Set("Accept", "text/event-stream")
	} else {
		_targetReq.Header.Set("Accept", _srcReq.Header.Get("Accept"))
	}
	_targetReq.Header.Set("X-Proxy-Provider", _provider.Config.ID)
	_targetReq.Header.Set("X-Proxy-Model", _model.Name)
	_targetReq.Header.Set("X-Proxy-Task-Type", _profile.TaskType)
	_targetReq.Header.Set("X-Proxy-Strategy", _selectionMeta.Strategy)
	copyProviderPassthroughHeaders(_srcReq, _targetReq, "")

	if _apiKey := providerAPIKey(_provider); _apiKey != "" {
		_targetReq.Header.Set("Authorization", "Bearer "+_apiKey)
	}

	return doProviderHTTPRequest(_c.HTTPClient, _targetReq, providerStreamIdleTimeout(_provider))
}

// -------------------------------------------------------------------------------------
func (_c *Client) sendRawProviderRequest(_ctx context.Context, _srcReq *http.Request, _provider *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _profile domain.RequestProfile, _selectionMeta balancer.SelectionMeta, _url string, _body []byte, _contentType string, _stream bool) (*http.Response, error) {
	return _c.sendRawProviderRouteRequest(_ctx, _srcReq, _provider, _model, _profile, _selectionMeta, _url, http.MethodPost, _body, _contentType, _stream)
}

// -------------------------------------------------------------------------------------
func (_c *Client) sendRawProviderRouteRequest(_ctx context.Context, _srcReq *http.Request, _provider *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _profile domain.RequestProfile, _selectionMeta balancer.SelectionMeta, _url string, _method string, _body []byte, _contentType string, _stream bool) (*http.Response, error) {
	if strings.TrimSpace(_method) == "" {
		_method = http.MethodPost
	}
	if _err := security.ValidateOutboundURL(_url); _err != nil {
		return nil, _err
	}
	_targetReq, _err := http.NewRequestWithContext(_ctx, _method, _url, bytes.NewReader(_body))
	if _err != nil {
		return nil, _err
	}

	if strings.TrimSpace(_contentType) != "" {
		_targetReq.Header.Set("Content-Type", _contentType)
	}
	if _stream {
		_targetReq.Header.Set("Accept", "text/event-stream")
	} else if _accept := strings.TrimSpace(_srcReq.Header.Get("Accept")); _accept != "" {
		_targetReq.Header.Set("Accept", _accept)
	}
	_targetReq.Header.Set("X-Proxy-Provider", _provider.Config.ID)
	_targetReq.Header.Set("X-Proxy-Model", _model.Name)
	_targetReq.Header.Set("X-Proxy-Task-Type", _profile.TaskType)
	_targetReq.Header.Set("X-Proxy-Strategy", _selectionMeta.Strategy)
	copyProviderPassthroughHeaders(_srcReq, _targetReq, "")

	if _apiKey := providerAPIKey(_provider); _apiKey != "" {
		_targetReq.Header.Set("Authorization", "Bearer "+_apiKey)
	}

	_client := _c.HTTPClient
	if _client == nil {
		_client = &http.Client{Timeout: 0}
	}
	return doProviderHTTPRequest(_client, _targetReq, providerStreamIdleTimeout(_provider))
}

// -------------------------------------------------------------------------------------
func copyProviderPassthroughHeaders(_srcReq *http.Request, _targetReq *http.Request, _defaultOpenAIBeta string) {
	if _targetReq == nil {
		return
	}
	_headerNames := []string{
		"OpenAI-Beta",
		"OpenAI-Organization",
		"OpenAI-Project",
		"Idempotency-Key",
		"Session_id",
		"Conversation_id",
		"Originator",
		"User-Agent",
		"Version",
		"Accept-Language",
	}
	if _srcReq != nil {
		for _, _name := range _headerNames {
			_values := _srcReq.Header.Values(_name)
			if len(_values) == 0 {
				continue
			}
			_targetReq.Header.Del(_name)
			for _, _value := range _values {
				if strings.TrimSpace(_value) != "" {
					_targetReq.Header.Add(_name, _value)
				}
			}
		}
	}
	if strings.TrimSpace(_defaultOpenAIBeta) != "" && strings.TrimSpace(_targetReq.Header.Get("OpenAI-Beta")) == "" {
		_targetReq.Header.Set("OpenAI-Beta", strings.TrimSpace(_defaultOpenAIBeta))
	}
}

// -------------------------------------------------------------------------------------
func providerAPIKey(_provider *balancer.ProviderRuntime) string {
	if _provider == nil || _provider.Config == nil {
		return ""
	}
	_apiKey := _provider.Config.APIKey
	if _apiKey == "" && _provider.Config.APIKeyEnv != "" {
		_apiKey = os.Getenv(_provider.Config.APIKeyEnv)
	}
	return _apiKey
}

// -------------------------------------------------------------------------------------
func copyAndFlush(_w http.ResponseWriter, _reader io.Reader) (int64, error) {
	_buffer := make([]byte, 32*1024)
	var _written int64
	for {
		_count, _err := _reader.Read(_buffer)
		if _count > 0 {
			_writeCount, _writeErr := _w.Write(_buffer[:_count])
			_written += int64(_writeCount)
			flushResponse(_w)
			if _writeErr != nil {
				return _written, _writeErr
			}
			if _writeCount != _count {
				return _written, io.ErrShortWrite
			}
		}
		if _err != nil {
			if errors.Is(_err, io.EOF) {
				return _written, nil
			}
			return _written, _err
		}
	}
}

// -------------------------------------------------------------------------------------
func writeProxyHeaders(_w http.ResponseWriter, _provider *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _profile domain.RequestProfile, _selectionMeta balancer.SelectionMeta, _stream bool) {
	_w.Header().Set("X-Proxy-Provider", _provider.Config.ID)
	_w.Header().Set("X-Proxy-Model", proxyVisibleModelName(_provider, _model))
	_w.Header().Set("X-Proxy-Task-Type", _profile.TaskType)
	_w.Header().Set("X-Proxy-Strategy", _selectionMeta.Strategy)
	_w.Header().Set("X-Proxy-Candidate-Count", fmt.Sprintf("%d", _selectionMeta.CandidateCount))
	_w.Header().Set("X-Proxy-Selection-Reason", _selectionMeta.Reason)
	if _stream {
		_w.Header().Del("Content-Length")
		_w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		_w.Header().Set("Cache-Control", "no-cache")
		_w.Header().Set("Connection", "keep-alive")
		_w.Header().Set("X-Accel-Buffering", "no")
	}
}

// -------------------------------------------------------------------------------------
func proxyVisibleModelName(_provider *balancer.ProviderRuntime, _model *domain.LLMModelConfig) string {
	if _model == nil {
		return ""
	}
	if isOpenAICodexProvider(_provider) {
		return codexUpstreamModelName(_model.Name)
	}
	return _model.Name
}

// -------------------------------------------------------------------------------------
// decodeJSONPreservingNumbers 以 UseNumber 解析 body。改寫後的 payload 會被重新序列化，
// 若用預設的 float64 解析，超過 2^53 的整數（例如 seed、user id）會被靜默改值。
func decodeJSONPreservingNumbers(_raw []byte, _payload *map[string]interface{}) error {
	_decoder := json.NewDecoder(bytes.NewReader(_raw))
	_decoder.UseNumber()
	return _decoder.Decode(_payload)
}

// -------------------------------------------------------------------------------------
func rewriteRequestModel(_rawBody []byte, _modelName string, _stream bool) ([]byte, bool, bool, error) {
	if strings.TrimSpace(_modelName) == "" {
		return nil, false, false, errors.New("selected model is empty")
	}

	var _payload map[string]interface{}
	if _err := decodeJSONPreservingNumbers(_rawBody, &_payload); _err != nil {
		return nil, false, false, _err
	}

	_allowTextFallback := boolValue(_payload["allow_text_completion_fallback"]) || boolValue(_payload["benchmark_text_completion_fallback"])
	_payload["model"] = _modelName
	normalizeChatPayloadMultimodal(_payload)
	delete(_payload, "provider")
	delete(_payload, "provider_id")
	delete(_payload, "attachments")
	delete(_payload, "allow_text_completion_fallback")
	delete(_payload, "benchmark_text_completion_fallback")

	_forwardStreamUsage := streamUsageRequested(_payload)
	if _stream {
		ensureStreamUsage(&_payload)
	}

	_body, _err := json.Marshal(_payload)
	return _body, _forwardStreamUsage, _allowTextFallback, _err
}

// -------------------------------------------------------------------------------------
func rewriteResponsesRequestBody(_rawBody []byte, _modelName string, _forceStream bool) ([]byte, bool, error) {
	_body, _stream, _, _err := rewriteResponsesRouteRequestBody(ResponsesProxyRoute{Method: http.MethodPost, Path: "/v1/responses"}, _rawBody, "application/json", _modelName, _forceStream)
	return _body, _stream, _err
}

// -------------------------------------------------------------------------------------
func rewriteResponsesRouteRequestBody(_route ResponsesProxyRoute, _rawBody []byte, _contentType string, _modelName string, _forceStream bool) ([]byte, bool, string, error) {
	if strings.TrimSpace(_modelName) == "" {
		return nil, false, "", errors.New("selected model is empty")
	}

	_method := strings.ToUpper(strings.TrimSpace(_route.Method))
	_path := strings.TrimRight(strings.TrimSpace(_route.Path), "/")
	_body := _rawBody
	_outContentType := strings.TrimSpace(_contentType)
	_stream := false
	_shouldRewriteJSON := _method == http.MethodPost && (_path == "/v1/responses" || _path == "/v1/responses/input_tokens" || _path == "/v1/responses/compact")
	if _shouldRewriteJSON {
		var _payload map[string]interface{}
		if len(strings.TrimSpace(string(_rawBody))) > 0 {
			if _err := decodeJSONPreservingNumbers(_rawBody, &_payload); _err != nil {
				return nil, false, "", _err
			}
		} else {
			_payload = map[string]interface{}{}
		}

		_payload["model"] = _modelName
		delete(_payload, "provider")
		delete(_payload, "provider_id")
		if _forceStream && _path == "/v1/responses" {
			_payload["stream"] = true
		}

		_stream = _path == "/v1/responses" && boolValue(_payload["stream"])
		var _err error
		_body, _err = json.Marshal(_payload)
		if _err != nil {
			return nil, false, "", _err
		}
		_outContentType = "application/json"
		return _body, _stream, _outContentType, nil
	}

	if _outContentType == "" && len(strings.TrimSpace(string(_body))) > 0 {
		_outContentType = "application/json"
	}
	return _body, _stream, _outContentType, nil
}

// -------------------------------------------------------------------------------------
func (_c *Client) responseRouteRecorder(_route ResponsesProxyRoute, _provider *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _source *http.Request, _body []byte) func(string, map[string]interface{}) {
	if !responsesRouteIsCreate(_route) {
		return nil
	}
	_input := responsesRequestInputFromBody(_body)
	_owner := ResponseRouteOwner(_source)
	_promptCacheRouteID := PromptCacheRouteID(PromptCacheKeyFromBody(_body))
	return func(_responseID string, _response map[string]interface{}) {
		// Input can contain the complete Codex conversation. Persist it once and
		// retain the existing snapshot on subsequent response-only updates.
		_snapshotInput := _input
		_input = nil
		_c.RecordResponseSnapshotForOwner(_responseID, providerRuntimeID(_provider), modelRuntimeName(_model), _owner, _snapshotInput, _response)
		// 同一段對話的後續請求可能不帶 previous_response_id，只帶 prompt_cache_key，
		// 因此額外記錄一份以 cache key 為鍵的對應。
		if _promptCacheRouteID != "" {
			_c.RecordPromptCacheRoute(_promptCacheRouteID, providerRuntimeID(_provider), modelRuntimeName(_model), _owner)
		}
	}
}

// -------------------------------------------------------------------------------------
func (_c *Client) recordResponseRouteFromBody(_route ResponsesProxyRoute, _provider *balancer.ProviderRuntime, _model *domain.LLMModelConfig, _source *http.Request, _requestBody []byte, _responseBody []byte) {
	if !responsesRouteIsCreate(_route) {
		return
	}
	if _responseID, _response := responseIDAndPayloadFromJSON(_responseBody); _responseID != "" {
		_owner := ResponseRouteOwner(_source)
		_c.RecordResponseSnapshotForOwner(_responseID, providerRuntimeID(_provider), modelRuntimeName(_model), _owner, responsesRequestInputFromBody(_requestBody), _response)
		if _promptCacheRouteID := PromptCacheRouteID(PromptCacheKeyFromBody(_requestBody)); _promptCacheRouteID != "" {
			_c.RecordPromptCacheRoute(_promptCacheRouteID, providerRuntimeID(_provider), modelRuntimeName(_model), _owner)
		}
	}
}

// -------------------------------------------------------------------------------------
func responsesRequestInputFromBody(_body []byte) interface{} {
	var _payload map[string]interface{}
	if _err := json.Unmarshal(_body, &_payload); _err != nil {
		return nil
	}
	return cloneJSONValue(_payload["input"])
}

// -------------------------------------------------------------------------------------
func responsesRouteIsCreate(_route ResponsesProxyRoute) bool {
	return strings.EqualFold(strings.TrimSpace(_route.Method), http.MethodPost) && strings.TrimRight(strings.TrimSpace(_route.Path), "/") == "/v1/responses"
}

// -------------------------------------------------------------------------------------
func providerRuntimeID(_provider *balancer.ProviderRuntime) string {
	if _provider == nil || _provider.Config == nil {
		return ""
	}
	return strings.TrimSpace(_provider.Config.ID)
}

// -------------------------------------------------------------------------------------
func modelRuntimeName(_model *domain.LLMModelConfig) string {
	if _model == nil {
		return ""
	}
	return strings.TrimSpace(_model.Name)
}

// -------------------------------------------------------------------------------------
func rewriteMultimodalRequestBody(_rawBody []byte, _contentType string, _modelName string) ([]byte, string, error) {
	if strings.TrimSpace(_modelName) == "" {
		return nil, "", errors.New("selected model is empty")
	}

	_mediaType := ""
	_params := map[string]string{}
	if strings.TrimSpace(_contentType) != "" {
		if _parsedMediaType, _parsedParams, _err := mime.ParseMediaType(_contentType); _err == nil {
			_mediaType = strings.ToLower(strings.TrimSpace(_parsedMediaType))
			_params = _parsedParams
		}
	}

	if _mediaType == "multipart/form-data" {
		_body, _rewrittenContentType, _err := rewriteMultipartRequestModel(_rawBody, _params["boundary"], _modelName)
		return _body, _rewrittenContentType, _err
	}

	_trimmed := strings.TrimSpace(string(_rawBody))
	if _mediaType == "application/json" || _mediaType == "" && (strings.HasPrefix(_trimmed, "{") || strings.HasPrefix(_trimmed, "[")) {
		_body, _err := rewriteJSONRequestModel(_rawBody, _modelName)
		if _contentType == "" {
			_contentType = "application/json"
		}
		return _body, _contentType, _err
	}

	return _rawBody, _contentType, nil
}

// -------------------------------------------------------------------------------------
func rewriteJSONRequestModel(_rawBody []byte, _modelName string) ([]byte, error) {
	var _payload map[string]interface{}
	if _err := decodeJSONPreservingNumbers(_rawBody, &_payload); _err != nil {
		return nil, _err
	}
	_payload["model"] = _modelName
	delete(_payload, "provider")
	delete(_payload, "provider_id")
	return json.Marshal(_payload)
}

// -------------------------------------------------------------------------------------
func rewriteMultipartRequestModel(_rawBody []byte, _boundary string, _modelName string) ([]byte, string, error) {
	if strings.TrimSpace(_boundary) == "" {
		return nil, "", errors.New("multipart boundary is required")
	}

	_reader := multipart.NewReader(bytes.NewReader(_rawBody), _boundary)
	_buffer := &bytes.Buffer{}
	_writer := multipart.NewWriter(_buffer)
	_hasModel := false

	for {
		_part, _err := _reader.NextPart()
		if errors.Is(_err, io.EOF) {
			break
		}
		if _err != nil {
			_ = _writer.Close()
			return nil, "", _err
		}

		_name := _part.FormName()
		if _part.FileName() == "" {
			_value, _readErr := io.ReadAll(_part)
			_ = _part.Close()
			if _readErr != nil {
				_ = _writer.Close()
				return nil, "", _readErr
			}
			if _name == "provider" || _name == "provider_id" {
				continue
			}
			if _name == "model" {
				_hasModel = true
				if _writeErr := _writer.WriteField(_name, _modelName); _writeErr != nil {
					_ = _writer.Close()
					return nil, "", _writeErr
				}
				continue
			}
			if _writeErr := _writer.WriteField(_name, string(_value)); _writeErr != nil {
				_ = _writer.Close()
				return nil, "", _writeErr
			}
			continue
		}

		_header := make(textproto.MIMEHeader)
		for _key, _values := range _part.Header {
			for _, _value := range _values {
				_header.Add(_key, _value)
			}
		}
		_targetPart, _createErr := _writer.CreatePart(_header)
		if _createErr != nil {
			_ = _part.Close()
			_ = _writer.Close()
			return nil, "", _createErr
		}
		if _, _copyErr := io.Copy(_targetPart, _part); _copyErr != nil {
			_ = _part.Close()
			_ = _writer.Close()
			return nil, "", _copyErr
		}
		_ = _part.Close()
	}

	if !_hasModel {
		if _err := _writer.WriteField("model", _modelName); _err != nil {
			_ = _writer.Close()
			return nil, "", _err
		}
	}
	if _err := _writer.Close(); _err != nil {
		return nil, "", _err
	}

	return _buffer.Bytes(), _writer.FormDataContentType(), nil
}

// -------------------------------------------------------------------------------------
func rewriteTextCompletionRequest(_rawBody []byte, _modelName string, _stream bool) ([]byte, error) {
	if strings.TrimSpace(_modelName) == "" {
		return nil, errors.New("selected model is empty")
	}

	var _payload map[string]interface{}
	if _err := decodeJSONPreservingNumbers(_rawBody, &_payload); _err != nil {
		return nil, _err
	}

	_prompt := promptFromMessages(_payload["messages"])
	if strings.TrimSpace(_prompt) == "" {
		return nil, errors.New("text completion fallback requires at least one message")
	}
	_payload["model"] = _modelName
	_payload["prompt"] = _prompt
	delete(_payload, "messages")
	delete(_payload, "provider")
	delete(_payload, "provider_id")
	delete(_payload, "tools")
	delete(_payload, "tool_choice")
	delete(_payload, "response_format")
	delete(_payload, "chat_template_kwargs")
	delete(_payload, "allow_text_completion_fallback")
	delete(_payload, "benchmark_text_completion_fallback")
	if _stream {
		ensureStreamUsage(&_payload)
	}

	return json.Marshal(_payload)
}

// -------------------------------------------------------------------------------------
func normalizeChatPayloadMultimodal(_payload map[string]interface{}) {
	_imageParts := imageContentPartsFromAttachments(_payload["attachments"])
	if len(_imageParts) == 0 {
		return
	}
	if chatPayloadHasUsableImageContent(_payload["messages"]) {
		return
	}

	_messages, _ok := _payload["messages"].([]interface{})
	if !_ok || len(_messages) == 0 {
		_payload["messages"] = []interface{}{
			map[string]interface{}{
				"role":    "user",
				"content": appendOpenAIContentParts("", _imageParts),
			},
		}
		return
	}

	for _idx := len(_messages) - 1; _idx >= 0; _idx-- {
		_message, _ok := _messages[_idx].(map[string]interface{})
		if !_ok {
			continue
		}
		_role, _ := _message["role"].(string)
		if !strings.EqualFold(strings.TrimSpace(_role), "user") {
			continue
		}
		_message["content"] = appendOpenAIContentParts(_message["content"], _imageParts)
		_messages[_idx] = _message
		_payload["messages"] = _messages
		return
	}

	_messages = append(_messages, map[string]interface{}{
		"role":    "user",
		"content": appendOpenAIContentParts("", _imageParts),
	})
	_payload["messages"] = _messages
}

// -------------------------------------------------------------------------------------
func chatPayloadHasImageContent(_value interface{}) bool {
	_messages, _ok := _value.([]interface{})
	if !_ok {
		return false
	}
	for _, _item := range _messages {
		_message, _ok := _item.(map[string]interface{})
		if !_ok {
			continue
		}
		if contentPartsHaveImage(_message["content"]) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func chatPayloadHasUsableImageContent(_value interface{}) bool {
	_messages, _ok := _value.([]interface{})
	if !_ok {
		return false
	}
	for _, _item := range _messages {
		_message, _ok := _item.(map[string]interface{})
		if !_ok {
			continue
		}
		if contentPartsHaveUsableImage(_message["content"]) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func contentPartsHaveImage(_value interface{}) bool {
	switch _typed := _value.(type) {
	case []interface{}:
		for _, _item := range _typed {
			if contentPartsHaveImage(_item) {
				return true
			}
		}
	case map[string]interface{}:
		if _type := strings.ToLower(strings.TrimSpace(stringFromMap(_typed, "type"))); _type == "image_url" || _type == "input_image" || strings.Contains(_type, "image") {
			return true
		}
		for _key := range _typed {
			_key = strings.ToLower(strings.TrimSpace(_key))
			if _key == "image_url" || _key == "image" || strings.Contains(_key, "image") {
				return true
			}
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func contentPartsHaveUsableImage(_value interface{}) bool {
	switch _typed := _value.(type) {
	case []interface{}:
		for _, _item := range _typed {
			if contentPartsHaveUsableImage(_item) {
				return true
			}
		}
	case map[string]interface{}:
		if _url := imageURLFromOpenAIContentPart(_typed); imageURLIsUsable(_url) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func imageURLFromOpenAIContentPart(_part map[string]interface{}) string {
	if _imageURL, _ok := _part["image_url"].(map[string]interface{}); _ok {
		return stringFromMap(_imageURL, "url")
	}
	if _imageURL, _ok := _part["imageUrl"].(map[string]interface{}); _ok {
		return stringFromMap(_imageURL, "url")
	}
	if _image, _ok := _part["image"].(map[string]interface{}); _ok {
		return stringFromMap(_image, "url")
	}
	return stringFromMapAny(_part, "image_url", "imageUrl", "input_image", "inputImage", "image")
}

// -------------------------------------------------------------------------------------
func imageURLIsUsable(_url string) bool {
	_url = strings.ToLower(strings.TrimSpace(_url))
	return strings.HasPrefix(_url, "data:image/") || strings.HasPrefix(_url, "http://") || strings.HasPrefix(_url, "https://")
}

// -------------------------------------------------------------------------------------
func imageContentPartsFromAttachments(_value interface{}) []map[string]interface{} {
	_items, _ok := _value.([]interface{})
	if !_ok || len(_items) == 0 {
		return nil
	}
	_parts := make([]map[string]interface{}, 0, len(_items))
	for _, _item := range _items {
		_attachment, _ok := _item.(map[string]interface{})
		if !_ok {
			continue
		}
		_mediaType := strings.ToLower(strings.TrimSpace(stringFromMapAny(_attachment, "media_type", "mediaType", "type")))
		_mimeType := strings.ToLower(strings.TrimSpace(stringFromMapAny(_attachment, "mime_type", "mimeType", "mimetype", "content_type", "contentType")))
		_name := strings.ToLower(strings.TrimSpace(stringFromMapAny(_attachment, "name", "filename", "file_name", "fileName")))
		_raw := attachmentRawData(_attachment)
		if !attachmentMetadataLooksLikeImage(_mediaType, _mimeType, _name) && !attachmentPayloadLooksLikeImage(_raw) {
			continue
		}
		_url := attachmentImageDataURL(_attachment, _raw)
		if _url == "" {
			continue
		}
		_parts = append(_parts, map[string]interface{}{
			"type": "image_url",
			"image_url": map[string]interface{}{
				"url":    _url,
				"detail": "auto",
			},
		})
	}
	return _parts
}

// -------------------------------------------------------------------------------------
func attachmentMetadataLooksLikeImage(_mediaType string, _mimeType string, _name string) bool {
	_mediaType = strings.ToLower(strings.TrimSpace(_mediaType))
	_mimeType = strings.ToLower(strings.TrimSpace(_mimeType))
	_name = strings.ToLower(strings.TrimSpace(_name))
	return _mediaType == "image" || strings.HasPrefix(_mediaType, "image/") || strings.HasPrefix(_mimeType, "image/") || looksLikeImageFile(_name)
}

// -------------------------------------------------------------------------------------
func attachmentPayloadLooksLikeImage(_raw string) bool {
	_raw = strings.ToLower(strings.TrimSpace(_raw))
	if _raw == "" {
		return false
	}
	if strings.HasPrefix(_raw, "data:image/") {
		return true
	}
	if strings.HasPrefix(_raw, "http://") || strings.HasPrefix(_raw, "https://") {
		return strings.Contains(_raw, ".png") || strings.Contains(_raw, ".jpg") || strings.Contains(_raw, ".jpeg") || strings.Contains(_raw, ".webp") || strings.Contains(_raw, ".gif")
	}
	return false
}

// -------------------------------------------------------------------------------------
func appendOpenAIContentParts(_content interface{}, _imageParts []map[string]interface{}) []interface{} {
	_parts := make([]interface{}, 0, len(_imageParts)+1)
	switch _typed := _content.(type) {
	case string:
		if strings.TrimSpace(_typed) != "" {
			_parts = append(_parts, map[string]interface{}{"type": "text", "text": _typed})
		}
	case []interface{}:
		_parts = append(_parts, _typed...)
	case map[string]interface{}:
		_parts = append(_parts, _typed)
	default:
		if _text := strings.TrimSpace(fmt.Sprint(_typed)); _text != "" && _text != "<nil>" {
			_parts = append(_parts, map[string]interface{}{"type": "text", "text": _text})
		}
	}
	for _, _part := range _imageParts {
		_parts = append(_parts, _part)
	}
	return _parts
}

// -------------------------------------------------------------------------------------
func attachmentRawData(_attachment map[string]interface{}) string {
	return firstNonEmptyString(
		stringFromMapAny(_attachment, "file_data", "fileData", "base64"),
		stringFromMapAny(_attachment, "content", "data", "data_url", "dataUrl", "url"),
	)
}

// -------------------------------------------------------------------------------------
func attachmentImageDataURL(_attachment map[string]interface{}, _raw string) string {
	if strings.TrimSpace(_raw) == "" {
		return ""
	}
	_raw = strings.TrimSpace(_raw)
	_lowerRaw := strings.ToLower(_raw)
	if strings.HasPrefix(_lowerRaw, "data:image/") || strings.HasPrefix(_lowerRaw, "http://") || strings.HasPrefix(_lowerRaw, "https://") {
		return _raw
	}
	if _idx := strings.Index(_raw, ","); _idx >= 0 {
		_raw = strings.TrimSpace(_raw[_idx+1:])
	}
	_raw = strings.NewReplacer("\r", "", "\n", "", "\t", "", " ", "").Replace(_raw)
	if _raw == "" {
		return ""
	}
	_mimeType := strings.ToLower(strings.TrimSpace(stringFromMapAny(_attachment, "mime_type", "mimeType", "mimetype", "content_type", "contentType")))
	if !strings.HasPrefix(_mimeType, "image/") {
		_mimeType = "image/png"
	}
	return "data:" + _mimeType + ";base64," + _raw
}

// -------------------------------------------------------------------------------------
func looksLikeImageFile(_name string) bool {
	_name = strings.ToLower(strings.TrimSpace(_name))
	return strings.HasSuffix(_name, ".png") || strings.HasSuffix(_name, ".jpg") || strings.HasSuffix(_name, ".jpeg") || strings.HasSuffix(_name, ".webp") || strings.HasSuffix(_name, ".gif")
}

// -------------------------------------------------------------------------------------
func stringFromMap(_item map[string]interface{}, _key string) string {
	_value, _ok := _item[_key]
	if !_ok {
		return ""
	}
	_text, _ok := _value.(string)
	if _ok {
		return strings.TrimSpace(_text)
	}
	return strings.TrimSpace(fmt.Sprint(_value))
}

// -------------------------------------------------------------------------------------
func stringFromMapAny(_item map[string]interface{}, _keys ...string) string {
	for _, _key := range _keys {
		if _value := stringFromMap(_item, _key); strings.TrimSpace(_value) != "" {
			return _value
		}
	}
	return ""
}

// -------------------------------------------------------------------------------------
func firstNonEmptyString(_values ...string) string {
	for _, _value := range _values {
		if strings.TrimSpace(_value) != "" {
			return strings.TrimSpace(_value)
		}
	}
	return ""
}

// -------------------------------------------------------------------------------------
func promptFromMessages(_value interface{}) string {
	_messages, _ok := _value.([]interface{})
	if !_ok {
		return ""
	}

	var _builder strings.Builder
	for _, _item := range _messages {
		_message, _ok := _item.(map[string]interface{})
		if !_ok {
			continue
		}
		_role, _ := _message["role"].(string)
		_content := messageContentText(_message["content"])
		if strings.TrimSpace(_content) == "" {
			continue
		}
		if _role == "" {
			_role = "user"
		}
		_builder.WriteString(strings.ToUpper(_role))
		_builder.WriteString(": ")
		_builder.WriteString(_content)
		_builder.WriteString("\n")
	}
	if _builder.Len() > 0 {
		_builder.WriteString("ASSISTANT:")
	}
	return strings.TrimSpace(_builder.String())
}

// -------------------------------------------------------------------------------------
func messageContentText(_value interface{}) string {
	switch _typed := _value.(type) {
	case string:
		return _typed
	case []interface{}:
		var _builder strings.Builder
		for _, _part := range _typed {
			_partMap, _ok := _part.(map[string]interface{})
			if !_ok {
				continue
			}
			if _text, _ok := _partMap["text"].(string); _ok {
				_builder.WriteString(_text)
			}
		}
		return _builder.String()
	default:
		return ""
	}
}

// -------------------------------------------------------------------------------------
func textCompletionURL(_provider *domain.LLMProviderConfig) string {
	if _provider == nil {
		return "/v1/completions"
	}
	_base := strings.TrimRight(_provider.BaseURL, "/")
	_path := strings.TrimSpace(_provider.ChatCompletionsPath)
	if _path == "" {
		return _base + "/v1/completions"
	}
	_path = "/" + strings.TrimLeft(_path, "/")
	_completionPath := strings.Replace(_path, "/chat/completions", "/completions", 1)
	if _completionPath == _path {
		_path = "/v1/completions"
	} else {
		_path = _completionPath
	}
	return _base + _path
}

// -------------------------------------------------------------------------------------
func responsesURL(_provider *domain.LLMProviderConfig) string {
	return responsesRouteURL(_provider, ResponsesProxyRoute{Path: "/v1/responses"})
}

// -------------------------------------------------------------------------------------
func responsesRouteURL(_provider *domain.LLMProviderConfig, _route ResponsesProxyRoute) string {
	_path := strings.TrimSpace(_route.Path)
	if _path == "" {
		_path = "/v1/responses"
	}
	if !strings.HasPrefix(_path, "/") {
		_path = "/" + _path
	}
	if _provider == nil {
		return appendQuery(_path, _route.Query)
	}
	_base := strings.TrimRight(_provider.BaseURL, "/")
	return appendQuery(_base+_path, _route.Query)
}

// -------------------------------------------------------------------------------------
func appendQuery(_url string, _query string) string {
	_query = strings.TrimSpace(_query)
	if _query == "" {
		return _url
	}
	if strings.Contains(_url, "?") {
		return _url + "&" + _query
	}
	return _url + "?" + _query
}

// -------------------------------------------------------------------------------------
func isChatTemplateError(_body []byte) bool {
	_text := strings.ToLower(string(_body))
	return strings.Contains(_text, "chat template") || strings.Contains(_text, "chat_template")
}

// -------------------------------------------------------------------------------------
func boolValue(_value interface{}) bool {
	_bool, _ok := _value.(bool)
	return _ok && _bool
}

// -------------------------------------------------------------------------------------
func streamUsageRequested(_payload map[string]interface{}) bool {
	_options, _ok := _payload["stream_options"].(map[string]interface{})
	if !_ok {
		return false
	}
	_include, _ok := _options["include_usage"].(bool)
	return _ok && _include
}

// -------------------------------------------------------------------------------------
func ensureStreamUsage(_payload *map[string]interface{}) {
	if _payload == nil {
		return
	}
	_options, _ := (*_payload)["stream_options"].(map[string]interface{})
	if _options == nil {
		_options = map[string]interface{}{}
	}
	_options["include_usage"] = true
	(*_payload)["stream_options"] = _options
}

// -------------------------------------------------------------------------------------
func copyResponseHeaders(_target http.Header, _source http.Header) {
	for _key, _values := range _source {
		_lower := strings.ToLower(_key)
		if isHopByHopHeader(_lower) {
			continue
		}
		for _, _value := range _values {
			_target.Add(_key, _value)
		}
	}
}

// -------------------------------------------------------------------------------------
func isHopByHopHeader(_key string) bool {
	switch _key {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
		"te", "trailer", "transfer-encoding", "upgrade", "content-length":
		return true
	default:
		return false
	}
}

// -------------------------------------------------------------------------------------
func streamCopy(_w http.ResponseWriter, _reader io.Reader, _started time.Time, _forwardUsage bool) (ChatMetrics, error) {
	return streamCopyWithResponseRecorder(_w, _reader, _started, _forwardUsage, nil, nil, nil, nil)
}

// -------------------------------------------------------------------------------------
func streamCopyWithProviderIdleTimeout(_w http.ResponseWriter, _reader io.ReadCloser, _started time.Time, _forwardUsage bool, _recordResponseID func(string, map[string]interface{}), _provider *balancer.ProviderRuntime, _heartbeatBody []byte, _refusalTerminal func(string) []byte, _failureTerminal streamFailureTerminal) (ChatMetrics, error) {
	_idleReader := newStreamIdleTimeoutReader(_reader, providerStreamIdleTimeout(_provider))
	defer _idleReader.Stop()
	_metrics, _err := streamCopyWithResponseRecorder(_w, _idleReader, _started, _forwardUsage, _recordResponseID, _heartbeatBody, _refusalTerminal, _failureTerminal)

	_providerName := "unknown"
	if _provider != nil && _provider.Config != nil {
		_providerName = firstNonEmptyString(_provider.Config.Name, _provider.Config.ID, _providerName)
	}
	switch {
	case errors.Is(_err, errProviderStreamIdleTimeout):
		log.Printf(
			"provider stream idle timeout: provider=%s idle=%s timeout=%s last_event=%s",
			_providerName,
			_idleReader.IdleFor().Round(time.Millisecond),
			_idleReader.timeout,
			_idleReader.LastEventType(),
		)
	case IsUpstreamRequestRejected(_err):
		log.Printf(
			"provider rejected request: provider=%s elapsed=%s reason=%v",
			_providerName,
			time.Since(_started).Round(time.Millisecond),
			_err,
		)
	case _err != nil:
		// Upstream read/stream error mid-flight. Client sees the stream end abruptly
		// (e.g. Codex: "stream closed before response.completed"). Logged so the drop
		// is visible instead of silent — this is the LBP<->provider layer.
		log.Printf(
			"provider stream ended with error: provider=%s elapsed=%s content_items=%d last_event=%s err=%v",
			_providerName,
			time.Since(_started).Round(time.Millisecond),
			_metrics.ClientContentItems,
			_idleReader.LastEventType(),
			_err,
		)
	case !_metrics.TerminalSeen:
		// Upstream closed cleanly (EOF) but never sent a terminal event
		// (response.completed / [DONE] / finish_reason). This is exactly what makes a
		// Responses client report "stream closed before response.completed".
		//
		// 先前這裡只記 log 就把 _err 留成 nil，等於把截斷的串流回報成「成功」：
		//   - 呼叫端不會換 provider 重試，即使一個字都還沒送出去
		//   - provider 健康度被記成成功，一直掉串流的來源看起來完全正常
		// 因此改為回報錯誤。ResponseForwarded 決定還能不能重試：內容已經送出去的
		// 那一刻起就沒有退路，只能讓客戶端自己重試。
		log.Printf(
			"provider stream closed WITHOUT terminal event: provider=%s elapsed=%s content_items=%d last_event=%s (client will see 'stream closed before response.completed')",
			_providerName,
			time.Since(_started).Round(time.Millisecond),
			_metrics.ClientContentItems,
			_idleReader.LastEventType(),
		)
		_err = &ProviderStreamError{
			Message:           "provider stream closed before sending a terminal event",
			ResponseForwarded: _metrics.ClientContentItems > 0,
			TruncatedStream:   true,
		}
	}
	return _metrics, _err
}

// -------------------------------------------------------------------------------------
// streamEventIsTerminalMarker reports whether an SSE event marks the end of a
// generation for either wire: chat ([DONE] / finish_reason) or Responses
// (response.completed / failed / incomplete / cancelled).
func streamEventIsTerminalMarker(_body string) bool {
	if strings.Contains(_body, "[DONE]") {
		return true
	}
	for _, _terminal := range []string{"response.completed", "response.failed", "response.incomplete", "response.cancelled"} {
		if strings.Contains(_body, _terminal) {
			return true
		}
	}
	return streamEventHasFinishReason(_body)
}

// -------------------------------------------------------------------------------------
func providerStreamIdleTimeout(_provider *balancer.ProviderRuntime) time.Duration {
	if _provider != nil && _provider.Config != nil && _provider.Config.TimeoutSeconds > 0 {
		return time.Duration(_provider.Config.TimeoutSeconds) * time.Second
	}
	return time.Duration(domain.DefaultProviderTimeoutSeconds) * time.Second
}

// -------------------------------------------------------------------------------------
func newStreamIdleTimeoutReader(_reader io.ReadCloser, _timeout time.Duration) *streamIdleTimeoutReader {
	_idleReader := &streamIdleTimeoutReader{
		reader:  _reader,
		timeout: _timeout,
		done:    make(chan struct{}),
	}
	_idleReader.lastActivity.Store(time.Now().UnixNano())
	_idleReader.lastEventType.Store("stream-open")
	go _idleReader.watch()
	return _idleReader
}

// -------------------------------------------------------------------------------------
func (_r *streamIdleTimeoutReader) Read(_buffer []byte) (int, error) {
	_count, _err := _r.reader.Read(_buffer)
	if _count == 0 && _r.timedOut.Load() {
		return 0, errProviderStreamIdleTimeout
	}
	return _count, _err
}

// -------------------------------------------------------------------------------------
func (_r *streamIdleTimeoutReader) MarkStreamActivity(_eventType string) {
	if _r == nil || _r.timedOut.Load() {
		return
	}
	_eventType = strings.TrimSpace(_eventType)
	if strings.EqualFold(_eventType, "comment") || IsSSEHeartbeatEvent(_eventType) {
		return
	}
	if _eventType == "" {
		_eventType = "sse-event"
	}
	_r.lastEventType.Store(_eventType)
	_r.lastActivity.Store(time.Now().UnixNano())
}

// -------------------------------------------------------------------------------------
func (_r *streamIdleTimeoutReader) IdleFor() time.Duration {
	if _r == nil {
		return 0
	}
	return time.Since(time.Unix(0, _r.lastActivity.Load()))
}

// -------------------------------------------------------------------------------------
func (_r *streamIdleTimeoutReader) LastEventType() string {
	if _r == nil {
		return "unknown"
	}
	_eventType, _ok := _r.lastEventType.Load().(string)
	if !_ok || strings.TrimSpace(_eventType) == "" {
		return "unknown"
	}
	return _eventType
}

// -------------------------------------------------------------------------------------
func (_r *streamIdleTimeoutReader) Stop() {
	if _r == nil {
		return
	}
	_r.stopOnce.Do(func() {
		close(_r.done)
	})
}

// -------------------------------------------------------------------------------------
func (_r *streamIdleTimeoutReader) Close() error {
	if _r == nil {
		return nil
	}
	_r.Stop()
	if _r.reader == nil {
		return nil
	}
	return _r.reader.Close()
}

// -------------------------------------------------------------------------------------
func (_r *streamIdleTimeoutReader) watch() {
	if _r == nil || _r.reader == nil || _r.timeout <= 0 {
		return
	}
	_timer := time.NewTimer(_r.timeout)
	defer _timer.Stop()
	for {
		select {
		case <-_r.done:
			return
		case <-_timer.C:
			_lastActivity := time.Unix(0, _r.lastActivity.Load())
			_idleFor := time.Since(_lastActivity)
			if _idleFor < _r.timeout {
				_timer.Reset(_r.timeout - _idleFor)
				continue
			}
			_r.timedOut.Store(true)
			_ = _r.reader.Close()
			return
		}
	}
}

// -------------------------------------------------------------------------------------
func streamCopyWithResponseRecorder(_w http.ResponseWriter, _reader io.Reader, _started time.Time, _forwardUsage bool, _recordResponseID func(string, map[string]interface{}), _heartbeatBody []byte, _refusalTerminal func(string) []byte, _failureTerminal streamFailureTerminal) (ChatMetrics, error) {
	return streamCopyWithResponseRecorderHeartbeat(_w, _reader, _started, _forwardUsage, _recordResponseID, downstreamStreamHeartbeatInterval, _heartbeatBody, _refusalTerminal, _failureTerminal)
}

// -------------------------------------------------------------------------------------
func streamCopyWithResponseRecorderHeartbeat(_w http.ResponseWriter, _reader io.Reader, _started time.Time, _forwardUsage bool, _recordResponseID func(string, map[string]interface{}), _heartbeatInterval time.Duration, _heartbeatBody []byte, _refusalTerminal func(string) []byte, _failureTerminal streamFailureTerminal) (ChatMetrics, error) {
	_events := newStreamEventQueue()
	_done := make(chan struct{})
	_result := make(chan streamResult, 1)
	_writerMetrics := ChatMetrics{}
	_terminalSeen := false
	_responseSnapshot := newResponsesStreamSnapshot(_recordResponseID)
	_failureState := responseFailureState{}
	_completionRepair := completionRepair{}
	if _failureTerminal != nil {
		_originalFailureTerminal := _failureTerminal
		_failureTerminal = func(_err error) []byte {
			return _failureState.decorate(_originalFailureTerminal(_err))
		}
	}
	var _heartbeatTimer *time.Timer
	var _heartbeat <-chan time.Time
	if _heartbeatInterval > 0 {
		_heartbeatTimer = time.NewTimer(_heartbeatInterval)
		_heartbeat = _heartbeatTimer.C
		defer _heartbeatTimer.Stop()
	}
	_resetHeartbeat := func() {
		if _heartbeatTimer == nil {
			return
		}
		if !_heartbeatTimer.Stop() {
			select {
			case <-_heartbeatTimer.C:
			default:
			}
		}
		_heartbeatTimer.Reset(_heartbeatInterval)
	}
	_abort := func(_writeErr error) (ChatMetrics, error) {
		close(_done)
		if _closer, _ok := _reader.(io.Closer); _ok {
			_ = _closer.Close()
		}
		_responseSnapshot.flush()
		_resultValue := <-_result
		_resultValue.Metrics.mergeClientDelivery(_writerMetrics)
		_resultValue.Metrics.finalizeClientDelivery()
		return _resultValue.Metrics, _writeErr
	}

	go readProviderStream(_reader, _started, _forwardUsage, _events, _result, _done)

	_streamOpen := true
	for _streamOpen {
		select {
		case _event, _ok := <-_events:
			if !_ok {
				_streamOpen = false
				continue
			}
			if _event.Suppress {
				continue
			}
			_body := _completionRepair.process(_event.Body)
			_responseSnapshot.consumeEvent(_body)
			_failureState.observe(_body)
			_delivered := _writerMetrics.ClientContentItems > 0 || responseWriterContentWritten(_w)
			if _event.TerminalError != "" && _refusalTerminal != nil && !_delivered &&
				(_event.RetryableCapacity || providerErrorTextIsAuthentication(_event.TerminalError) ||
					providerErrorTextIsRetryableUpstreamFailure(_event.TerminalError)) {
				_resultValue := <-_result
				_resultValue.Metrics.mergeClientDelivery(_writerMetrics)
				_resultValue.Metrics.finalizeClientDelivery()
				return _resultValue.Metrics, &ProviderStreamError{
					FailureDetails:    _event.FailureDetails,
					Message:           _event.TerminalError,
					RetryableCapacity: _event.RetryableCapacity,
					ResponseForwarded: false,
				}
			}
			if _event.TerminalError != "" && _failureTerminal != nil && _delivered {
				if _, _writeErr := _w.Write(_failureTerminal(errors.New(_event.TerminalError))); _writeErr != nil {
					return _abort(_writeErr)
				}
				flushResponse(_w)
				_resultValue := <-_result
				_resultValue.Metrics.mergeClientDelivery(_writerMetrics)
				_resultValue.Metrics.finalizeClientDelivery()
				_resultValue.Metrics.TerminalSeen = true
				return _resultValue.Metrics, &ProviderStreamError{FailureDetails: _event.FailureDetails, Message: _event.TerminalError, ResponseForwarded: true}
			}
			if _event.TerminalError != "" && _refusalTerminal != nil && !_delivered {
				if _, _writeErr := _w.Write(_refusalTerminal(_event.TerminalError)); _writeErr != nil {
					return _abort(_writeErr)
				}
				flushResponse(_w)
				_resultValue := <-_result
				_resultValue.Metrics.mergeClientDelivery(_writerMetrics)
				_resultValue.Metrics.finalizeClientDelivery()
				_resultValue.Metrics.TerminalSeen = true
				return _resultValue.Metrics, &ProviderStreamError{
					FailureDetails:    _event.FailureDetails,
					Message:           _event.TerminalError,
					ResponseForwarded: true,
					UpstreamRejected:  providerErrorTextIsRequestRejection(_event.TerminalError),
				}
			}
			if !strings.HasSuffix(_body, "\n\n") {
				_body += "\n\n"
			}
			if _, _writeErr := _w.Write([]byte(_body)); _writeErr != nil {
				return _abort(_writeErr)
			}
			flushResponse(_w)
			_resetHeartbeat()
			if _event.HasContent {
				_writerMetrics.recordClientContentWrite(time.Since(_started))
			}
			if streamEventIsTerminalMarker(_event.Body) {
				_terminalSeen = true
			}
			if _event.TerminalError != "" {
				_resultValue := <-_result
				_resultValue.Metrics.mergeClientDelivery(_writerMetrics)
				_resultValue.Metrics.finalizeClientDelivery()
				return _resultValue.Metrics, &ProviderStreamError{
					FailureDetails:    _event.FailureDetails,
					Message:           _event.TerminalError,
					RetryableCapacity: _event.RetryableCapacity,
					ResponseForwarded: true,
				}
			}
		case <-_heartbeat:
			if _writeErr := writeDownstreamStreamHeartbeat(_w, _heartbeatBody); _writeErr != nil {
				return _abort(_writeErr)
			}
			_resetHeartbeat()
		}
	}

	_responseSnapshot.flush()
	_resultValue := <-_result
	_metrics := _resultValue.Metrics
	_metrics.mergeClientDelivery(_writerMetrics)
	_metrics.finalizeClientDelivery()
	_metrics.TerminalSeen = _terminalSeen || _resultValue.DoneSeen
	if _resultValue.Err != nil {
		if _failureTerminal != nil && !errors.Is(_resultValue.Err, context.Canceled) &&
			(_writerMetrics.ClientContentItems > 0 || responseWriterContentWritten(_w)) {
			if _, _writeErr := _w.Write(_failureTerminal(_resultValue.Err)); _writeErr != nil {
				return _metrics, _writeErr
			}
			flushResponse(_w)
			_metrics.TerminalSeen = true
			return _metrics, &ProviderStreamError{
				Message:           _resultValue.Err.Error(),
				ResponseForwarded: true,
			}
		}
		return _metrics, _resultValue.Err
	}
	if !_resultValue.DoneSeen {
		if _failureTerminal != nil {
			if _writerMetrics.ClientContentItems == 0 && !responseWriterContentWritten(_w) {
				return _metrics, errResponsesStreamMissingTerminal
			}
			if _, _writeErr := _w.Write(_failureTerminal(errResponsesStreamMissingTerminal)); _writeErr != nil {
				return _metrics, _writeErr
			}
			flushResponse(_w)
			_metrics.TerminalSeen = true
			return _metrics, &ProviderStreamError{
				Message:           errResponsesStreamMissingTerminal.Error(),
				ResponseForwarded: true,
			}
		}
		if _, _writeErr := _w.Write([]byte("\n\ndata: [DONE]\n\n")); _writeErr != nil {
			return _metrics, _writeErr
		}
		flushResponse(_w)
	}
	return _metrics, nil
}

// -------------------------------------------------------------------------------------
func responseWriterContentWritten(_w http.ResponseWriter) bool {
	_writer, _ok := _w.(interface{ ContentWritten() bool })
	return _ok && _writer.ContentWritten()
}

// -------------------------------------------------------------------------------------
func writeDownstreamStreamHeartbeat(_w http.ResponseWriter, _heartbeat []byte) error {
	if len(_heartbeat) == 0 {
		_heartbeat = []byte(": keep-alive\n\n")
	}
	if _writer, _ok := _w.(interface{ WriteStreamHeartbeat([]byte) error }); _ok {
		return _writer.WriteStreamHeartbeat(_heartbeat)
	}
	if _, _err := _w.Write(_heartbeat); _err != nil {
		return _err
	}
	flushResponse(_w)
	return nil
}

// -------------------------------------------------------------------------------------
// chatStreamHeartbeat 是串流靜默時送給 chat/completions 客戶端的保活訊息。
// 用「空 delta 的正規 chunk」而非 SSE 註解（": keep-alive"）：OpenAI 風格客戶端
// （例如 Codex CLI）的 eventsource 解析器會丟棄註解、不重置其 idle 計時器，只有真正的
// data: 事件才會重置。空 delta 對客戶端而言是 no-op，不會顯示任何內容。
// ChatStreamHeartbeat / ResponsesStreamHeartbeat 供 API 層在「換帳號重試的空檔」
// 保持客戶端連線存活。那段期間沒有任何上游串流，內建的心跳照不到。
func ChatStreamHeartbeat() []byte { return chatStreamHeartbeat() }

// -------------------------------------------------------------------------------------
func ResponsesStreamHeartbeat() []byte { return responsesStreamHeartbeat() }

// -------------------------------------------------------------------------------------
func chatStreamHeartbeat() []byte {
	return []byte("data: {\"id\":\"chatcmpl-keepalive\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"keepalive\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":null}]}\n\n")
}

// -------------------------------------------------------------------------------------
// responsesStreamHeartbeat 是串流靜默時送給 Responses API 客戶端的保活事件。
// 送一個未知事件型別；Codex 的 SSE 解析器會 dispatch（重置 idle 計時器）後，在預設
// match 分支（_ => trace!("unhandled responses event")）忽略它，不會中斷或污染回應。
func responsesStreamHeartbeat() []byte {
	return []byte("event: response.ping\ndata: {\"type\":\"response.ping\"}\n\n")
}

// -------------------------------------------------------------------------------------
// responsesStreamFailureTerminal converts an upstream transport failure into a
// standard Responses failed event. This gives Codex a deterministic terminal
// event and preserves the original failure reason instead of ending at EOF.
func responsesStreamFailureTerminal(_err error) []byte {
	_message := "upstream response stream interrupted"
	if _err != nil && strings.TrimSpace(_err.Error()) != "" {
		_message += ": " + strings.TrimSpace(_err.Error())
	}
	return ResponsesFailureTerminal(_message)
}

// ResponsesFailureTerminal 明確表示請求失敗，不偽裝成成功的 assistant 回覆。
func ResponsesFailureTerminal(reason string) []byte {
	payload, _ := json.Marshal(map[string]interface{}{
		"type": "response.failed", "sequence_number": 0,
		"response": map[string]interface{}{
			"id":     fmt.Sprintf("resp_proxy_failure_%d", time.Now().UnixNano()),
			"object": "response", "created_at": time.Now().Unix(), "status": "failed", "output": []interface{}{},
			"error": map[string]interface{}{"code": "server_error", "message": reason},
		},
	})
	return []byte("event: response.failed\ndata: " + string(payload) + "\n\n")
}

// -------------------------------------------------------------------------------------
// gracefulRefusalMessage frames an upstream rejection so the client sees the
// provider reason instead of interpreting the terminal event as a broken stream.
func gracefulRefusalMessage(_reason string) string {
	_reason = strings.TrimSpace(_reason)
	if _reason == "" {
		_reason = "the upstream provider ended the response without completing it"
	}
	return "⚠️ Upstream provider rejected or could not complete this request:\n\n" + _reason
}

// ChatRefusalTerminal 供 API 層在重試用盡後，用一則正常完成的訊息收尾。
func ChatRefusalTerminal(_reason string) []byte { return chatRefusalTerminal(_reason) }

// -------------------------------------------------------------------------------------
// ResponsesRefusalTerminal 供 API 層在重試用盡後，用一則正常完成的訊息收尾。
func ResponsesRefusalTerminal(_reason string) []byte { return responsesRefusalTerminal(_reason) }

// -------------------------------------------------------------------------------------
// chatRefusalTerminal builds a complete chat.completion stream that delivers the refusal text
// and finishes with finish_reason=stop, so the client renders it and does NOT retry (instead of
// an ambiguous error the client would spin on).
func chatRefusalTerminal(_reason string) []byte {
	_text := gracefulRefusalMessage(_reason)
	_id := fmt.Sprintf("chatcmpl-refusal-%d", time.Now().UnixNano())
	_created := time.Now().Unix()
	_chunk := func(_delta map[string]interface{}, _finish interface{}) string {
		_payload := map[string]interface{}{
			"id":      _id,
			"object":  "chat.completion.chunk",
			"created": _created,
			"model":   "load-balance-provider",
			"choices": []interface{}{map[string]interface{}{"index": 0, "delta": _delta, "finish_reason": _finish}},
		}
		_data, _ := json.Marshal(_payload)
		return "data: " + string(_data) + "\n\n"
	}
	var _builder strings.Builder
	_builder.WriteString(_chunk(map[string]interface{}{"role": "assistant", "content": _text}, nil))
	_builder.WriteString(_chunk(map[string]interface{}{}, "stop"))
	_builder.WriteString("data: [DONE]\n\n")
	return []byte(_builder.String())
}

// responsesRefusalTerminal builds a Responses API stream that delivers the refusal text and ends
// with response.completed, so a Responses client (e.g. Codex CLI) renders the reason and stops
// retrying — instead of the retryable response.failed it would spin on.
func responsesRefusalTerminal(_reason string) []byte {
	_text := gracefulRefusalMessage(_reason)
	_now := time.Now().UnixNano()
	_respID := fmt.Sprintf("resp_refusal_%d", _now)
	_msgID := fmt.Sprintf("msg_refusal_%d", _now)
	_message := map[string]interface{}{
		"id":     _msgID,
		"type":   "message",
		"status": "completed",
		"role":   "assistant",
		"content": []interface{}{map[string]interface{}{
			"type":        "output_text",
			"text":        _text,
			"annotations": []interface{}{},
		}},
	}
	_event := func(_name string, _payload map[string]interface{}) string {
		_data, _ := json.Marshal(_payload)
		return "event: " + _name + "\ndata: " + string(_data) + "\n\n"
	}
	var _builder strings.Builder
	_sequence := 0
	_appendEvent := func(_name string, _payload map[string]interface{}) {
		_payload["sequence_number"] = _sequence
		_sequence++
		_builder.WriteString(_event(_name, _payload))
	}
	_appendEvent("response.created", map[string]interface{}{
		"type": "response.created",
		"response": map[string]interface{}{
			"id": _respID, "object": "response", "status": "in_progress",
			"model": "load-balance-provider", "output": []interface{}{},
		},
	})
	_appendEvent("response.output_item.added", map[string]interface{}{
		"type":         "response.output_item.added",
		"response_id":  _respID,
		"output_index": 0,
		"item":         map[string]interface{}{"id": _msgID, "type": "message", "status": "in_progress", "role": "assistant", "content": []interface{}{}},
	})
	_appendEvent("response.output_text.delta", map[string]interface{}{
		"type": "response.output_text.delta", "response_id": _respID, "item_id": _msgID, "output_index": 0, "content_index": 0, "delta": _text,
	})
	_appendEvent("response.output_text.done", map[string]interface{}{
		"type": "response.output_text.done", "response_id": _respID, "item_id": _msgID, "output_index": 0, "content_index": 0, "text": _text,
	})
	_appendEvent("response.output_item.done", map[string]interface{}{
		"type": "response.output_item.done", "response_id": _respID, "output_index": 0, "item": _message,
	})
	_appendEvent("response.completed", map[string]interface{}{
		"type":     "response.completed",
		"response": map[string]interface{}{"id": _respID, "object": "response", "status": "completed", "output": []interface{}{_message}},
	})
	return []byte(_builder.String())
}

// -------------------------------------------------------------------------------------
func responseIDFromSSEEvent(_event string) string {
	_responseID, _ := responseIDAndPayloadFromSSEEvent(_event)
	return _responseID
}

// -------------------------------------------------------------------------------------
func responseIDAndPayloadFromSSEEvent(_event string) (string, map[string]interface{}) {
	for _, _line := range strings.Split(_event, "\n") {
		_line = strings.TrimSpace(_line)
		if !strings.HasPrefix(_line, "data:") {
			continue
		}
		_payloadText := strings.TrimSpace(strings.TrimPrefix(_line, "data:"))
		if _payloadText == "" || _payloadText == "[DONE]" {
			continue
		}
		if _responseID, _response := responseIDAndPayloadFromJSON([]byte(_payloadText)); _responseID != "" {
			return _responseID, _response
		}
	}
	return "", nil
}

// -------------------------------------------------------------------------------------
func responseIDFromJSON(_data []byte) string {
	_responseID, _ := responseIDAndPayloadFromJSON(_data)
	return _responseID
}

// -------------------------------------------------------------------------------------
func responseIDAndPayloadFromJSON(_data []byte) (string, map[string]interface{}) {
	var _payload map[string]interface{}
	if _err := json.Unmarshal(_data, &_payload); _err != nil {
		return "", nil
	}
	return responseIDAndPayloadFromPayload(_payload)
}

// -------------------------------------------------------------------------------------
func responseIDFromPayload(_payload map[string]interface{}) string {
	_responseID, _ := responseIDAndPayloadFromPayload(_payload)
	return _responseID
}

// -------------------------------------------------------------------------------------
func responseIDAndPayloadFromPayload(_payload map[string]interface{}) (string, map[string]interface{}) {
	if _payload == nil {
		return "", nil
	}
	if _response, _ok := _payload["response"].(map[string]interface{}); _ok {
		if _id := strings.TrimSpace(stringFromAny(_response["id"])); looksLikeResponseID(_id) {
			return _id, _response
		}
	}
	if _id := strings.TrimSpace(stringFromAny(_payload["id"])); looksLikeResponseID(_id) {
		return _id, _payload
	}
	for _, _key := range []string{"item"} {
		_child, _ok := _payload[_key].(map[string]interface{})
		if !_ok {
			continue
		}
		if _id, _response := responseIDAndPayloadFromPayload(_child); _id != "" {
			return _id, _response
		}
	}
	return "", nil
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
type responsesStreamSnapshot struct {
	record     func(string, map[string]interface{})
	responseID string
	response   map[string]interface{}
	dirty      bool
	routeSaved bool
}

// -------------------------------------------------------------------------------------
func newResponsesStreamSnapshot(_record func(string, map[string]interface{})) *responsesStreamSnapshot {
	return &responsesStreamSnapshot{record: _record}
}

// -------------------------------------------------------------------------------------
func (_s *responsesStreamSnapshot) consumeEvent(_event string) {
	if _s == nil || _s.record == nil {
		return
	}
	for _, _payload := range responseEventPayloads(_event) {
		_s.consumePayload(_payload)
	}
}

// -------------------------------------------------------------------------------------
func (_s *responsesStreamSnapshot) consumePayload(_payload map[string]interface{}) {
	if _payload == nil {
		return
	}
	_type := strings.TrimSpace(stringFromAny(_payload["type"]))
	if _id := strings.TrimSpace(stringFromAny(_payload["response_id"])); looksLikeResponseID(_id) {
		_s.responseID = _id
	}
	if _response, _ok := _payload["response"].(map[string]interface{}); _ok {
		_s.mergeResponse(_response)
	}
	if _item, _ok := _payload["item"].(map[string]interface{}); _ok && strings.Contains(_type, "output_item") {
		_s.upsertOutputItem(numberFieldOrNegative(_payload, "output_index"), _item)
	}
	if _part, _ok := _payload["part"].(map[string]interface{}); _ok && strings.Contains(_type, "content_part") {
		_s.upsertContentPart(numberFieldOrNegative(_payload, "output_index"), numberFieldOrZero(_payload, "content_index"), _part, false)
	}
	if strings.Contains(_type, "output_text") {
		_s.consumeOutputText(_payload, strings.HasSuffix(_type, ".done"))
	}
	_s.dirty = true
	_s.emit(responseSnapshotTerminalType(_type))
}

// -------------------------------------------------------------------------------------
func responseSnapshotTerminalType(_eventType string) bool {
	switch strings.TrimSpace(_eventType) {
	case "response.completed", "response.failed", "response.incomplete", "response.cancelled":
		return true
	default:
		return false
	}
}

// -------------------------------------------------------------------------------------
func (_s *responsesStreamSnapshot) mergeResponse(_response map[string]interface{}) {
	if _response == nil {
		return
	}
	_id := strings.TrimSpace(stringFromAny(_response["id"]))
	if looksLikeResponseID(_id) {
		_s.responseID = _id
	}
	_existingOutput := interface{}(nil)
	if _s.response != nil {
		_existingOutput = _s.response["output"]
	}
	_next := cloneJSONMap(_response)
	if len(responsesItemsSlice(_next["output"])) == 0 && len(responsesItemsSlice(_existingOutput)) > 0 {
		_next["output"] = cloneJSONValue(_existingOutput)
	}
	_s.response = _next
}

// -------------------------------------------------------------------------------------
func (_s *responsesStreamSnapshot) consumeOutputText(_payload map[string]interface{}, _done bool) {
	if _payload == nil {
		return
	}
	_outputIndex := numberFieldOrZero(_payload, "output_index")
	_contentIndex := numberFieldOrZero(_payload, "content_index")
	_part := _s.contentPart(_outputIndex, _contentIndex)
	_part["type"] = firstNonEmptyString(stringFromAny(_part["type"]), "output_text")
	if _done {
		if _text, _exists := _payload["text"]; _exists {
			_part["text"] = stringFromAny(_text)
		}
	} else if _delta := stringFromAny(_payload["delta"]); _delta != "" {
		_part["text"] = stringFromAny(_part["text"]) + _delta
	}
	if _annotations, _exists := _payload["annotations"]; _exists {
		_part["annotations"] = cloneJSONValue(_annotations)
	}
	_s.upsertContentPart(_outputIndex, _contentIndex, _part, true)
}

// -------------------------------------------------------------------------------------
func (_s *responsesStreamSnapshot) contentPart(_outputIndex int, _contentIndex int) map[string]interface{} {
	_item := _s.outputItem(_outputIndex)
	_content := responsesItemsSlice(_item["content"])
	if _contentIndex >= 0 && _contentIndex < len(_content) {
		if _part, _ok := _content[_contentIndex].(map[string]interface{}); _ok {
			return cloneJSONMap(_part)
		}
	}
	return map[string]interface{}{"type": "output_text", "text": ""}
}

// -------------------------------------------------------------------------------------
func (_s *responsesStreamSnapshot) upsertContentPart(_outputIndex int, _contentIndex int, _part map[string]interface{}, _merge bool) {
	if _part == nil {
		return
	}
	_item := _s.outputItem(_outputIndex)
	_content := responsesItemsSlice(_item["content"])
	for len(_content) <= _contentIndex {
		_content = append(_content, map[string]interface{}{"type": "output_text", "text": ""})
	}
	if _merge {
		if _existing, _ok := _content[_contentIndex].(map[string]interface{}); _ok {
			_content[_contentIndex] = mergeJSONObjects(_existing, _part)
		} else {
			_content[_contentIndex] = cloneJSONMap(_part)
		}
	} else {
		_content[_contentIndex] = cloneJSONMap(_part)
	}
	_item["content"] = _content
	_s.upsertOutputItem(_outputIndex, _item)
}

// -------------------------------------------------------------------------------------
func (_s *responsesStreamSnapshot) outputItem(_outputIndex int) map[string]interface{} {
	_s.ensureResponse()
	_output := responsesItemsSlice(_s.response["output"])
	if _outputIndex >= 0 && _outputIndex < len(_output) {
		if _item, _ok := _output[_outputIndex].(map[string]interface{}); _ok {
			return cloneJSONMap(_item)
		}
	}
	return map[string]interface{}{
		"type":    "message",
		"role":    "assistant",
		"content": []interface{}{},
	}
}

// -------------------------------------------------------------------------------------
func (_s *responsesStreamSnapshot) upsertOutputItem(_outputIndex int, _item map[string]interface{}) {
	if _item == nil {
		return
	}
	_s.ensureResponse()
	_output := responsesItemsSlice(_s.response["output"])
	if _outputIndex < 0 {
		_outputIndex = outputItemIndexByID(_output, stringFromAny(_item["id"]))
	}
	if _outputIndex < 0 {
		_output = append(_output, cloneJSONMap(_item))
		_s.response["output"] = _output
		return
	}
	for len(_output) <= _outputIndex {
		_output = append(_output, map[string]interface{}{
			"type":    "message",
			"role":    "assistant",
			"content": []interface{}{},
		})
	}
	if _existing, _ok := _output[_outputIndex].(map[string]interface{}); _ok {
		_output[_outputIndex] = mergeJSONObjects(_existing, _item)
	} else {
		_output[_outputIndex] = cloneJSONMap(_item)
	}
	_s.response["output"] = _output
}

// -------------------------------------------------------------------------------------
func (_s *responsesStreamSnapshot) ensureResponse() {
	if _s.response == nil {
		_s.response = map[string]interface{}{
			"object": "response",
			"output": []interface{}{},
		}
	}
	if _s.responseID != "" && strings.TrimSpace(stringFromAny(_s.response["id"])) == "" {
		_s.response["id"] = _s.responseID
	}
}

// -------------------------------------------------------------------------------------
func (_s *responsesStreamSnapshot) emit(_force bool) {
	if _s == nil || !_s.dirty || _s.record == nil || _s.responseID == "" || _s.response == nil {
		return
	}
	// The first snapshot establishes response_id -> Provider ownership. Further
	// deltas stay in memory and are persisted only at a terminal event or flush,
	// keeping response resource bookkeeping out of the streaming hot path.
	if !_force && _s.routeSaved {
		return
	}
	_s.ensureResponse()
	_s.record(_s.responseID, _s.response)
	_s.dirty = false
	_s.routeSaved = true
}

// -------------------------------------------------------------------------------------
func (_s *responsesStreamSnapshot) flush() {
	_s.emit(true)
}

// -------------------------------------------------------------------------------------
func outputItemIndexByID(_items []interface{}, _id string) int {
	_id = strings.TrimSpace(_id)
	if _id == "" {
		return -1
	}
	for _idx, _item := range _items {
		_itemMap, _ok := _item.(map[string]interface{})
		if !_ok {
			continue
		}
		if strings.TrimSpace(stringFromAny(_itemMap["id"])) == _id {
			return _idx
		}
	}
	return -1
}

// -------------------------------------------------------------------------------------
func numberFieldOrNegative(_source map[string]interface{}, _key string) int {
	if _source == nil {
		return -1
	}
	if _, _exists := _source[_key]; !_exists {
		return -1
	}
	return numberAsInt(_source[_key])
}

// -------------------------------------------------------------------------------------
func numberFieldOrZero(_source map[string]interface{}, _key string) int {
	if _source == nil {
		return 0
	}
	if _, _exists := _source[_key]; !_exists {
		return 0
	}
	return numberAsInt(_source[_key])
}

// -------------------------------------------------------------------------------------
func mergeJSONObjects(_base map[string]interface{}, _override map[string]interface{}) map[string]interface{} {
	_result := cloneJSONMap(_base)
	if _result == nil {
		_result = map[string]interface{}{}
	}
	for _key, _value := range _override {
		if _value == nil {
			continue
		}
		if _key == "content" {
			if len(responsesItemsSlice(_value)) == 0 && len(responsesItemsSlice(_result[_key])) > 0 {
				continue
			}
		}
		_result[_key] = cloneJSONValue(_value)
	}
	return _result
}

// -------------------------------------------------------------------------------------
func looksLikeResponseID(_id string) bool {
	_id = strings.TrimSpace(_id)
	return strings.HasPrefix(_id, "resp_") || strings.HasPrefix(_id, "response_")
}

// -------------------------------------------------------------------------------------
func cloneJSONValue(_value interface{}) interface{} {
	if _value == nil {
		return nil
	}
	_data, _err := json.Marshal(_value)
	if _err != nil {
		return _value
	}
	var _cloned interface{}
	if _err := json.Unmarshal(_data, &_cloned); _err != nil {
		return _value
	}
	return _cloned
}

// -------------------------------------------------------------------------------------
func cloneJSONMap(_value map[string]interface{}) map[string]interface{} {
	if _value == nil {
		return nil
	}
	_cloned, _ok := cloneJSONValue(_value).(map[string]interface{})
	if !_ok {
		return _value
	}
	return _cloned
}

// -------------------------------------------------------------------------------------
func newStreamEventQueue() chan streamEvent {
	return make(chan streamEvent, streamEventBufferSize)
}

// -------------------------------------------------------------------------------------
func readProviderStream(_reader io.Reader, _started time.Time, _forwardUsage bool, _events chan<- streamEvent, _result chan<- streamResult, _done <-chan struct{}) {
	defer close(_events)

	_buffered := bufio.NewReaderSize(_reader, 32*1024)
	_doneSeen := false
	_metrics := ChatMetrics{}
	var _pendingEvent strings.Builder
	_emit := func() bool {
		_event := _pendingEvent.String()
		_pendingEvent.Reset()
		if strings.TrimSpace(_event) == "" {
			return true
		}
		markStreamActivity(_reader, _event)
		return _metrics.mergeProviderEvent(_event, _started, &_doneSeen, _forwardUsage, _events, _done)
	}

	for {
		_line, _err := _buffered.ReadString('\n')
		if _line != "" {
			_pendingEvent.WriteString(_line)
			if _line == "\n" || _line == "\r\n" {
				if !_emit() {
					_metrics.finalizeTiming(time.Since(_started))
					_result <- streamResult{Metrics: _metrics, DoneSeen: _doneSeen}
					return
				}
			}
		}

		if _err != nil {
			if _err == io.EOF {
				if _pendingEvent.Len() > 0 && !_emit() {
					_metrics.finalizeTiming(time.Since(_started))
					_result <- streamResult{Metrics: _metrics, DoneSeen: _doneSeen}
					return
				}
				_metrics.finalizeTiming(time.Since(_started))
				_result <- streamResult{Metrics: _metrics, DoneSeen: _doneSeen}
				return
			}
			_metrics.finalizeTiming(time.Since(_started))
			_result <- streamResult{Metrics: _metrics, DoneSeen: _doneSeen, Err: _err}
			return
		}
	}
}

// -------------------------------------------------------------------------------------
func markStreamActivity(_reader io.Reader, _event string) {
	_eventType, _ok := streamEventActivityType(_event)
	if !_ok {
		return
	}
	_marker, _ok := _reader.(interface{ MarkStreamActivity(string) })
	if _ok {
		_marker.MarkStreamActivity(_eventType)
	}
}

// -------------------------------------------------------------------------------------
func streamEventActivityType(_event string) (string, bool) {
	_eventType := ""
	_hasData := false
	_hasComment := false
	for _, _line := range strings.Split(strings.ReplaceAll(_event, "\r\n", "\n"), "\n") {
		_line = strings.TrimSpace(_line)
		if _line == "" {
			continue
		}
		if strings.HasPrefix(_line, ":") {
			_hasComment = true
			continue
		}
		if strings.HasPrefix(_line, "event:") {
			_eventType = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(_line, "event:")))
			continue
		}
		if !strings.HasPrefix(_line, "data:") {
			return "stream-data", true
		}
		_payloadText := strings.TrimSpace(strings.TrimPrefix(_line, "data:"))
		if _payloadText == "" {
			continue
		}
		_hasData = true
		if _payloadText == "[DONE]" {
			return "done", true
		}
		var _payload map[string]interface{}
		if _err := json.Unmarshal([]byte(_payloadText), &_payload); _err == nil {
			_type := strings.ToLower(strings.TrimSpace(stringFromAny(_payload["type"])))
			if _type != "" {
				return _type, true
			}
		}
	}
	if _eventType != "" {
		return _eventType, true
	}
	if _hasData {
		return "data", true
	}
	if _hasComment {
		return "comment", true
	}
	return "", false
}

// -------------------------------------------------------------------------------------
func nextSSEEvent(_input string) (string, string, bool) {
	_lfIdx := strings.Index(_input, "\n\n")
	_crlfIdx := strings.Index(_input, "\r\n\r\n")
	switch {
	case _lfIdx >= 0 && (_crlfIdx < 0 || _lfIdx < _crlfIdx):
		return _input[:_lfIdx+2], _input[_lfIdx+2:], true
	case _crlfIdx >= 0:
		return _input[:_crlfIdx+4], _input[_crlfIdx+4:], true
	}
	return "", _input, false
}

// -------------------------------------------------------------------------------------
func (_m *ChatMetrics) mergeProviderEvent(_event string, _started time.Time, _doneSeen *bool, _forwardUsage bool, _events chan<- streamEvent, _done <-chan struct{}) bool {
	_eventMetrics := streamEventMetrics(_event)
	if _m.FirstResponseMS <= 0 && _eventMetrics.ContentSeen {
		_eventMetrics.FirstResponseMS = durationMilliseconds(time.Since(_started))
	}
	_m.merge(_eventMetrics)
	_terminalError := responsesTerminalErrorMessage(_event)
	if _terminalError != "" || strings.Contains(_event, "[DONE]") || streamEventHasResponsesTerminalEvent(_event) {
		*_doneSeen = true
	}
	// OpenAI-compatible providers send the optional usage/timings chunk after the
	// finish_reason chunk. Keep reading when the caller requested that final chunk.
	_terminalEvent := *_doneSeen || (streamEventHasFinishReason(_event) && !_forwardUsage)
	_streamEvent := streamEvent{
		FailureDetails:    failureDetailsFromEvent(_event),
		Body:              _event,
		Suppress:          !_forwardUsage && isUsageOnlyEvent(_event),
		HasContent:        _eventMetrics.ContentSeen,
		TerminalError:     _terminalError,
		RetryableCapacity: ClassifyFailure(&ProviderStreamError{FailureDetails: failureDetailsFromEvent(_event), Message: _terminalError}).Capacity,
	}
	select {
	case <-_done:
		return false
	case _events <- _streamEvent:
		return !_terminalEvent
	}
}

// -------------------------------------------------------------------------------------
func responsesTerminalErrorMessage(_event string) string {
	_eventType := ""
	_payloads := responseEventPayloads(_event)
	for _, _line := range strings.Split(_event, "\n") {
		_line = strings.TrimSpace(_line)
		if strings.HasPrefix(_line, "event:") {
			_eventType = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(_line, "event:")))
			break
		}
	}
	for _, _payload := range _payloads {
		_type := strings.ToLower(strings.TrimSpace(firstNonEmpty(stringFromAny(_payload["type"]), _eventType)))
		if !responsePayloadIndicatesFailure(_payload, _type) {
			continue
		}
		if _message := responsePayloadErrorMessage(_payload); _message != "" {
			return _message
		}
		if _type != "" {
			return _type
		}
		return "provider response stream failed"
	}
	if responseEventTypeIsFailure(_eventType) {
		return _eventType
	}
	return ""
}

// -------------------------------------------------------------------------------------
func responseEventTypeIsFailure(_eventType string) bool {
	switch strings.ToLower(strings.TrimSpace(_eventType)) {
	case "response.failed", "response.incomplete", "error":
		return true
	default:
		return false
	}
}

// -------------------------------------------------------------------------------------
func responsePayloadIndicatesFailure(_payload map[string]interface{}, _eventType string) bool {
	if responseEventTypeIsFailure(_eventType) {
		return true
	}
	if _payload == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(stringFromAny(_payload["status"]))) {
	case "failed", "incomplete":
		return true
	}
	for _, _key := range []string{"error", "last_error"} {
		if _message := responseErrorValueMessage(_payload[_key]); _message != "" {
			return true
		}
	}
	if strings.TrimSpace(stringFromAny(_payload["detail"])) != "" {
		return true
	}
	if _response, _ok := _payload["response"].(map[string]interface{}); _ok {
		return responsePayloadIndicatesFailure(_response, strings.TrimSpace(stringFromAny(_response["type"])))
	}
	return false
}

// -------------------------------------------------------------------------------------
func responseEventPayloads(_event string) []map[string]interface{} {
	_payloads := []map[string]interface{}{}
	for _, _frame := range ParseSSEDataFrames(_event) {
		_payloadText := strings.TrimSpace(_frame.Data)
		if _payloadText == "" || _payloadText == "[DONE]" {
			continue
		}
		var _payload map[string]interface{}
		if _err := json.Unmarshal([]byte(_payloadText), &_payload); _err == nil {
			if _payload == nil {
				continue
			}
			if stringFromAny(_payload["type"]) == "" && _frame.Event != "" && _frame.Event != "message" {
				_payload["type"] = _frame.Event
			}
			_payloads = append(_payloads, _payload)
		}
	}
	return _payloads
}

// -------------------------------------------------------------------------------------
func responsePayloadErrorMessage(_payload map[string]interface{}) string {
	if _payload == nil {
		return ""
	}
	for _, _key := range []string{"message", "detail"} {
		if _message := strings.TrimSpace(stringFromAny(_payload[_key])); _message != "" {
			return _message
		}
	}
	for _, _key := range []string{"error", "last_error", "incomplete_details"} {
		if _message := responseErrorValueMessage(_payload[_key]); _message != "" {
			return _message
		}
	}
	if _response, _ok := _payload["response"].(map[string]interface{}); _ok {
		return responsePayloadErrorMessage(_response)
	}
	return ""
}

// -------------------------------------------------------------------------------------
func responseErrorValueMessage(_value interface{}) string {
	switch _typed := _value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(_typed)
	case map[string]interface{}:
		for _, _key := range []string{"message", "detail", "code", "type", "reason"} {
			if _message := strings.TrimSpace(stringFromAny(_typed[_key])); _message != "" {
				return _message
			}
		}
		return responsePayloadErrorMessage(_typed)
	default:
		return strings.TrimSpace(fmt.Sprint(_typed))
	}
}

// -------------------------------------------------------------------------------------
func providerErrorTextIsRetryableCapacity(_text string) bool {
	_text = strings.ToLower(strings.TrimSpace(_text))
	if _text == "" {
		return false
	}
	_patterns := []string{
		"at capacity",
		"model is currently overloaded",
		"server is overloaded",
		"overloaded",
		"temporarily unavailable",
		"try again later",
		"too many requests",
		"rate limit",
		"rate_limit",
		"resource exhausted",
	}
	for _, _pattern := range _patterns {
		if strings.Contains(_text, _pattern) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func providerErrorTextIsAuthentication(_text string) bool {
	_text = strings.ToLower(strings.TrimSpace(_text))
	for _, _pattern := range []string{
		"status 401", "status 403", "unauthorized", "authentication", "invalid api key",
		"invalid token", "token expired", "token_revoked", "token_invalidated",
	} {
		if strings.Contains(_text, _pattern) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
// providerErrorTextIsRetryableUpstreamFailure 辨識上游「暫時性、可重試」的一般性錯誤。
// OpenAI 這類回應本身就寫著 "You can retry your request"，而且不帶任何可判讀的原因，
// 與內容政策拒絕完全不同 —— 重試（通常是同一個帳號）多半就會成功，
// 不該直接把錯誤訊息丟給使用者。
func providerErrorTextIsRetryableUpstreamFailure(_text string) bool {
	_text = strings.ToLower(strings.TrimSpace(_text))
	if _text == "" || providerErrorTextIsRequestRejection(_text) {
		return false
	}
	for _, _pattern := range []string{
		"you can retry your request",
		"an error occurred while processing your request",
		"internal server error",
		"internal_error",
		"server_error",
		"please try again",
	} {
		if strings.Contains(_text, _pattern) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func providerErrorTextIsRequestRejection(_text string) bool {
	_text = strings.ToLower(strings.TrimSpace(_text))
	for _, _pattern := range []string{
		"content policy", "content filter", "content_filter", "safety", "policy violation",
		"unsafe", "flagged", "blocked", "disallowed", "moderation", "invalid_prompt",
		"cybersecurity risk", "sensitive content", "jailbreak", "refused",
	} {
		if strings.Contains(_text, _pattern) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func streamEventHasResponsesTerminalEvent(_event string) bool {
	for _, _line := range strings.Split(_event, "\n") {
		_line = strings.TrimSpace(_line)
		if strings.HasPrefix(_line, "event:") {
			switch strings.TrimSpace(strings.TrimPrefix(_line, "event:")) {
			case "response.completed", "response.failed", "response.incomplete", "response.cancelled":
				return true
			}
			continue
		}
		if !strings.HasPrefix(_line, "data:") {
			continue
		}
		_payloadText := strings.TrimSpace(strings.TrimPrefix(_line, "data:"))
		if _payloadText == "" || _payloadText == "[DONE]" {
			continue
		}
		var _payload map[string]interface{}
		if _err := json.Unmarshal([]byte(_payloadText), &_payload); _err != nil {
			continue
		}
		switch strings.TrimSpace(stringFromAny(_payload["type"])) {
		case "response.completed", "response.failed", "response.incomplete", "response.cancelled":
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func streamEventHasFinishReason(_event string) bool {
	for _, _line := range strings.Split(_event, "\n") {
		_line = strings.TrimSpace(_line)
		if !strings.HasPrefix(_line, "data:") {
			continue
		}
		_payloadText := strings.TrimSpace(strings.TrimPrefix(_line, "data:"))
		if _payloadText == "" || _payloadText == "[DONE]" {
			continue
		}
		var _payload map[string]interface{}
		if _err := json.Unmarshal([]byte(_payloadText), &_payload); _err != nil {
			continue
		}
		_choices, _ok := _payload["choices"].([]interface{})
		if !_ok {
			continue
		}
		for _, _choice := range _choices {
			_choiceMap, _ok := _choice.(map[string]interface{})
			if !_ok {
				continue
			}
			if _finishReason, _exists := _choiceMap["finish_reason"]; _exists && _finishReason != nil && strings.TrimSpace(fmt.Sprint(_finishReason)) != "" {
				return true
			}
		}
	}
	return false
}

// -------------------------------------------------------------------------------------
func streamEventMetrics(_event string) ChatMetrics {
	_metrics := ChatMetrics{}
	for _, _payload := range responseEventPayloads(_event) {
		_encoded, _err := json.Marshal(_payload)
		if _err == nil {
			_metrics.merge(streamDataMetrics("data: " + string(_encoded)))
		}
	}
	return _metrics
}

// -------------------------------------------------------------------------------------
func isUsageOnlyEvent(_event string) bool {
	for _, _line := range strings.Split(_event, "\n") {
		_line = strings.TrimSpace(_line)
		if !strings.HasPrefix(_line, "data:") {
			continue
		}
		_payloadText := strings.TrimSpace(strings.TrimPrefix(_line, "data:"))
		if _payloadText == "" || _payloadText == "[DONE]" {
			continue
		}
		var _payload map[string]interface{}
		if _err := json.Unmarshal([]byte(_payloadText), &_payload); _err != nil {
			continue
		}
		if _payload["usage"] == nil {
			continue
		}
		_choices, _hasChoices := _payload["choices"].([]interface{})
		return !_hasChoices || len(_choices) == 0
	}
	return false
}

// -------------------------------------------------------------------------------------
// ProseTokens 是「真的給人看的」輸出量估算。工具呼叫參數與推理內容都不算在內，
// 所以一個只發工具呼叫的 turn 會回傳 0。
func (_m ChatMetrics) ProseTokens() int {
	return estimateTokensFromCounts(_m.ProseHanChars, _m.ProseOtherChars)
}

// -------------------------------------------------------------------------------------
// ReasoningTokens 優先回傳上游回報的精確值，沒有回報時才退回字元估算。
func (_m ChatMetrics) ReasoningTokens() int {
	if _m.ReportedReasoningTokens > 0 {
		return _m.ReportedReasoningTokens
	}
	return estimateTokensFromCounts(_m.ReasoningHanChars, _m.ReasoningOtherChars)
}

// -------------------------------------------------------------------------------------
// ToolTokens 是工具呼叫參數的量估算。
func (_m ChatMetrics) ToolTokens() int {
	return estimateTokensFromCounts(_m.ToolHanChars, _m.ToolOtherChars)
}

// -------------------------------------------------------------------------------------
func (_m *ChatMetrics) merge(_other ChatMetrics) {
	if _m.FirstResponseMS <= 0 && _other.FirstResponseMS > 0 {
		_m.FirstResponseMS = _other.FirstResponseMS
	}
	if _other.ContentSeen {
		_m.ContentSeen = true
	}
	_m.ProviderContentEvents += _other.ProviderContentEvents
	if _other.StreamedHanChars > 0 || _other.StreamedOtherChars > 0 {
		_m.StreamedHanChars += _other.StreamedHanChars
		_m.StreamedOtherChars += _other.StreamedOtherChars
	}
	_m.ProseHanChars += _other.ProseHanChars
	_m.ProseOtherChars += _other.ProseOtherChars
	_m.ReasoningHanChars += _other.ReasoningHanChars
	_m.ReasoningOtherChars += _other.ReasoningOtherChars
	_m.ToolHanChars += _other.ToolHanChars
	_m.ToolOtherChars += _other.ToolOtherChars
	_m.ToolCallCount += _other.ToolCallCount
	// usage 在串流裡通常只出現一次且是總量，相加會重複計算。
	if _other.ReportedReasoningTokens > _m.ReportedReasoningTokens {
		_m.ReportedReasoningTokens = _other.ReportedReasoningTokens
	}
	if _other.ReasoningReported {
		_m.ReasoningReported = true
	}
	if _other.CompletionTokens > 0 {
		if _other.EstimatedTokens {
			if _m.EstimatedTokens || _m.CompletionTokens <= 0 {
				_m.CompletionTokens = estimateTokensFromCounts(_m.StreamedHanChars, _m.StreamedOtherChars)
				_m.EstimatedTokens = true
			}
		} else {
			_m.CompletionTokens = _other.CompletionTokens
			_m.EstimatedTokens = false
		}
	}
	if _other.ProviderTiming {
		_m.ProviderTiming = true
	}
	if _m.GenerationDuration <= 0 && _other.GenerationDuration > 0 {
		_m.GenerationDuration = _other.GenerationDuration
	}
	if _m.GenerationTPS <= 0 && _other.GenerationTPS > 0 {
		_m.GenerationTPS = _other.GenerationTPS
	}
	if _m.TotalResponseMS <= 0 && _other.TotalResponseMS > 0 {
		_m.TotalResponseMS = _other.TotalResponseMS
	}
}

// -------------------------------------------------------------------------------------
func (_m *ChatMetrics) finalizeTiming(_elapsed time.Duration) {
	_totalMS := durationMilliseconds(_elapsed)
	_m.TotalResponseMS = _totalMS
	if _m.GenerationDuration > 0 {
		return
	}
	if _m.FirstResponseMS > 0 && _totalMS > _m.FirstResponseMS {
		_m.GenerationDuration = _totalMS - _m.FirstResponseMS
		return
	}
	_m.GenerationDuration = _totalMS
}

// -------------------------------------------------------------------------------------
func durationMilliseconds(_duration time.Duration) float64 {
	if _duration <= 0 {
		return 0
	}
	return float64(_duration) / float64(time.Millisecond)
}

// -------------------------------------------------------------------------------------
func (_m *ChatMetrics) recordClientContentWrite(_elapsed time.Duration) {
	_elapsedMS := durationMilliseconds(_elapsed)
	if _elapsedMS <= 0 {
		return
	}
	if _m.ClientFirstWriteMS <= 0 {
		_m.ClientFirstWriteMS = _elapsedMS
	}
	_m.ClientLastWriteMS = _elapsedMS
	_m.ClientContentItems++
}

// -------------------------------------------------------------------------------------
func (_m *ChatMetrics) mergeClientDelivery(_other ChatMetrics) {
	if _other.ClientFirstWriteMS > 0 && (_m.ClientFirstWriteMS <= 0 || _other.ClientFirstWriteMS < _m.ClientFirstWriteMS) {
		_m.ClientFirstWriteMS = _other.ClientFirstWriteMS
	}
	if _other.ClientLastWriteMS > _m.ClientLastWriteMS {
		_m.ClientLastWriteMS = _other.ClientLastWriteMS
	}
	_m.ClientContentItems += _other.ClientContentItems
}

// -------------------------------------------------------------------------------------
func (_m *ChatMetrics) finalizeClientDelivery() {
	_tokens := _m.deliveryTokenCount()
	if _tokens <= 0 {
		return
	}
	// 樣本太小的請求代表不了串流速率。與其換一套公式硬算出一個跳動的數字，
	// 不如完全不更新統計，讓 EWMA 維持既有值。
	if _m.ClientContentItems < minimumDeliverySampleChunks {
		return
	}
	// 時間窗從第一個 token 出現（TTFT）起算，與生成速度同基準。
	// 若改用「第一次寫出 → 最後一次寫出」，N 個 chunk 只有 N−1 個間隔，
	// 卻仍計入全部 token，短回應會被系統性高估（也是輸出速度會反超生成速度的原因）。
	_deliveryMS := _m.ClientLastWriteMS - _m.deliveryWindowStartMS()
	if _deliveryMS < minimumDeliveryWindowMS {
		return
	}
	_rate := float64(_tokens) / (_deliveryMS / 1000)
	// token 必須先生成才能送出，送出速度不可能高於生成速度。
	if _generationRate := _m.generationRate(); _generationRate > 0 && _rate > _generationRate {
		_rate = _generationRate
	}
	_m.ClientDeliveryTPS = _rate
}

// -------------------------------------------------------------------------------------
func (_m ChatMetrics) deliveryWindowStartMS() float64 {
	if _m.FirstResponseMS > 0 {
		return _m.FirstResponseMS
	}
	return _m.ClientFirstWriteMS
}

// -------------------------------------------------------------------------------------
// generationRate 與輸出速度共用分子，時間窗同樣自 TTFT 起算，兩者才能互相比較。
func (_m ChatMetrics) generationRate() float64 {
	_tokens := _m.deliveryTokenCount()
	if _tokens <= 0 {
		return 0
	}
	_windowMS := _m.wallClockGenerationWindowMS()
	if _windowMS <= 0 {
		return 0
	}
	return float64(_tokens) / (_windowMS / 1000)
}

// -------------------------------------------------------------------------------------
// wallClockGenerationWindowMS 一律使用代理端觀測到的時間（TTFT → 回應結束）。
// provider 自報的解碼時間走的是模型內部時鐘，不含排隊／prompt eval／網路，
// 與輸出速度不可比；顯示用的速度統一用牆鐘，兩個數字才有相同基準。
func (_m ChatMetrics) wallClockGenerationWindowMS() float64 {
	if _m.TotalResponseMS > _m.FirstResponseMS {
		return _m.TotalResponseMS - _m.FirstResponseMS
	}
	return _m.GenerationDuration
}

// -------------------------------------------------------------------------------------
// ProviderReportedGenerationTPS 是 provider 自報的解碼速度（模型內部時鐘）。
// 顯示用的生成速度改採牆鐘後，這個值仍保留下來供 provider 明細比較硬體／模型效能。
func (_m ChatMetrics) ProviderReportedGenerationTPS() float64 {
	if _m.GenerationTPS > 0 {
		return _m.GenerationTPS
	}
	if _m.ProviderTiming && _m.GenerationDuration > 0 && _m.CompletionTokens > 0 {
		return float64(_m.CompletionTokens) / (_m.GenerationDuration / 1000)
	}
	return 0
}

// -------------------------------------------------------------------------------------
func (_m ChatMetrics) deliveryTokenCount() int {
	// Upstream usage is tokenizer-accurate and includes reasoning output. Character
	// estimation remains the fallback for providers that omit usage in streaming mode.
	if _m.CompletionTokens > 0 && !_m.EstimatedTokens {
		return _m.CompletionTokens
	}
	return _m.streamedOutputTokens()
}

// -------------------------------------------------------------------------------------
func (_m ChatMetrics) streamedOutputTokens() int {
	return estimateTokensFromCounts(_m.StreamedHanChars, _m.StreamedOtherChars)
}

// -------------------------------------------------------------------------------------
func (_m ChatMetrics) TokenGenerationSpeed(_fallback time.Duration) float64 {
	if _m.CompletionTokens <= 0 {
		return 0
	}
	// 一律以代理牆鐘為準，與輸出速度同一時鐘。
	_generationMS := _m.wallClockGenerationWindowMS()
	_fallbackMS := durationMilliseconds(_fallback)
	// 極短的窗多半是上游思考很久、最後一次沖出整段回應，量到的是 socket flush
	// 而非生成速率。這個判斷與 provider 有沒有自報時間無關 —— 我們量的是牆鐘，
	// 先前保留 ProviderTiming 豁免是舊設計（窗取自 provider 自報時間）的遺留，
	// 會讓這類請求算出上千 tok/s 的荒謬數字。
	if _generationMS <= 0 || (_generationMS < minimumReliableStreamMetricWindowMS && _fallbackMS > _generationMS) {
		_generationMS = _fallbackMS
	}
	// 串流回應若只由少數幾個事件構成，代表上游想了很久、最後一次把整段沖出來。
	// 這時窗量到的是沖出的瞬間，不是生成速率 —— 毫秒下限擋不住（500ms 也會過），
	// 必須改看事件數。非串流回應沒有事件計數，維持原本行為。
	if _m.ProviderContentEvents > 0 && _m.ProviderContentEvents < minimumGenerationSampleEvents && _fallbackMS > _generationMS {
		_generationMS = _fallbackMS
	}
	if _generationMS > 0 {
		return float64(_m.CompletionTokens) / (_generationMS / 1000)
	}
	if _m.GenerationTPS > 0 {
		return _m.GenerationTPS
	}
	return 0
}

// -------------------------------------------------------------------------------------
func responseMetrics(_body []byte) ChatMetrics {
	var _payload map[string]interface{}
	if _err := json.Unmarshal(_body, &_payload); _err != nil {
		return ChatMetrics{}
	}

	_text := responseText(_payload)
	_tokens := responseCompletionTokens(_payload)
	if _tokens <= 0 && strings.TrimSpace(_text) == "" {
		return ChatMetrics{}
	}
	_generationDuration := responseGenerationDurationMS(_payload)
	_generationTPS := responseGenerationTPS(_payload)
	_parts := responseParts(_payload)
	_metrics := ChatMetrics{
		CompletionTokens:        _tokens,
		FirstResponseMS:         responseFirstTokenMS(_payload),
		GenerationDuration:      _generationDuration,
		GenerationTPS:           _generationTPS,
		ContentSeen:             strings.TrimSpace(_text) != "",
		ProviderTiming:          _generationDuration > 0 || _generationTPS > 0,
		ToolCallCount:           _parts.ToolCalls,
		ReportedReasoningTokens: payloadReasoningTokens(_payload),
		ReasoningReported:       payloadReportsReasoning(_payload),
	}
	_metrics.ProseHanChars, _metrics.ProseOtherChars = tokenCharCounts(_parts.Prose)
	_metrics.ReasoningHanChars, _metrics.ReasoningOtherChars = tokenCharCounts(_parts.Reasoning)
	_metrics.ToolHanChars, _metrics.ToolOtherChars = tokenCharCounts(_parts.Tool)
	if _metrics.CompletionTokens <= 0 {
		_metrics.CompletionTokens = estimateTokens(_text)
		_metrics.EstimatedTokens = true
	}
	return _metrics
}

// -------------------------------------------------------------------------------------
// responseParts 是非串流回應的用途拆分。串流走事件型別，這裡走輸出項目的 type。
func responseParts(_payload map[string]interface{}) streamTextParts {
	var _prose, _reasoning, _tool strings.Builder
	_calls := appendChoiceParts(&_prose, &_reasoning, &_tool, _payload)
	appendTextFields(&_prose, _payload, "output_text", "text")
	_calls += appendOutputItems(&_prose, &_reasoning, &_tool, _payload["output"])
	if _nested, _ok := _payload["response"].(map[string]interface{}); _ok {
		_inner := responseParts(_nested)
		_prose.WriteString(_inner.Prose)
		_reasoning.WriteString(_inner.Reasoning)
		_tool.WriteString(_inner.Tool)
		_calls += _inner.ToolCalls
	}
	return streamTextParts{
		Prose:     _prose.String(),
		Reasoning: _reasoning.String(),
		Tool:      _tool.String(),
		ToolCalls: _calls,
	}
}

// -------------------------------------------------------------------------------------
// appendOutputItems 走 Responses API 的 output 陣列，依項目型別分流。
func appendOutputItems(_prose *strings.Builder, _reasoning *strings.Builder, _tool *strings.Builder, _value interface{}) int {
	_items, _ok := _value.([]interface{})
	if !_ok {
		return 0
	}

	_calls := 0
	for _, _raw := range _items {
		_item, _ok := _raw.(map[string]interface{})
		if !_ok {
			continue
		}
		_type := strings.ToLower(strings.TrimSpace(stringFromAny(_item["type"])))
		switch {
		case strings.HasSuffix(_type, "_call"):
			_calls++
			appendTextFields(_tool, _item, "arguments", "input")
		case strings.Contains(_type, "reasoning"):
			appendTextValue(_reasoning, _item["summary"])
			appendTextFields(_reasoning, _item, "content", "text")
		default:
			appendTextValue(_prose, _item["content"])
			appendTextFields(_prose, _item, "text", "output_text")
		}
	}
	return _calls
}

// -------------------------------------------------------------------------------------
func streamDataMetrics(_line string) ChatMetrics {
	_line = strings.TrimSpace(_line)
	if !strings.HasPrefix(_line, "data:") {
		return ChatMetrics{}
	}

	_payloadText := strings.TrimSpace(strings.TrimPrefix(_line, "data:"))
	if _payloadText == "" || _payloadText == "[DONE]" {
		return ChatMetrics{}
	}

	var _payload map[string]interface{}
	if _err := json.Unmarshal([]byte(_payloadText), &_payload); _err != nil {
		return ChatMetrics{}
	}

	_generationDuration := responseGenerationDurationMS(_payload)
	_generationTPS := responseGenerationTPS(_payload)
	_metrics := ChatMetrics{
		FirstResponseMS:    responseFirstTokenMS(_payload),
		GenerationDuration: _generationDuration,
		GenerationTPS:      _generationTPS,
		ProviderTiming:     _generationDuration > 0 || _generationTPS > 0,
	}
	if _tokens := responseCompletionTokens(_payload); _tokens > 0 {
		_metrics.CompletionTokens = _tokens
	}

	_metrics.ReportedReasoningTokens = payloadReasoningTokens(_payload)
	_metrics.ReasoningReported = payloadReportsReasoning(_payload)
	_parts := streamResponseParts(_payload)
	_metrics.ToolCallCount = _parts.ToolCalls
	_metrics.ProseHanChars, _metrics.ProseOtherChars = tokenCharCounts(_parts.Prose)
	_metrics.ReasoningHanChars, _metrics.ReasoningOtherChars = tokenCharCounts(_parts.Reasoning)
	_metrics.ToolHanChars, _metrics.ToolOtherChars = tokenCharCounts(_parts.Tool)

	_text := _parts.Prose + _parts.Reasoning + _parts.Tool
	if strings.TrimSpace(_text) == "" {
		return _metrics
	}
	_hanChars, _otherChars := tokenCharCounts(_text)
	_metrics.ContentSeen = true
	_metrics.ProviderContentEvents = 1
	_metrics.StreamedHanChars = _hanChars
	_metrics.StreamedOtherChars = _otherChars
	if _metrics.CompletionTokens <= 0 {
		_metrics.CompletionTokens = estimateTokensFromCounts(_hanChars, _otherChars)
		_metrics.EstimatedTokens = true
	}
	return _metrics
}

// -------------------------------------------------------------------------------------
func responseCompletionTokens(_payload map[string]interface{}) int {
	if _tokens := usageCompletionTokens(_payload); _tokens > 0 {
		return _tokens
	}
	for _, _timings := range responseTimingMaps(_payload) {
		if _tokens := firstPositiveInt(_timings, "predicted_n", "completion_tokens", "output_tokens"); _tokens > 0 {
			return _tokens
		}
	}
	if _response, _ok := _payload["response"].(map[string]interface{}); _ok {
		return responseCompletionTokens(_response)
	}
	return 0
}

// -------------------------------------------------------------------------------------
func usageCompletionTokens(_payload map[string]interface{}) int {
	_usage, _ok := _payload["usage"].(map[string]interface{})
	if !_ok {
		return 0
	}

	_completionTokens := firstPositiveInt(_usage, "completion_tokens", "output_tokens", "generated_tokens")
	if _completionTokens > 0 {
		// OpenAI 的 reasoning_tokens 是 output/completion tokens 的子集合，不可重複相加。
		return _completionTokens
	}
	return reasoningTokenCount(_usage)
}

// -------------------------------------------------------------------------------------
// payloadReportsReasoning 檢查 usage 裡「有沒有這個欄位」，不看值。
// 回報 0 代表這輪沒有推理；完全不回報代表這個模型量不到，兩者必須分開。
func payloadReportsReasoning(_payload map[string]interface{}) bool {
	if _usage, _ok := _payload["usage"].(map[string]interface{}); _ok {
		for _, _key := range []string{"reasoning_tokens", "reasoning_output_tokens"} {
			if _, _ok := _usage[_key]; _ok {
				return true
			}
		}
		for _, _key := range []string{"completion_tokens_details", "output_tokens_details"} {
			_details, _ok := _usage[_key].(map[string]interface{})
			if !_ok {
				continue
			}
			for _, _field := range []string{"reasoning_tokens", "reasoning"} {
				if _, _ok := _details[_field]; _ok {
					return true
				}
			}
		}
	}
	if _nested, _ok := _payload["response"].(map[string]interface{}); _ok {
		return payloadReportsReasoning(_nested)
	}
	return false
}

// -------------------------------------------------------------------------------------
// payloadReasoningTokens 從 usage 取出上游回報的推理量，並跟著 response 巢狀往下找。
func payloadReasoningTokens(_payload map[string]interface{}) int {
	if _usage, _ok := _payload["usage"].(map[string]interface{}); _ok {
		if _tokens := reasoningTokenCount(_usage); _tokens > 0 {
			return _tokens
		}
	}
	if _nested, _ok := _payload["response"].(map[string]interface{}); _ok {
		return payloadReasoningTokens(_nested)
	}
	return 0
}

// -------------------------------------------------------------------------------------
func reasoningTokenCount(_usage map[string]interface{}) int {
	_reasoningTokens := firstPositiveInt(_usage, "reasoning_tokens", "reasoning_output_tokens")
	for _, _key := range []string{"completion_tokens_details", "output_tokens_details"} {
		_details, _ok := _usage[_key].(map[string]interface{})
		if !_ok {
			continue
		}
		if _value := firstPositiveInt(_details, "reasoning_tokens", "reasoning"); _value > _reasoningTokens {
			_reasoningTokens = _value
		}
	}
	return _reasoningTokens
}

// -------------------------------------------------------------------------------------
func responseFirstTokenMS(_payload map[string]interface{}) float64 {
	for _, _key := range []string{"time_to_first_token", "ttft", "first_token_ms"} {
		if _ms := durationValueMS(_key, _payload[_key]); _ms > 0 {
			return _ms
		}
	}

	if _usage, _ok := _payload["usage"].(map[string]interface{}); _ok {
		for _, _key := range []string{"time_to_first_token", "ttft", "first_token_ms"} {
			if _ms := durationValueMS(_key, _usage[_key]); _ms > 0 {
				return _ms
			}
		}
	}
	if _response, _ok := _payload["response"].(map[string]interface{}); _ok {
		return responseFirstTokenMS(_response)
	}

	return 0
}

// -------------------------------------------------------------------------------------
func responseGenerationDurationMS(_payload map[string]interface{}) float64 {
	for _, _key := range []string{"generation_time_ms", "completion_time_ms", "decode_time_ms", "output_time_ms", "generation_ms", "completion_ms", "decode_ms", "generation_duration", "generation_time", "completion_time", "decode_time", "output_time"} {
		if _ms := durationValueMS(_key, _payload[_key]); _ms > 0 {
			return _ms
		}
	}

	for _, _timings := range responseTimingMaps(_payload) {
		for _, _key := range []string{"predicted_ms", "generation_ms", "decode_ms", "completion_ms"} {
			if _ms := durationValueMS(_key, _timings[_key]); _ms > 0 {
				return _ms
			}
		}
	}

	if _usage, _ok := _payload["usage"].(map[string]interface{}); _ok {
		for _, _key := range []string{"generation_time_ms", "completion_time_ms", "decode_time_ms", "output_time_ms", "generation_ms", "completion_ms", "decode_ms", "generation_duration", "generation_time", "completion_time", "decode_time", "output_time"} {
			if _ms := durationValueMS(_key, _usage[_key]); _ms > 0 {
				return _ms
			}
		}
	}
	if _response, _ok := _payload["response"].(map[string]interface{}); _ok {
		return responseGenerationDurationMS(_response)
	}

	return 0
}

// -------------------------------------------------------------------------------------
func responseGenerationTPS(_payload map[string]interface{}) float64 {
	for _, _key := range []string{"generation_tokens_per_second", "tokens_per_second", "output_tokens_per_second", "completion_tokens_per_second"} {
		if _tps := numberAsFloat(_payload[_key]); _tps > 0 {
			return _tps
		}
	}

	for _, _timings := range responseTimingMaps(_payload) {
		for _, _key := range []string{"predicted_per_second", "generation_tokens_per_second", "tokens_per_second"} {
			if _tps := numberAsFloat(_timings[_key]); _tps > 0 {
				return _tps
			}
		}
	}

	if _usage, _ok := _payload["usage"].(map[string]interface{}); _ok {
		for _, _key := range []string{"generation_tokens_per_second", "tokens_per_second", "output_tokens_per_second", "completion_tokens_per_second"} {
			if _tps := numberAsFloat(_usage[_key]); _tps > 0 {
				return _tps
			}
		}
	}
	if _response, _ok := _payload["response"].(map[string]interface{}); _ok {
		return responseGenerationTPS(_response)
	}

	return 0
}

// -------------------------------------------------------------------------------------
func responseTimingMaps(_payload map[string]interface{}) []map[string]interface{} {
	_timings := []map[string]interface{}{}
	if _value, _ok := _payload["timings"].(map[string]interface{}); _ok {
		_timings = append(_timings, _value)
	}
	if _usage, _ok := _payload["usage"].(map[string]interface{}); _ok {
		if _value, _ok := _usage["timings"].(map[string]interface{}); _ok {
			_timings = append(_timings, _value)
		}
	}
	return _timings
}

// -------------------------------------------------------------------------------------
func durationValueMS(_key string, _value interface{}) float64 {
	_number := numberAsFloat(_value)
	if _number <= 0 {
		return 0
	}
	_key = strings.ToLower(_key)
	if strings.Contains(_key, "ms") {
		return _number
	}
	return _number * 1000
}

// -------------------------------------------------------------------------------------
func responseText(_payload map[string]interface{}) string {
	var _builder strings.Builder
	appendChoiceText(&_builder, _payload)
	appendTextFields(&_builder, _payload, "output_text", "text")
	appendTextValue(&_builder, _payload["output"])
	if _response, _ok := _payload["response"].(map[string]interface{}); _ok {
		_builder.WriteString(responseText(_response))
	}
	return _builder.String()
}

// -------------------------------------------------------------------------------------
func streamResponseText(_payload map[string]interface{}) string {
	_parts := streamResponseParts(_payload)
	return _parts.Prose + _parts.Reasoning + _parts.Tool
}

// -------------------------------------------------------------------------------------
// streamTextParts 是同一個事件裡依用途拆開的文字。三段相加等於先前
// streamResponseText 的結果，所以既有的字元／token 估算完全不受影響。
type streamTextParts struct {
	Prose     string
	Reasoning string
	Tool      string
	ToolCalls int
}

// -------------------------------------------------------------------------------------
func streamResponseParts(_payload map[string]interface{}) streamTextParts {
	var _prose, _reasoning, _tool strings.Builder
	_calls := appendChoiceParts(&_prose, &_reasoning, &_tool, _payload)

	_eventType := strings.ToLower(strings.TrimSpace(stringFromAny(_payload["type"])))
	switch _eventType {
	case "response.output_text.delta", "response.refusal.delta":
		appendTextValue(&_prose, _payload["delta"])
	case "response.reasoning_text.delta", "response.reasoning_summary_text.delta":
		appendTextValue(&_reasoning, _payload["delta"])
	case "response.function_call_arguments.delta", "response.mcp_call_arguments.delta", "response.custom_tool_call_input.delta":
		appendTextValue(&_tool, _payload["delta"])
	case "response.output_item.added":
		// 用字尾比對而不是列舉工具型別：function_call、local_shell_call、
		// custom_tool_call、mcp_call…… 上游新增型別時不必跟著改。
		if _item, _ok := _payload["item"].(map[string]interface{}); _ok {
			if strings.HasSuffix(strings.ToLower(stringFromAny(_item["type"])), "_call") {
				_calls++
			}
		}
	}

	return streamTextParts{
		Prose:     _prose.String(),
		Reasoning: _reasoning.String(),
		Tool:      _tool.String(),
		ToolCalls: _calls,
	}
}

// -------------------------------------------------------------------------------------
// appendChoiceParts 處理 chat completions 的 choices 形狀，回傳新起頭的工具呼叫數。
func appendChoiceParts(_prose *strings.Builder, _reasoning *strings.Builder, _tool *strings.Builder, _payload map[string]interface{}) int {
	_choices, _ok := _payload["choices"].([]interface{})
	if !_ok {
		return 0
	}

	_calls := 0
	for _, _item := range _choices {
		_choice, _ok := _item.(map[string]interface{})
		if !_ok {
			continue
		}
		for _, _key := range []string{"delta", "message"} {
			_section, _ok := _choice[_key].(map[string]interface{})
			if !_ok {
				continue
			}
			appendTextFields(_reasoning, _section, "reasoning_content", "reasoning", "thinking", "reasoning_text")
			appendTextFields(_prose, _section, "text", "output_text")
			appendMessageContent(_prose, _section["content"])
			_calls += appendToolCallArguments(_tool, _section["tool_calls"])
		}
		appendTextFields(_reasoning, _choice, "reasoning_content", "reasoning", "thinking", "reasoning_text")
		appendTextFields(_prose, _choice, "text", "output_text")
	}
	return _calls
}

// -------------------------------------------------------------------------------------
func appendMessageContent(_builder *strings.Builder, _value interface{}) {
	switch _content := _value.(type) {
	case string:
		_builder.WriteString(_content)
	case []interface{}:
		for _, _part := range _content {
			if _partMap, _ok := _part.(map[string]interface{}); _ok {
				if _text, _ok := _partMap["text"].(string); _ok {
					_builder.WriteString(_text)
				}
			}
		}
	}
}

// -------------------------------------------------------------------------------------
// appendToolCallArguments 收集 chat completions 的 tool_calls 參數。
// 只有每個工具呼叫的第一個分塊帶 id，用它來計數才不會把同一個呼叫算很多次。
func appendToolCallArguments(_builder *strings.Builder, _value interface{}) int {
	_items, _ok := _value.([]interface{})
	if !_ok {
		return 0
	}

	_calls := 0
	for _, _item := range _items {
		_call, _ok := _item.(map[string]interface{})
		if !_ok {
			continue
		}
		if strings.TrimSpace(stringFromAny(_call["id"])) != "" {
			_calls++
		}
		if _function, _ok := _call["function"].(map[string]interface{}); _ok {
			appendTextFields(_builder, _function, "arguments")
		}
	}
	return _calls
}

// -------------------------------------------------------------------------------------
func appendChoiceText(_builder *strings.Builder, _payload map[string]interface{}) {
	_choices, _ok := _payload["choices"].([]interface{})
	if !_ok {
		return
	}
	for _, _item := range _choices {
		_choice, _ok := _item.(map[string]interface{})
		if !_ok {
			continue
		}
		appendContent(_builder, _choice["delta"])
		appendContent(_builder, _choice["message"])
		appendTextFields(_builder, _choice, "reasoning_content", "reasoning", "thinking", "reasoning_text", "text", "output_text")
	}
}

// -------------------------------------------------------------------------------------
func appendContent(_builder *strings.Builder, _value interface{}) {
	_map, _ok := _value.(map[string]interface{})
	if !_ok {
		return
	}

	appendTextFields(_builder, _map, "reasoning_content", "reasoning", "thinking", "reasoning_text", "text", "output_text")
	switch _content := _map["content"].(type) {
	case string:
		_builder.WriteString(_content)
	case []interface{}:
		for _, _part := range _content {
			if _partMap, _ok := _part.(map[string]interface{}); _ok {
				if _text, _ok := _partMap["text"].(string); _ok {
					_builder.WriteString(_text)
				}
			}
		}
	}
}

// -------------------------------------------------------------------------------------
func appendTextFields(_builder *strings.Builder, _source map[string]interface{}, _keys ...string) {
	for _, _key := range _keys {
		appendTextValue(_builder, _source[_key])
	}
}

// -------------------------------------------------------------------------------------
func appendTextValue(_builder *strings.Builder, _value interface{}) {
	switch _typed := _value.(type) {
	case string:
		_builder.WriteString(_typed)
	case []interface{}:
		for _, _item := range _typed {
			appendTextValue(_builder, _item)
		}
	case map[string]interface{}:
		appendTextFields(_builder, _typed, "content", "text", "output_text", "reasoning_content", "reasoning", "thinking", "reasoning_text")
	}
}

// -------------------------------------------------------------------------------------
func firstPositiveInt(_source map[string]interface{}, _keys ...string) int {
	for _, _key := range _keys {
		if _value := numberAsInt(_source[_key]); _value > 0 {
			return _value
		}
	}
	return 0
}

// -------------------------------------------------------------------------------------
func numberAsInt(_value interface{}) int {
	switch _typed := _value.(type) {
	case float64:
		return int(_typed)
	case float32:
		return int(_typed)
	case int:
		return _typed
	case int64:
		return int(_typed)
	case json.Number:
		_intValue, _ := _typed.Int64()
		return int(_intValue)
	default:
		return 0
	}
}

// -------------------------------------------------------------------------------------
func numberAsFloat(_value interface{}) float64 {
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
		_floatValue, _ := _typed.Float64()
		return _floatValue
	default:
		return 0
	}
}

// -------------------------------------------------------------------------------------
func estimateTokens(_text string) int {
	_chineseChars, _otherChars := tokenCharCounts(_text)
	return estimateTokensFromCounts(_chineseChars, _otherChars)
}

// -------------------------------------------------------------------------------------
func tokenCharCounts(_text string) (int, int) {
	_chineseChars := 0
	_otherChars := 0
	for _, _rune := range _text {
		if unicode.In(_rune, unicode.Han) {
			_chineseChars++
			continue
		}
		if !unicode.IsSpace(_rune) {
			_otherChars++
		}
	}
	return _chineseChars, _otherChars
}

// -------------------------------------------------------------------------------------
func estimateTokensFromCounts(_chineseChars int, _otherChars int) int {
	_tokens := int(math.Ceil(float64(_chineseChars)/1.5 + float64(_otherChars)/4.0))
	if _tokens < 1 && (_chineseChars > 0 || _otherChars > 0) {
		return 1
	}
	return _tokens
}

// -------------------------------------------------------------------------------------
func flushResponse(_w http.ResponseWriter) {
	_ = FlushResponseWriter(_w)
}

// -------------------------------------------------------------------------------------
func embeddedResponseWriter(_w http.ResponseWriter) http.ResponseWriter {
	_value := reflect.ValueOf(_w)
	if !_value.IsValid() {
		return nil
	}
	if _value.Kind() == reflect.Pointer {
		if _value.IsNil() {
			return nil
		}
		_value = _value.Elem()
	}
	if _value.Kind() != reflect.Struct {
		return nil
	}

	_field := _value.FieldByName("ResponseWriter")
	if !_field.IsValid() || !_field.CanInterface() {
		return nil
	}

	_writer, _ok := _field.Interface().(http.ResponseWriter)
	if !_ok || _writer == _w {
		return nil
	}
	return _writer
}

// -------------------------------------------------------------------------------------
