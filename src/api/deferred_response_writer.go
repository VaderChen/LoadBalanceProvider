package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"LoadBalanceProvider/src/proxy"
)

// -------------------------------------------------------------------------------------
const deferredResponseBufferLimit = 2 * 1024 * 1024

var errResponseBufferLimit = errors.New("回應暫存超過 2 MiB 安全上限，未送出部分內容；請縮小單次輸出")

// -------------------------------------------------------------------------------------
type deferredResponseWriter struct {
	// lock 保護所有狀態：保活心跳由另一個 goroutine 送出，與轉發同時進行。
	lock              sync.Mutex
	target            http.ResponseWriter
	header            http.Header
	statusCode        int
	buffer            bytes.Buffer
	stream            bool
	deferUntilSuccess bool
	committed         bool
	// contentWritten 表示「真正的回應內容」已經**送達客戶端**（不是只寫進緩衝）。
	// 它和 committed 是兩回事：保活心跳會 commit（header 必須先送出去），
	// 但心跳不帶任何回應內容，所以送過心跳之後仍然可以換帳號重試 ——
	// 客戶端只是多收到幾個會被忽略的 ping。
	contentWritten   bool
	firstCommitEvent string
	// pendingContent 表示緩衝裡有尚未送出的回應內容。只有在 commit 時才會
	// 升級成 contentWritten —— 沒送出去的內容不該剝奪重試能力。
	pendingContent bool
	writeErr       error
}

// -------------------------------------------------------------------------------------
func newDeferredResponseWriter(_target http.ResponseWriter, _stream bool) *deferredResponseWriter {
	return &deferredResponseWriter{target: _target, header: make(http.Header), stream: _stream}
}

// 由支援的來源啟用，所有 SSE 內容待轉送流程確認成功後才由 Commit 送出。
func (_w *deferredResponseWriter) DeferStreamUntilSuccess() {
	_w.lock.Lock()
	defer _w.lock.Unlock()
	_w.deferUntilSuccess = _w.stream
}

// -------------------------------------------------------------------------------------
// AdoptCommitted 承接前一次嘗試的送出狀態。每次重試都會建立新的 deferred writer，
// 但它們共用同一個底層 ResponseWriter —— 保活心跳一旦把 header 送出去，
// 後續的 writer 必須知道，否則會重複送 header。
func (_w *deferredResponseWriter) AdoptCommitted() {
	if _w == nil {
		return
	}
	_w.lock.Lock()
	defer _w.lock.Unlock()
	_w.committed = true
}

// -------------------------------------------------------------------------------------
func (_w *deferredResponseWriter) Header() http.Header {
	_w.lock.Lock()
	defer _w.lock.Unlock()
	return _w.header
}

// -------------------------------------------------------------------------------------
func (_w *deferredResponseWriter) WriteHeader(_statusCode int) {
	_w.lock.Lock()
	defer _w.lock.Unlock()
	if _w.statusCode == 0 {
		_w.statusCode = _statusCode
	}
}

// -------------------------------------------------------------------------------------
func (_w *deferredResponseWriter) Write(_data []byte) (int, error) {
	_w.lock.Lock()
	defer _w.lock.Unlock()
	// 這裡寫的一律是真正的回應內容（心跳走 WriteStreamHeartbeat）。
	if _w.contentWritten && _w.statusCode < http.StatusBadRequest {
		defer _w.boundWriteLocked()()
		if _w.buffer.Len() > 0 {
			if _, _err := io.Copy(_w.target, &_w.buffer); _err != nil {
				return 0, _err
			}
		}
		_w.contentWritten = true
		return _w.target.Write(_data)
	}
	_w.pendingContent = true
	if _w.deferUntilSuccess && len(_data) > deferredResponseBufferLimit-_w.buffer.Len() {
		_w.writeErr = errResponseBufferLimit
		return 0, _w.writeErr
	}
	if _w.statusCode == 0 {
		_w.statusCode = http.StatusOK
	}
	_count, _err := _w.buffer.Write(_data)
	if _err != nil {
		return _count, _err
	}
	if _w.deferUntilSuccess {
		return _count, nil
	}
	_reason := ""
	if _w.stream && _w.statusCode < http.StatusBadRequest {
		_reason = streamBufferForwardableEvent(_w.buffer.Bytes())
	}
	if _w.stream && _w.statusCode < http.StatusBadRequest && (_reason != "" || _w.buffer.Len() >= deferredResponseBufferLimit) {
		if _w.firstCommitEvent == "" {
			_w.firstCommitEvent = _reason
			if _reason == "" {
				_w.firstCommitEvent = "buffer-limit"
			}
		}
		_w.writeErr = _w.commitLocked()
		if _w.writeErr != nil {
			return _count, _w.writeErr
		}
	}
	return _count, nil
}

// -------------------------------------------------------------------------------------
func (_w *deferredResponseWriter) Flush() {
	_w.lock.Lock()
	defer _w.lock.Unlock()
	if !_w.committed {
		return
	}
	defer _w.boundWriteLocked()()
	flushHTTPResponseWriter(_w.target)
}

// WriteStreamHeartbeat 送出保活心跳。它必須 commit（header 得先送出去客戶端才會
// 開始讀串流），但心跳不帶任何回應內容，所以 contentWritten 維持 false ——
// 送過心跳之後仍然可以換帳號重試，客戶端只會多收到幾個會被忽略的 ping。
// -------------------------------------------------------------------------------------
func (_w *deferredResponseWriter) WriteStreamHeartbeat(_data []byte) error {
	if _w == nil || len(_data) == 0 {
		return nil
	}
	_w.lock.Lock()
	defer _w.lock.Unlock()
	if _w.statusCode >= http.StatusBadRequest {
		return nil
	}
	defer _w.boundWriteLocked()()
	if !_w.committed {
		// 不讀取轉發 goroutine 正在填寫的 header，也不送出待判斷的失敗內容。
		_header := _w.target.Header()
		_header.Set("Content-Type", "text/event-stream")
		_header.Set("Cache-Control", "no-cache")
		_header.Set("X-Accel-Buffering", "no")
		_w.target.WriteHeader(http.StatusOK)
		_w.committed = true
	}
	_, _w.writeErr = _w.target.Write(_data)
	if _w.writeErr == nil {
		_w.writeErr = proxy.FlushResponseWriter(_w.target)
		if errors.Is(_w.writeErr, http.ErrNotSupported) {
			_w.writeErr = nil
		}
	}
	return _w.writeErr
}

// 寫入期限只涵蓋下游 I/O，不限制工具執行或正常串流的總時間。
func (_w *deferredResponseWriter) boundWriteLocked() func() {
	_ = proxy.SetResponseWriteDeadline(_w.target, time.Now().Add(30*time.Second))
	return func() { _ = proxy.SetResponseWriteDeadline(_w.target, time.Time{}) }
}

// -------------------------------------------------------------------------------------
func (_w *deferredResponseWriter) Commit() error {
	if _w == nil {
		return nil
	}
	_w.lock.Lock()
	defer _w.lock.Unlock()
	return _w.commitLocked()
}

// -------------------------------------------------------------------------------------
func (_w *deferredResponseWriter) commitLocked() error {
	if _w == nil {
		return nil
	}
	if _w.deferUntilSuccess && _w.pendingContent {
		if err := validateBufferedResponse(_w.buffer.String()); err != nil {
			return err
		}
	}
	defer _w.boundWriteLocked()()
	if !_w.committed {
		for _name, _values := range _w.header {
			_w.target.Header().Del(_name)
			for _, _value := range _values {
				_w.target.Header().Add(_name, _value)
			}
		}
		_statusCode := _w.statusCode
		if _statusCode == 0 {
			_statusCode = http.StatusOK
		}
		_w.target.WriteHeader(_statusCode)
		_w.committed = true
	}
	if _w.pendingContent {
		if _w.deferUntilSuccess && _w.firstCommitEvent == "" {
			_w.firstCommitEvent = "validated-response"
		}
		// 內容真的離開緩衝送給客戶端了，這一刻起才沒有退路。
		_w.contentWritten = true
	}
	if _w.buffer.Len() > 0 {
		_, _w.writeErr = io.Copy(_w.target, &_w.buffer)
	}
	flushHTTPResponseWriter(_w.target)
	return _w.writeErr
}

func validateBufferedResponse(body string) error {
	completed := false
	for _, frame := range proxy.ParseSSEDataFrames(body) {
		if strings.TrimSpace(frame.Data) == "[DONE]" {
			continue
		}
		var payload map[string]interface{}
		if json.Unmarshal([]byte(frame.Data), &payload) != nil || payload == nil {
			return &proxy.ProviderStreamError{Message: "provider returned invalid buffered SSE", TruncatedStream: true}
		}
		kind := stringValue(payload["type"])
		if kind == "" {
			kind = frame.Event
			payload["type"] = kind
		}
		if streamPayloadIsFailure(payload) || kind == "response.incomplete" || kind == "response.cancelled" {
			return &proxy.ProviderStreamError{Message: "provider buffered response did not complete successfully", TruncatedStream: true}
		}
		if kind == "response.completed" {
			response, ok := payload["response"].(map[string]interface{})
			if !ok || (stringValue(response["status"]) != "" && stringValue(response["status"]) != "completed") || response["error"] != nil {
				return &proxy.ProviderStreamError{Message: "provider returned invalid response completion", TruncatedStream: true}
			}
			completed = true
		}
	}
	if !completed {
		return &proxy.ProviderStreamError{Message: "provider stream ended without response.completed", TruncatedStream: true}
	}
	return nil
}

// -------------------------------------------------------------------------------------
// ResetForGracefulTerminal 丟棄尚未送出的緩衝內容，讓呼叫端改用一則正常完成的訊息收尾。
// 已經送出真正的回應內容之後不可使用（回傳 false）。
// 只送過心跳的串流仍然可以收尾：header 雖然出去了，但客戶端還沒收到任何內容，
// 直接把結尾事件接在心跳後面即可。
func (_w *deferredResponseWriter) ResetForGracefulTerminal() bool {
	_w.lock.Lock()
	defer _w.lock.Unlock()
	if _w.contentWritten {
		return false
	}
	_w.buffer.Reset()
	_w.deferUntilSuccess = false
	_w.pendingContent = false
	_w.statusCode = 0
	_w.header = make(http.Header)
	_w.writeErr = nil
	return true
}

// -------------------------------------------------------------------------------------
func (_w *deferredResponseWriter) Committed() bool {
	if _w == nil {
		return false
	}
	_w.lock.Lock()
	defer _w.lock.Unlock()
	return _w.committed
}

// -------------------------------------------------------------------------------------
// ContentWritten 表示真正的回應內容已經送給客戶端 —— 這才是「不能再重試」的判準。
// 只送過保活心跳的串流仍可換帳號重試。
func (_w *deferredResponseWriter) ContentWritten() bool {
	if _w == nil {
		return false
	}
	_w.lock.Lock()
	defer _w.lock.Unlock()
	return _w.contentWritten
}

// -------------------------------------------------------------------------------------
func (_w *deferredResponseWriter) StatusCode() int {
	if _w != nil {
		_w.lock.Lock()
		defer _w.lock.Unlock()
	}
	if _w == nil || _w.statusCode == 0 {
		return http.StatusOK
	}
	return _w.statusCode
}

// -------------------------------------------------------------------------------------
func (_w *deferredResponseWriter) BufferedBody() string {
	if _w == nil {
		return ""
	}
	_w.lock.Lock()
	defer _w.lock.Unlock()
	return _w.buffer.String()
}

// -------------------------------------------------------------------------------------
func (_w *deferredResponseWriter) HasBufferedResponse() bool {
	if _w == nil {
		return false
	}
	_w.lock.Lock()
	defer _w.lock.Unlock()
	return _w.buffer.Len() > 0 || _w.statusCode >= http.StatusBadRequest
}

// -------------------------------------------------------------------------------------
func flushHTTPResponseWriter(_writer http.ResponseWriter) {
	_ = proxy.FlushResponseWriter(_writer)
}

// -------------------------------------------------------------------------------------
func streamBufferHasForwardableEvent(_data []byte) bool {
	return streamBufferForwardableEvent(_data) != ""
}

func streamBufferForwardableEvent(_data []byte) string {
	// 初始化事件及分段 JSON 留在緩衝，直到完整的有效事件或成功終止事件到達。
	_text := strings.ReplaceAll(string(_data), "\r\n", "\n")
	_end := strings.LastIndex(_text, "\n\n")
	if _end < 0 {
		return ""
	}
	for _, _frame := range proxy.ParseSSEDataFrames(_text[:_end+2]) {
		_payloadText := strings.TrimSpace(_frame.Data)
		if _payloadText == "" {
			continue
		}
		if _payloadText == "[DONE]" {
			return "done-marker"
		}
		var _payload map[string]interface{}
		if _err := json.Unmarshal([]byte(_payloadText), &_payload); _err != nil {
			return "invalid-json"
		}
		if _payload == nil {
			return "non-object-json"
		}
		if stringValue(_payload["type"]) == "" && _frame.Event != "" && _frame.Event != "message" {
			_payload["type"] = _frame.Event
		}
		if proxy.IsSSEHeartbeatEvent(stringValue(_payload["type"])) {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(stringValue(_payload["type"]))) {
		case "response.created", "response.queued", "response.in_progress", "codex.rate_limits", "codex.response.metadata":
			continue
		case "response.output_item.added":
			if streamItemIsEmptyPlaceholder(_payload["item"]) {
				continue
			}
		case "response.content_part.added", "response.reasoning_summary_part.added":
			if streamPartIsEmptyPlaceholder(_payload["part"]) {
				continue
			}
		}
		if !streamPayloadIsFailure(_payload) {
			return streamDiagnosticEventType(stringValue(_payload["type"]))
		}
	}
	return ""
}

func (_w *deferredResponseWriter) FirstCommitEvent() string {
	_w.lock.Lock()
	defer _w.lock.Unlock()
	return _w.firstCommitEvent
}

func streamPartIsEmptyPlaceholder(value interface{}) bool {
	part, ok := value.(map[string]interface{})
	if !ok {
		return false
	}
	switch stringValue(part["type"]) {
	case "output_text", "reasoning_text", "summary_text":
	default:
		return false
	}
	for key, value := range part {
		if key == "type" {
			continue
		}
		switch value := value.(type) {
		case nil:
		case string:
			if value != "" {
				return false
			}
		case []interface{}:
			if len(value) != 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// 保留未知協定事件的識別名稱；限制命名空間、長度與字元，不記錄事件內容。
func streamDiagnosticEventType(kind string) string {
	if kind == "" {
		return "untyped-json"
	}
	switch kind {
	case "response.output_item.added", "response.output_item.done", "response.content_part.added", "response.content_part.done",
		"response.reasoning_summary_part.added", "response.reasoning_summary_part.done",
		"response.output_text.delta", "response.output_text.done", "response.reasoning_text.delta", "response.reasoning_text.done",
		"response.reasoning_summary_text.delta", "response.reasoning_summary_text.done",
		"response.function_call_arguments.delta", "response.function_call_arguments.done",
		"response.custom_tool_call_input.delta", "response.custom_tool_call_input.done",
		"response.completed", "response.incomplete", "response.refusal.delta":
		return kind
	default:
		if len(kind) <= 96 {
			valid := true
			for _, ch := range kind {
				if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '.' && ch != '_' && ch != '-' {
					valid = false
					break
				}
			}
			if valid {
				return kind
			}
		}
		return "other-event"
	}
}

// -------------------------------------------------------------------------------------
// 只延後已知的空殼事件；參數、內容、完成事件及未知工具一律維持原轉送邊界。
func streamItemIsEmptyPlaceholder(value interface{}) bool {
	item, ok := value.(map[string]interface{})
	if !ok {
		return false
	}
	switch stringValue(item["type"]) {
	case "message", "reasoning", "function_call", "custom_tool_call":
	default:
		return false
	}
	if status := stringValue(item["status"]); status != "" && status != "in_progress" {
		return false
	}
	for key, value := range item {
		switch key {
		case "id", "type", "status", "role", "name", "call_id":
			continue
		}
		switch value := value.(type) {
		case nil:
		case string:
			if value != "" {
				return false
			}
		case []interface{}:
			if len(value) != 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func streamPayloadIsFailure(_payload map[string]interface{}) bool {
	_type := strings.ToLower(strings.TrimSpace(stringValue(_payload["type"])))
	if _type == "error" || strings.HasSuffix(_type, ".failed") {
		return true
	}
	_, _hasError := _payload["error"]
	return _type == "" && _hasError
}
