package proxy

import "strings"

// IsSSEHeartbeatEvent 統一保活分類，避免心跳被視為回應內容或重設有效進度計時。
func IsSSEHeartbeatEvent(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "ping", "response.ping", "heartbeat", "keepalive", "keep-alive":
		return true
	default:
		return false
	}
}

type SSEDataFrame struct {
	Event string
	Data  string
}

// ParseSSEDataFrames 供已組成完整事件的呼叫端使用；多個 data 欄位依 SSE 規則以換行串接。
func ParseSSEDataFrames(input string) []SSEDataFrame {
	input = strings.ReplaceAll(input, "\r\n", "\n")
	var frames []SSEDataFrame
	var event string
	var data []string
	flush := func() {
		if len(data) > 0 {
			frames = append(frames, SSEDataFrame{Event: event, Data: strings.Join(data, "\n")})
		}
		event, data = "", nil
	}
	for _, line := range strings.Split(input, "\n") {
		if line == "" {
			flush()
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			event = value
		case "data":
			data = append(data, value)
		}
	}
	flush()
	return frames
}
