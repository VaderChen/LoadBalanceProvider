package api

import (
	"encoding/json"
	"fmt"
	"strings"
)

type turnRecoveryContextKey struct{}

// 僅恢復自包含歷史，不補造工具結果，也不重新執行已完成的工具。
func recoverFullHistoryBody(body []byte) ([]byte, error) {
	return prepareRecoveryBody(body, true)
}

func prepareRecoveryBody(body []byte, requireHistory bool) ([]byte, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	var previous string
	_ = json.Unmarshal(payload["previous_response_id"], &previous)
	if requireHistory && strings.TrimSpace(previous) != "" {
		return nil, fmt.Errorf("這個請求只有增量前文，請用完整歷史重送或開啟新對話")
	}
	var text string
	if json.Unmarshal(payload["input"], &text) == nil && strings.TrimSpace(text) != "" {
		return body, nil
	}
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(payload["input"], &items); err != nil {
		return nil, fmt.Errorf("恢復問答需要完整訊息歷史")
	}
	read := func(raw json.RawMessage) string { var value string; _ = json.Unmarshal(raw, &value); return value }
	calls := make(map[string]bool)
	kept := make([]map[string]json.RawMessage, 0, len(items))
	hasUser := false
	for _, item := range items {
		kind := read(item["type"])
		if kind == "reasoning" {
			continue
		}
		if requireHistory && kind == "item_reference" {
			return nil, fmt.Errorf("前文包含未展開的項目參照，請提供完整歷史")
		}
		if read(item["role"]) == "user" {
			hasUser = true
		}
		if strings.HasSuffix(kind, "_call") {
			if id := read(item["call_id"]); id != "" {
				calls[id] = true
			}
		}
		if requireHistory && strings.HasSuffix(kind, "_call_output") {
			if !calls[read(item["call_id"])] {
				return nil, fmt.Errorf("工具結果缺少原始呼叫，無法安全恢復；請開啟新對話")
			}
		}
		kept = append(kept, item)
	}
	if requireHistory && !hasUser {
		return nil, fmt.Errorf("請求缺少使用者前文，請提供完整歷史")
	}
	encoded, err := json.Marshal(kept)
	if err != nil {
		return nil, err
	}
	payload["input"] = encoded
	return json.Marshal(payload)
}
