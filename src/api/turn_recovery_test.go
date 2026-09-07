package api

import (
	"strings"
	"testing"
)

func TestRecoveryPreservesToolHistory(t *testing.T) {
	body := []byte(`{"input":[{"role":"user","content":"continue"},{"type":"reasoning","encrypted_content":"old"},{"type":"custom_tool_call","call_id":"c1","input":"command"},{"type":"custom_tool_call_output","call_id":"c1","output":""}],"seed":9007199254740993}`)
	got, err := recoverFullHistoryBody(body)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{`"output":""`, `"input":"command"`, `9007199254740993`} {
		if !strings.Contains(string(got), value) {
			t.Fatalf("lost history: %s", got)
		}
	}
	if strings.Contains(string(got), "encrypted_content") {
		t.Fatal("old reasoning retained")
	}
}

func TestRecoveryRejectsIncompleteHistory(t *testing.T) {
	for _, body := range []string{
		`{"previous_response_id":"old","input":[{"role":"user","content":"continue"}]}`,
		`{"input":[{"role":"user","content":"continue"},{"type":"function_call_output","call_id":"missing","output":"done"}]}`,
	} {
		if _, err := recoverFullHistoryBody([]byte(body)); err == nil {
			t.Fatal("incomplete history accepted")
		}
	}
}
