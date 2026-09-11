package proxy

import (
	"encoding/json"
	"strings"
	"testing"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/domain"
)

// -------------------------------------------------------------------------------------
func TestBuildCodexImageResponsesRequestSeparatesMainAndImageModels(t *testing.T) {
	_compression := 80
	_body, _err := buildCodexImageResponsesRequest("gpt-5.6-sol", openAIImageGenerationRequest{
		Prompt:            "Draw a factory dashboard",
		Model:             "gpt-image-2",
		Size:              "1024x1024",
		Quality:           "high",
		OutputCompression: &_compression,
	})
	if _err != nil {
		t.Fatalf("build codex image request failed: %v", _err)
	}

	_payload := map[string]interface{}{}
	if _err := json.Unmarshal(_body, &_payload); _err != nil {
		t.Fatalf("decode codex image request failed: %v", _err)
	}
	if _payload["model"] != "gpt-5.6-sol" {
		t.Fatalf("unexpected responses model: %#v", _payload["model"])
	}
	_tools := _payload["tools"].([]interface{})
	_tool := _tools[0].(map[string]interface{})
	if _tool["type"] != "image_generation" || _tool["model"] != "gpt-image-2" {
		t.Fatalf("unexpected image tool: %#v", _tool)
	}
	if _payload["stream"] != true || _payload["store"] != false {
		t.Fatalf("unexpected responses flags: %#v", _payload)
	}
}

// -------------------------------------------------------------------------------------
func TestCodexImageUsesConfiguredMainModel(t *testing.T) {
	_provider := &balancer.ProviderRuntime{Config: &domain.LLMProviderConfig{Models: []domain.LLMModelConfig{{Name: "gpt-5.6-sol"}}}}
	if _model := codexImageMainModel(_provider); _model != "gpt-5.6-sol" {
		t.Fatalf("image main model should respect provider default: %s", _model)
	}
}

// -------------------------------------------------------------------------------------
func TestReadCodexGeneratedImageFromSSE(t *testing.T) {
	_stream := strings.Join([]string{
		`event: response.output_item.done`,
		`data: {"type":"response.output_item.done","item":{"type":"image_generation_call","result":"aGVsbG8=","revised_prompt":"draw a dashboard","output_format":"png"}}`,
		``,
		`event: response.completed`,
		`data: {"type":"response.completed","response":{"created_at":1710000000,"output":[]}}`,
		``,
	}, "\n")

	_result, _createdAt, _err := readCodexGeneratedImage(strings.NewReader(_stream))
	if _err != nil {
		t.Fatalf("read codex image stream failed: %v", _err)
	}
	if _result.Base64 != "aGVsbG8=" || _result.RevisedPrompt != "draw a dashboard" {
		t.Fatalf("unexpected image result: %#v", _result)
	}
	if _createdAt != 1710000000 {
		t.Fatalf("unexpected created_at: %d", _createdAt)
	}

	_response, _err := buildOpenAIImageGenerationResponse([]codexImageResult{_result}, _createdAt, "b64_json")
	if _err != nil {
		t.Fatalf("build OpenAI image response failed: %v", _err)
	}
	if !strings.Contains(string(_response), `"b64_json":"aGVsbG8="`) {
		t.Fatalf("unexpected OpenAI image response: %s", string(_response))
	}
}
