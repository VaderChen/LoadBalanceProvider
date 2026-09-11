package proxy

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"mime/multipart"
	"net/http/httptest"
	"testing"
)

func TestCodexTurnStateRequiresMatchingVerifiedProvider(t *testing.T) {
	for _, tc := range []struct {
		verified, target string
		want             bool
	}{{"", "a", false}, {"a", "b", false}, {"a", "a", true}} {
		source := httptest.NewRequest("POST", "/v1/responses", nil)
		source.Header.Set("X-Codex-Turn-State", "opaque-state")
		source = source.WithContext(WithVerifiedTurnStateProvider(source.Context(), tc.verified))
		target := httptest.NewRequest("POST", "/v1/responses", nil)
		copyProviderPassthroughHeaders(source, target, "")
		copyVerifiedCodexTurnState(source, target, tc.target)
		if got := target.Header.Get("X-Codex-Turn-State") != ""; got != tc.want {
			t.Fatalf("case %+v: forwarded=%v", tc, got)
		}
	}
}

func TestCodexManifestDoesNotAdvertiseWebSockets(t *testing.T) {
	body, err := AddAutoModelToCodexManifest([]byte(`{"models":[{"slug":"model-a","prefer_websockets":true}]}`))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Models []struct {
			Prefer bool `json:"prefer_websockets"`
		} `json:"models"`
	}
	if err := json.Unmarshal(body, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Models) != 2 {
		t.Fatal("missing AUTO model")
	}
	for _, model := range manifest.Models {
		if model.Prefer {
			t.Fatal("advertises unsupported WebSocket transport")
		}
	}
}

func TestCodexMultipartImageEditPreservesImagesAndMask(t *testing.T) {
	png, err := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aK1sAAAAASUVORK5CYII=")
	if err != nil {
		t.Fatal(err)
	}
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	if err := w.WriteField("prompt", "change the background"); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteField("model", "gpt-image-2"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"image[]", "image[]", "mask"} {
		part, err := w.CreateFormFile(name, "input.png")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := part.Write(png); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	req, err := decodeCodexImageEdit(body.Bytes(), w.FormDataContentType())
	if err != nil {
		t.Fatal(err)
	}
	if len(req.InputImages) != 2 || req.Mask == nil {
		t.Fatal("lost image or mask")
	}
	encoded, err := buildCodexImageResponsesRequest("gpt-5.5", req)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(encoded, &payload); err != nil {
		t.Fatal(err)
	}
	tool := payload["tools"].([]interface{})[0].(map[string]interface{})
	if tool["action"] != "edit" || tool["input_image_mask"] == nil {
		t.Fatal("edit tool not preserved")
	}
}
