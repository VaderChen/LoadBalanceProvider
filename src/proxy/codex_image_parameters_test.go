package proxy

import (
	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/domain"
	"strings"
	"testing"
	"time"
)

func TestImageTwoAliasParameters(t *testing.T) {
	for _, tc := range []struct{ model, size, want string }{
		{"gpt-image-2-2k", "", "2048x2048"},
		{"gpt-image-2-4k", "auto", "3840x2160"},
		{"gpt-image-2-4k", "2160x3840", "2160x3840"},
	} {
		r := openAIImageGenerationRequest{Model: tc.model, Size: tc.size}
		if err := normalizeCodexImageParameters(&r); err != nil || r.Model != "gpt-image-2" || r.Size != tc.want {
			t.Fatalf("parameters = %+v, error = %v", r, err)
		}
	}
	for _, size := range []string{"4096x4096", "1023x1024", "256x256", "3840x640", "abc", "0x0"} {
		r := openAIImageGenerationRequest{Model: "gpt-image-2", Size: size}
		if normalizeCodexImageParameters(&r) == nil {
			t.Fatalf("accepted invalid size %q", size)
		}
	}
}

func TestImageStreamKeepsLimitDetails(t *testing.T) {
	_, _, err := readCodexGeneratedImage(strings.NewReader("event: error\ndata: {\"type\":\"error\",\"error\":{\"code\":\"rate_limit_exceeded\",\"message\":\"limited\",\"retry_after\":60}}\n\n"))
	policy := ClassifyFailure(err)
	if !policy.TransientCapacity || policy.RetryAfter != time.Minute {
		t.Fatalf("lost limit details: %+v, %v", policy, err)
	}
}

func TestImageStreamRequiresCompletion(t *testing.T) {
	_, _, err := readCodexGeneratedImage(strings.NewReader("event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"image_generation_call\",\"result\":\"aGVsbG8=\"}}\n\n"))
	if err == nil {
		t.Fatal("accepted a stream without response.completed")
	}
}

func TestImageMainModelUsesProviderDefault(t *testing.T) {
	t.Setenv("MARS_CODEX_IMAGE_MAIN_MODEL", "ignored-override")
	for _, name := range []string{"gpt-5.5", "gpt-6-astra", "AUTO", "gpt-image-2", ""} {
		provider := &balancer.ProviderRuntime{Config: &domain.LLMProviderConfig{Models: []domain.LLMModelConfig{{Name: name}, {Name: "other-model"}}}}
		want := name
		if name == "AUTO" || name == "gpt-image-2" {
			want = ""
		}
		if got := codexImageMainModel(provider); got != want {
			t.Fatalf("default %q: got %q, want %q", name, got, want)
		}
	}
	if got := codexImageMainModel(nil); got != "" {
		t.Fatalf("missing provider = %q", got)
	}
}
