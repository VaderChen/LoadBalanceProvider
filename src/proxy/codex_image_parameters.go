package proxy

import (
	"fmt"
	"strconv"
	"strings"
)

func normalizeCodexImageParameters(req *openAIImageGenerationRequest) error {
	model := strings.ToLower(strings.TrimSpace(req.Model))
	req.Size = strings.ToLower(strings.TrimSpace(req.Size))
	if req.Size == "" || req.Size == "auto" {
		switch model {
		case "gpt-image-2-2k":
			req.Size = "2048x2048"
		case "gpt-image-2-4k":
			req.Size = "3840x2160"
		}
	}
	req.Model = normalizeCodexImageToolModel(model)
	if req.Model != "gpt-image-2" && !strings.HasPrefix(req.Model, "gpt-image-2-20") {
		return nil
	}
	req.Quality = strings.ToLower(strings.TrimSpace(req.Quality))
	switch req.Quality {
	case "", "auto", "low", "medium", "high":
	default:
		return fmt.Errorf("gpt-image-2 quality must be auto, low, medium or high")
	}
	if req.Size == "" || req.Size == "auto" {
		return nil
	}
	parts := strings.Split(req.Size, "x")
	if len(parts) != 2 {
		return fmt.Errorf("gpt-image-2 size must be WIDTHxHEIGHT or auto")
	}
	w, ew := strconv.Atoi(parts[0])
	h, eh := strconv.Atoi(parts[1])
	if ew != nil || eh != nil || w <= 0 || h <= 0 || w > 3840 || h > 3840 || w%16 != 0 || h%16 != 0 {
		return fmt.Errorf("gpt-image-2 size edges must be positive multiples of 16 and at most 3840")
	}
	if max(w, h) > 3*min(w, h) || w*h < 655360 || w*h > 8294400 {
		return fmt.Errorf("gpt-image-2 size must have aspect ratio at most 3:1 and 655360 to 8294400 pixels")
	}
	return nil
}
