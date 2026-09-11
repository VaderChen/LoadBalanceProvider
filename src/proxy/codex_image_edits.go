package proxy

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"

	"LoadBalanceProvider/src/security"
)

func isOpenAIImageEditRoute(r *http.Request) bool {
	return r != nil && r.URL != nil && normalizeResponsesRoutePath(r.URL.Path) == "/v1/images/edits"
}

func decodeCodexImageEdit(raw []byte, contentType string) (openAIImageGenerationRequest, error) {
	var result openAIImageGenerationRequest
	media, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return result, fmt.Errorf("invalid image edit content type: %w", err)
	}
	fields := make(map[string]interface{})
	var images []map[string]interface{}
	var mask map[string]interface{}
	switch media {
	case "multipart/form-data":
		if params["boundary"] == "" {
			return result, fmt.Errorf("image edit multipart boundary is required")
		}
		reader := multipart.NewReader(bytes.NewReader(raw), params["boundary"])
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return result, err
			}
			name := part.FormName()
			imagePart := name == "image" || name == "image[]" || name == "mask"
			limit := int64(64 * 1024)
			if imagePart {
				limit = 50 * 1024 * 1024
			}
			data, err := io.ReadAll(io.LimitReader(part, limit+1))
			part.Close()
			if err != nil {
				return result, err
			}
			if int64(len(data)) > limit {
				return result, fmt.Errorf("image edit field %q exceeds size limit", name)
			}
			if imagePart {
				kind := http.DetectContentType(data)
				if kind != "image/png" && kind != "image/jpeg" && kind != "image/webp" {
					return result, fmt.Errorf("image edit requires PNG, JPEG or WebP input")
				}
				url := "data:" + kind + ";base64," + base64.StdEncoding.EncodeToString(data)
				if name == "mask" {
					if mask != nil || kind != "image/png" {
						return result, fmt.Errorf("image edit accepts one PNG mask")
					}
					mask = map[string]interface{}{"image_url": url}
				} else {
					images = append(images, map[string]interface{}{"type": "input_image", "image_url": url})
				}
				if len(images) > 16 {
					return result, fmt.Errorf("image edit accepts at most 16 images")
				}
			} else {
				value := strings.TrimSpace(string(data))
				switch name {
				case "n", "output_compression":
					var number int
					if json.Unmarshal(data, &number) != nil {
						return result, fmt.Errorf("image edit field %q must be an integer", name)
					}
					fields[name] = number
				default:
					fields[name] = value
				}
			}
		}
	case "application/json":
		if err := json.Unmarshal(raw, &fields); err != nil {
			return result, err
		}
		refs, ok := fields["images"].([]interface{})
		if !ok {
			return result, fmt.Errorf("image edit requires images with image_url")
		}
		for _, ref := range refs {
			item, ok := ref.(map[string]interface{})
			if !ok {
				return result, fmt.Errorf("invalid image edit reference")
			}
			url := strings.TrimSpace(stringFromAny(item["image_url"]))
			if err := validateEditImageURL(url); err != nil {
				return result, err
			}
			images = append(images, map[string]interface{}{"type": "input_image", "image_url": url})
		}
		if value, exists := fields["mask"]; exists {
			item, ok := value.(map[string]interface{})
			if !ok {
				return result, fmt.Errorf("image edit mask requires image_url")
			}
			url := strings.TrimSpace(stringFromAny(item["image_url"]))
			if err := validateEditImageURL(url); err != nil {
				return result, err
			}
			mask = map[string]interface{}{"image_url": url}
		}
	default:
		return result, fmt.Errorf("image edits require multipart/form-data or application/json")
	}
	if len(images) == 0 || len(images) > 16 {
		return result, fmt.Errorf("image edit requires 1 to 16 images")
	}
	encoded, err := json.Marshal(fields)
	if err != nil {
		return result, err
	}
	result, err = decodeOpenAIImageGenerationRequest(encoded)
	result.InputImages, result.Mask = images, mask
	return result, err
}

func validateEditImageURL(url string) error {
	if strings.HasPrefix(url, "data:image/") {
		header, encoded, ok := strings.Cut(url, ",")
		if !ok || (header != "data:image/png;base64" && header != "data:image/jpeg;base64" && header != "data:image/webp;base64") {
			return fmt.Errorf("image edit requires a base64 PNG, JPEG or WebP data URL")
		}
		if len(encoded) > 70*1024*1024 {
			return fmt.Errorf("image edit data URL exceeds size limit")
		}
		data, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil || len(data) == 0 || len(data) > 50*1024*1024 {
			return fmt.Errorf("invalid image edit base64 data")
		}
		return nil
	}
	if url == "" {
		return fmt.Errorf("image edit requires image_url; account-scoped file_id is not supported")
	}
	return security.ValidateOutboundURL(url)
}
