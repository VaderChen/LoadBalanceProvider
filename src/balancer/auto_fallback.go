package balancer

import (
	"strings"

	"LoadBalanceProvider/src/domain"
)

// long_context 是分析器依 32K 門檻推導的標記；備援仍驗證模型真正的 token 上限。
func autoFallbackProfile(req *domain.ChatCompletionRequest, profile domain.RequestProfile) domain.RequestProfile {
	for _, capability := range req.RequiredCapabilities {
		if strings.EqualFold(strings.TrimSpace(capability), "long_context") {
			return profile
		}
	}
	profile.HardRequirements = append([]string(nil), profile.HardRequirements...)
	filtered := profile.HardRequirements[:0]
	for _, capability := range profile.HardRequirements {
		if !strings.EqualFold(strings.TrimSpace(capability), "long_context") {
			filtered = append(filtered, capability)
		}
	}
	profile.HardRequirements = filtered
	return profile
}
