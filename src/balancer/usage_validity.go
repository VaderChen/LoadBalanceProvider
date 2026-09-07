package balancer

import (
	"math"
	"strconv"
	"strings"
)

// 必須有可驗證的分子／分母或明確百分比；零值本身不表示資料存在。
func (s ProviderUsageSnapshot) KnownRemainingPercent() (float64, bool) {
	values := s.knownRemainingDimensions()
	remaining := 100.0
	for _, value := range values {
		remaining = math.Min(remaining, value)
	}
	return remaining, len(values) > 0
}

func usageFiniteNumber(text string) (float64, bool) {
	n, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
	return n, err == nil && !math.IsNaN(n) && !math.IsInf(n, 0) && n >= 0
}

func (s ProviderUsageSnapshot) knownRemainingDimensions() map[string]float64 {
	values := make(map[string]float64)
	for key, pair := range map[string][2]string{"requests": {s.LimitRequests, s.RemainingRequests}, "tokens": {s.LimitTokens, s.RemainingTokens}} {
		limit, okLimit := usageFiniteNumber(pair[0])
		remaining, okRemaining := usageFiniteNumber(pair[1])
		if okLimit && okRemaining && limit > 0 && remaining <= limit {
			values[key] = remaining / limit * 100
		}
	}
	for _, prefix := range []string{"primary", "secondary"} {
		if n, ok := usageFiniteNumber(s.Headers["x-codex-"+prefix+"-remaining-percent"]); ok && n <= 100 {
			values[prefix] = n
		} else if n, ok := usageFiniteNumber(s.Headers["x-codex-"+prefix+"-used-percent"]); ok && n <= 100 {
			values[prefix] = 100 - n
		}
	}
	return values
}

func (s ProviderUsageSnapshot) coversUsage(previous ProviderUsageSnapshot) bool {
	current := s.knownRemainingDimensions()
	for key := range previous.knownRemainingDimensions() {
		if _, ok := current[key]; !ok {
			return false
		}
	}
	return len(current) > 0
}
