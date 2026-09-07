package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"LoadBalanceProvider/src/domain"
)

const (
	MinConversationAffinityTTLMinutes = 1
	MaxConversationAffinityTTLMinutes = 7 * 24 * 60
	MinResponseRouteMaxEntries        = 100
	MaxResponseRouteMaxEntries        = 100000
	MinProviderCapacityCooldownSecs   = 1
	MaxProviderCapacityCooldownSecs   = 300
	MinBindingsPerProvider            = 1
	MaxBindingsPerProviderLimit       = 1000
	MinYieldThresholdPercent          = 0
	MaxYieldThresholdPercent          = 100
	MinDemotionRequestsPerMinute      = 0.1
	MaxDemotionRequestsPerMinute      = 600.0
	MinDemotionReasoningPercent       = 0
	MaxDemotionReasoningPercent       = 100
	MinDemotionTargetTier             = 1
	MaxDemotionTargetTier             = 10
	MinDemotionMinutes                = 1
	MaxDemotionMinutes                = 24 * 60
	MinDemotionDailyUsagePercent      = 0
	MaxDemotionDailyUsagePercent      = 100
)

// -------------------------------------------------------------------------------------
func DefaultAdvancedSettingsConfig() domain.AdvancedSettingsConfig {
	return domain.AdvancedSettingsConfig{
		ProviderRetryRounds:                      2,
		ProviderRetrySourcesPerRound:             0,
		ProviderRetryWaitSeconds:                 30,
		PersistQuotaCooldown:                     true,
		ConversationAffinityTTLMinutes:           30,
		ConversationAffinityQuotaTolerancePoints: 10,
		ResponseRouteMaxEntries:                  2000,
		ProviderCapacityCooldownSeconds:          10,
		ProviderServerErrorCooldownSeconds:       30,
		MaxBindingsPerProvider:                   8,
		YieldLowMaxPercent:                       2,
		YieldMidMaxPercent:                       20,
		LowReasoningDemotionEnabled:              false,
		LowReasoningDemotionRequestsPerMin:       8,
		LowReasoningDemotionReasoningPercent:     10,
		LowReasoningDemotionTargetTier:           4,
		LowReasoningDemotionMinutes:              10,
		LowReasoningDemotionMinDailyUsagePercent: 18,
	}
}

// -------------------------------------------------------------------------------------
func LoadAdvancedSettingsConfig(_path string) (domain.AdvancedSettingsConfig, error) {
	if strings.TrimSpace(_path) == "" {
		_path = "data/advanced_settings.json"
	}

	_bytes, _err := os.ReadFile(_path)
	if os.IsNotExist(_err) {
		return DefaultAdvancedSettingsConfig(), nil
	}
	if _err != nil {
		return domain.AdvancedSettingsConfig{}, _err
	}

	_config := DefaultAdvancedSettingsConfig()
	if _err := json.Unmarshal(_bytes, &_config); _err != nil {
		return domain.AdvancedSettingsConfig{}, _err
	}
	applyLegacyAdvancedSettingsKeys(_bytes, &_config)
	if _err := ValidateAdvancedSettingsConfig(_config); _err != nil {
		return domain.AdvancedSettingsConfig{}, _err
	}
	return _config, nil
}

// -------------------------------------------------------------------------------------
// applyLegacyAdvancedSettingsKeys 讓改名前存下的設定檔仍然生效。
// max_bindings_per_provider 原本叫 max_conversations_per_provider，
// 直接改名會讓既有設定被悄悄重設回預設值。
func applyLegacyAdvancedSettingsKeys(_bytes []byte, _config *domain.AdvancedSettingsConfig) {
	var _keys struct {
		MaxBindings      *int `json:"max_bindings_per_provider"`
		MaxConversations *int `json:"max_conversations_per_provider"`
	}
	if _err := json.Unmarshal(_bytes, &_keys); _err != nil {
		return
	}
	if _keys.MaxBindings == nil && _keys.MaxConversations != nil {
		_config.MaxBindingsPerProvider = *_keys.MaxConversations
	}
}

// -------------------------------------------------------------------------------------
func SaveAdvancedSettingsConfig(_path string, _config domain.AdvancedSettingsConfig) error {
	if _err := ValidateAdvancedSettingsConfig(_config); _err != nil {
		return _err
	}
	if strings.TrimSpace(_path) == "" {
		_path = "data/advanced_settings.json"
	}

	_dir := filepath.Dir(_path)
	if _dir != "." && _dir != "" {
		if _err := os.MkdirAll(_dir, 0755); _err != nil {
			return _err
		}
	}

	_bytes, _err := json.MarshalIndent(_config, "", "  ")
	if _err != nil {
		return _err
	}
	_bytes = append(_bytes, '\n')
	return os.WriteFile(_path, _bytes, 0600)
}

// -------------------------------------------------------------------------------------
func ValidateAdvancedSettingsConfig(_config domain.AdvancedSettingsConfig) error {
	if _config.ProviderRetryRounds < 0 || _config.ProviderRetryRounds > 10 ||
		_config.ProviderRetrySourcesPerRound < 0 || _config.ProviderRetrySourcesPerRound > 100 ||
		_config.ProviderRetryWaitSeconds < 0 || _config.ProviderRetryWaitSeconds > 300 {
		return fmt.Errorf("重試額外輪數須為 0–10、每輪來源上限須為 0–100、累計等待須為 0–300 秒")
	}
	if _config.ConversationAffinityTTLMinutes < MinConversationAffinityTTLMinutes ||
		_config.ConversationAffinityTTLMinutes > MaxConversationAffinityTTLMinutes {
		return fmt.Errorf("conversation affinity TTL must be between %d and %d minutes", MinConversationAffinityTTLMinutes, MaxConversationAffinityTTLMinutes)
	}
	if _config.ConversationAffinityQuotaTolerancePoints < 0 || _config.ConversationAffinityQuotaTolerancePoints > 100 {
		return fmt.Errorf("conversation affinity quota tolerance must be between 0 and 100 percentage points")
	}
	if _config.ResponseRouteMaxEntries < MinResponseRouteMaxEntries || _config.ResponseRouteMaxEntries > MaxResponseRouteMaxEntries {
		return fmt.Errorf("response route max entries must be between %d and %d", MinResponseRouteMaxEntries, MaxResponseRouteMaxEntries)
	}
	if _config.ProviderCapacityCooldownSeconds < MinProviderCapacityCooldownSecs ||
		_config.ProviderCapacityCooldownSeconds > MaxProviderCapacityCooldownSecs {
		return fmt.Errorf("provider capacity cooldown must be between %d and %d seconds", MinProviderCapacityCooldownSecs, MaxProviderCapacityCooldownSecs)
	}
	if _config.ProviderServerErrorCooldownSeconds < 1 || _config.ProviderServerErrorCooldownSeconds > 300 {
		return fmt.Errorf("上游伺服器錯誤冷卻必須是 1 到 300 秒的整數")
	}
	if _config.MaxBindingsPerProvider < MinBindingsPerProvider ||
		_config.MaxBindingsPerProvider > MaxBindingsPerProviderLimit {
		return fmt.Errorf("max bindings per provider must be between %d and %d", MinBindingsPerProvider, MaxBindingsPerProviderLimit)
	}
	if _config.YieldLowMaxPercent < MinYieldThresholdPercent ||
		_config.YieldLowMaxPercent > MaxYieldThresholdPercent {
		return fmt.Errorf("low yield threshold must be between %d and %d percent", MinYieldThresholdPercent, MaxYieldThresholdPercent)
	}
	if _config.YieldMidMaxPercent < MinYieldThresholdPercent ||
		_config.YieldMidMaxPercent > MaxYieldThresholdPercent {
		return fmt.Errorf("medium yield threshold must be between %d and %d percent", MinYieldThresholdPercent, MaxYieldThresholdPercent)
	}
	if _config.YieldLowMaxPercent >= _config.YieldMidMaxPercent {
		return fmt.Errorf("low yield threshold must be lower than medium yield threshold")
	}
	if _config.LowReasoningDemotionRequestsPerMin < MinDemotionRequestsPerMinute ||
		_config.LowReasoningDemotionRequestsPerMin > MaxDemotionRequestsPerMinute {
		return fmt.Errorf("demotion request rate must be between %g and %g per minute", MinDemotionRequestsPerMinute, MaxDemotionRequestsPerMinute)
	}
	if _config.LowReasoningDemotionReasoningPercent < MinDemotionReasoningPercent ||
		_config.LowReasoningDemotionReasoningPercent > MaxDemotionReasoningPercent {
		return fmt.Errorf("demotion reasoning threshold must be between %d and %d percent", MinDemotionReasoningPercent, MaxDemotionReasoningPercent)
	}
	if _config.LowReasoningDemotionTargetTier < MinDemotionTargetTier ||
		_config.LowReasoningDemotionTargetTier > MaxDemotionTargetTier {
		return fmt.Errorf("demotion target tier must be between %d and %d", MinDemotionTargetTier, MaxDemotionTargetTier)
	}
	if _config.LowReasoningDemotionMinutes < MinDemotionMinutes ||
		_config.LowReasoningDemotionMinutes > MaxDemotionMinutes {
		return fmt.Errorf("demotion duration must be between %d and %d minutes", MinDemotionMinutes, MaxDemotionMinutes)
	}
	if _config.LowReasoningDemotionMinDailyUsagePercent < MinDemotionDailyUsagePercent ||
		_config.LowReasoningDemotionMinDailyUsagePercent > MaxDemotionDailyUsagePercent {
		return fmt.Errorf("demotion daily usage gate must be between %d and %d percent", MinDemotionDailyUsagePercent, MaxDemotionDailyUsagePercent)
	}
	return nil
}
