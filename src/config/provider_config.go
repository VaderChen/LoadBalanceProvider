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
	defaultProviderMaxInputTokens  = 1048576
	defaultProviderMaxOutputTokens = 262144
)

// -------------------------------------------------------------------------------------
func LoadProxyConfig(_agentPath string, _proxyPath string) (*domain.ProxyConfig, error) {
	if _proxyPath != "" {
		_proxyConfig, _err := loadProxyConfigFile(_proxyPath)
		if _err == nil {
			ApplyDefaults(_proxyConfig)
			return _proxyConfig, nil
		}
	}

	_bytes, _err := os.ReadFile(_agentPath)
	if _err != nil {
		return nil, _err
	}

	var _agentConfig domain.AgentConfig
	if _err := json.Unmarshal(_bytes, &_agentConfig); _err != nil {
		return nil, _err
	}

	_config := _agentConfig.LLMProxy
	if !hasProxyConfig(&_config) {
		return nil, fmt.Errorf("llm proxy config not found: %s", _proxyPath)
	}

	ApplyDefaults(&_config)

	return &_config, nil
}

// -------------------------------------------------------------------------------------
func LoadAgentConfig(_agentPath string) (domain.AgentConfig, error) {
	setConfigSecretPath(_agentPath)

	_bytes, _err := os.ReadFile(_agentPath)
	if _err != nil {
		return domain.AgentConfig{}, _err
	}

	var _agentConfig domain.AgentConfig
	if _err := json.Unmarshal(_bytes, &_agentConfig); _err != nil {
		return domain.AgentConfig{}, _err
	}

	_defaultPassword, _err := DecryptConfigSecret(_agentConfig.DefaultPassword)
	if _err != nil {
		return domain.AgentConfig{}, fmt.Errorf("default_pwd decrypt failed: %w", _err)
	}
	_agentConfig.DefaultPassword = _defaultPassword

	return _agentConfig, nil
}

// -------------------------------------------------------------------------------------
func loadProxyConfigFile(_path string) (*domain.ProxyConfig, error) {
	_bytes, _err := os.ReadFile(_path)
	if _err != nil {
		return nil, _err
	}

	var _config domain.ProxyConfig
	if _err := json.Unmarshal(_bytes, &_config); _err != nil {
		return nil, _err
	}

	return &_config, nil
}

// -------------------------------------------------------------------------------------
func hasProxyConfig(_config *domain.ProxyConfig) bool {
	return _config.SelectionStrategy != "" || _config.RetryCount != 0 || len(_config.Providers) > 0
}

// -------------------------------------------------------------------------------------
func SaveProxyConfig(_path string, _config *domain.ProxyConfig) error {
	if _config == nil {
		_config = DefaultProxyConfig()
	}

	ApplyDefaults(_config)
	for _, provider := range _config.Providers {
		if provider.Downtime != nil {
			if err := provider.Downtime.Validate(); err != nil {
				return fmt.Errorf("provider %s: %w", provider.ID, err)
			}
		}
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
	_tmp, _err := os.CreateTemp(_dir, filepath.Base(_path)+".tmp.*")
	if _err != nil {
		return _err
	}
	_tmpPath := _tmp.Name()
	defer os.Remove(_tmpPath)
	if _err := _tmp.Chmod(0644); _err != nil {
		_tmp.Close()
		return _err
	}
	if _, _err := _tmp.Write(_bytes); _err != nil {
		_tmp.Close()
		return _err
	}
	if _err := _tmp.Sync(); _err != nil {
		_tmp.Close()
		return _err
	}
	if _err := _tmp.Close(); _err != nil {
		return _err
	}
	return os.Rename(_tmpPath, _path)
}

// -------------------------------------------------------------------------------------
func DefaultProxyConfig() *domain.ProxyConfig {
	_config := domain.ProxyConfig{
		SelectionStrategy: "random",
		RetryCount:        2,
		Providers:         []domain.LLMProviderConfig{},
	}
	ApplyDefaults(&_config)
	return &_config
}

// -------------------------------------------------------------------------------------
func ApplyDefaults(_config *domain.ProxyConfig) {
	if _config.SelectionStrategy == "" {
		_config.SelectionStrategy = "random"
	}
	if _config.RetryCount < 0 {
		_config.RetryCount = 0
	}

	for _idx := range _config.Providers {
		_provider := &_config.Providers[_idx]
		if _provider.Downtime == nil {
			_provider.Downtime = &domain.ProviderDowntime{Start: "04:00", End: "05:00"}
		}

		if _provider.ID == "" {
			_provider.ID = fmt.Sprintf("provider-%d", _idx+1)
		}
		if _provider.Name == "" {
			_provider.Name = _provider.ID
		}
		if _provider.Type == "" {
			_provider.Type = "openai_compatible"
		}
		applyProviderKindDefaults(_provider)
		if _provider.Role == "" {
			_provider.Role = "main"
		}
		if _provider.ChatCompletionsPath == "" {
			_provider.ChatCompletionsPath = "/v1/chat/completions"
		}
		if _provider.Weight <= 0 {
			_provider.Weight = 1
		}
		if _provider.TimeoutSeconds <= 0 {
			_provider.TimeoutSeconds = domain.DefaultProviderTimeoutSeconds
		}
		if _provider.MaxConcurrent <= 0 {
			_provider.MaxConcurrent = 4
		}

		for _modelIdx := range _provider.Models {
			_model := &_provider.Models[_modelIdx]
			_defaultMaxInputTokens := defaultMaxInputTokensForKind(_provider.Kind)
			if _model.MaxInputTokens <= 0 || shouldLiftMaxInputTokens(_provider.Kind, _model.MaxInputTokens, _defaultMaxInputTokens) {
				_model.MaxInputTokens = _defaultMaxInputTokens
			}
			_defaultMaxOutputTokens := defaultMaxOutputTokensForKind(_provider.Kind)
			if _model.MaxOutputTokens <= 0 || shouldLiftMaxOutputTokens(_provider.Kind, _model.MaxOutputTokens, _defaultMaxOutputTokens) {
				_model.MaxOutputTokens = _defaultMaxOutputTokens
			}
			if _model.CostTier <= 0 {
				_model.CostTier = 3
			}
			if _model.QualityTier <= 0 {
				_model.QualityTier = 3
			}
			if len(_model.Capabilities) == 0 {
				_model.Capabilities = []string{"chat"}
			}
		}
	}
}

// -------------------------------------------------------------------------------------
func defaultMaxInputTokensForKind(_kind string) int {
	return defaultProviderMaxInputTokens
}

// -------------------------------------------------------------------------------------
func defaultMaxOutputTokensForKind(_kind string) int {
	return defaultProviderMaxOutputTokens
}

// -------------------------------------------------------------------------------------
func shouldLiftMaxInputTokens(_kind string, _current int, _default int) bool {
	return _current < _default
}

// -------------------------------------------------------------------------------------
func shouldLiftMaxOutputTokens(_kind string, _current int, _default int) bool {
	return _current < _default
}

// -------------------------------------------------------------------------------------
func applyProviderKindDefaults(_provider *domain.LLMProviderConfig) {
	if _provider == nil {
		return
	}
	if !isOpenAICodexProvider(_provider) {
		return
	}

	_baseURL := strings.TrimRight(strings.TrimSpace(_provider.BaseURL), "/")
	_chatPath := strings.TrimSpace(_provider.ChatCompletionsPath)
	_hasAPIKey := strings.TrimSpace(_provider.APIKey) != "" || strings.TrimSpace(_provider.APIKeyEnv) != ""
	_openAIAPIBase := strings.Contains(strings.ToLower(_baseURL), "api.openai.com")
	_chatGPTBase := strings.Contains(strings.ToLower(_baseURL), "chatgpt.com")

	if _hasAPIKey && (_baseURL == "" || _chatGPTBase) {
		_provider.BaseURL = "https://api.openai.com"
	} else if !_hasAPIKey && (_baseURL == "" || _openAIAPIBase) {
		_provider.BaseURL = "https://chatgpt.com"
	}
	if _hasAPIKey && (_chatPath == "" || strings.EqualFold(_chatPath, "/backend-api/codex/responses") || strings.EqualFold(_chatPath, "/v1/chat/completions")) {
		_provider.ChatCompletionsPath = "/v1/responses"
	} else if !_hasAPIKey && (_chatPath == "" || strings.EqualFold(_chatPath, "/v1/responses") || strings.EqualFold(_chatPath, "/v1/chat/completions")) {
		_provider.ChatCompletionsPath = "/backend-api/codex/responses"
	}
}

// -------------------------------------------------------------------------------------
func isOpenAICodexProvider(_provider *domain.LLMProviderConfig) bool {
	_text := strings.ToLower(strings.TrimSpace(_provider.Kind + " " + _provider.Type + " " + _provider.Name + " " + _provider.ID))
	return strings.Contains(_text, "openai-codex") || strings.Contains(_text, "openai codex") || strings.Contains(_text, "codex")
}

// -------------------------------------------------------------------------------------
