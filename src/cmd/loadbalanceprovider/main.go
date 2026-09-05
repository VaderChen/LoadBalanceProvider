package main

// -------------------------------------------------------------------------------------
import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"LoadBalanceProvider/src/api"
	"LoadBalanceProvider/src/auth"
	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/benchmark"
	"LoadBalanceProvider/src/config"
	"LoadBalanceProvider/src/desktop"
	"LoadBalanceProvider/src/history"
	"LoadBalanceProvider/src/keyusage"
	"LoadBalanceProvider/src/notification"
	"LoadBalanceProvider/src/providerusage"
	"LoadBalanceProvider/src/proxy"
	"LoadBalanceProvider/src/service"
	"LoadBalanceProvider/src/systemmonitor"

	"github.com/MarsSemi/MarsCloud-SaaS/SDK/MarsService"
	"github.com/MarsSemi/MarsCloud-SaaS/SDK/Tools"
)

// -------------------------------------------------------------------------------------
func RunService() {
	_ctx, _stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer _stop()

	_agentPath := "agent.properties"
	if _err := ensureAgentProperties(_agentPath, "agent.sample.properties"); _err != nil {
		Tools.Log.Print(Tools.LL_Info, "agent.properties 初始化失敗: "+_err.Error())
	}
	if _err := config.EnsureAgentPropertiesSecretsEncrypted(_agentPath); _err != nil {
		Tools.Log.Print(Tools.LL_Info, "agent.properties 密碼欄位加密失敗: "+_err.Error())
	}
	ensureTLSPemProperties(_agentPath)
	_agentConfig, _agentErr := config.LoadAgentConfig(_agentPath)
	if _agentErr != nil {
		Tools.Log.Print(Tools.LL_Info, "agent.properties 載入失敗: "+_agentErr.Error())
	}

	_proxyConfigPath := "data/llm_proxy.json"
	_proxyConfig, _err := config.LoadProxyConfig(_agentPath, _proxyConfigPath)
	if _err != nil {
		Tools.Log.Print(Tools.LL_Info, "LLM Proxy 設定載入失敗，服務將以空 Provider 清單啟動: "+_err.Error())
		_proxyConfig = config.DefaultProxyConfig()
	}

	_balancer := balancer.NewLoadBalancer(_proxyConfig)
	_proxyClient := proxy.NewClient()
	_serviceHandler := &service.CloudService{Balancer: _balancer}

	_marsService := MarsService.Create(_agentPath, _serviceHandler)
	_systemMonitor := systemmonitor.New(".")
	_systemMonitor.Start(_ctx)

	_httpAPI := &api.HTTPAPI{
		Balancer:               _balancer,
		Client:                 _proxyClient,
		ConfigPath:             _proxyConfigPath,
		NotificationConfigPath: "data/notification_target.json",
		MCPSettingsConfigPath:  "data/mcp_settings.json",
		BenchmarkManager:       benchmark.NewManager(),
		SystemMonitor:          _systemMonitor,
		DefaultAccount:         _agentConfig.DefaultAccount,
		DefaultPassword:        _agentConfig.DefaultPassword,
	}

	startAPIKeyCleanup(_ctx)
	_proxyClient.StartProviderUsageRefresher(_ctx, _balancer)
	_proxyClient.StartProviderUsageDailyAccounting(_ctx, _balancer)
	notification.StartProviderMonitor(_ctx, _balancer, "data/notification_target.json")

	registerHTTPAPIRoutes(_marsService, _httpAPI, _agentConfig.HTTPBasePaths)

	// 向 MarsCloud 服務註冊、並啟動 MarsClient 與服務相關連線。
	_marsService.RegistryServerInfo("1.0.0", "pack", true)
	_marsService.Start()

	<-_ctx.Done()
	if _err := auth.DefaultAPIKeyStore().Flush(); _err != nil {
		Tools.Log.Print(Tools.LL_Info, "API Key 使用次數儲存失敗: "+_err.Error())
	}
	if _err := keyusage.DefaultRecorder().Flush(); _err != nil {
		Tools.Log.Print(Tools.LL_Info, "API Key 每月統計儲存失敗: "+_err.Error())
	}
	if _err := providerusage.DefaultRecorder().Flush(); _err != nil {
		Tools.Log.Print(Tools.LL_Info, "Provider 額度每日統計儲存失敗: "+_err.Error())
	}
	if _err := history.Flush(); _err != nil {
		Tools.Log.Print(Tools.LL_Info, "聊天歷史儲存失敗: "+_err.Error())
	}
}

// -------------------------------------------------------------------------------------
func registerHTTPAPIRoutes(_marsService *MarsService.MarsService, _httpAPI *api.HTTPAPI, _basePaths []string) {
	if _marsService == nil || _httpAPI == nil {
		return
	}
	_seen := map[string]bool{}
	for _, _basePath := range httpBasePaths(_basePaths) {
		_mcpRoute := "/mcp"
		if _basePath != "" {
			_mcpRoute = _basePath + _mcpRoute
		}
		if !_seen[_mcpRoute] {
			_seen[_mcpRoute] = true
			_marsService.AddRestfulAPI(_mcpRoute, _httpAPI)
		}
		for _, _apiRoot := range []string{"/v1", "/api", "/backend-api/codex"} {
			_route := _apiRoot
			if _basePath != "" {
				_route = _basePath + _apiRoot
			}
			if _seen[_route] {
				continue
			}
			_seen[_route] = true
			_marsService.AddRestfulAPI(_route, _httpAPI)
		}
	}
}

// -------------------------------------------------------------------------------------
func httpBasePaths(_configured []string) []string {
	_paths := []string{""}
	_add := func(_path string) {
		_path = strings.TrimSpace(_path)
		if _path == "" || _path == "/" {
			return
		}
		if !strings.HasPrefix(_path, "/") {
			_path = "/" + _path
		}
		_path = strings.TrimRight(_path, "/")
		for _, _existing := range _paths {
			if _existing == _path {
				return
			}
		}
		_paths = append(_paths, _path)
	}

	for _, _path := range _configured {
		_add(_path)
	}
	for _, _path := range strings.Split(os.Getenv("LBP_HTTP_BASE_PATHS"), ",") {
		_add(_path)
	}
	return _paths
}

// -------------------------------------------------------------------------------------
func startAPIKeyCleanup(_ctx context.Context) {
	go func() {
		_ticker := time.NewTicker(time.Minute)
		defer _ticker.Stop()
		for {
			select {
			case <-_ctx.Done():
				return
			case <-_ticker.C:
				if _err := auth.DefaultAPIKeyStore().PruneExpiredTemporary(); _err != nil {
					Tools.Log.Print(Tools.LL_Info, "過期臨時 API Key 清理失敗: "+_err.Error())
				}
			}
		}
	}()
}

// -------------------------------------------------------------------------------------
func ensureAgentProperties(_agentPath string, _samplePath string) error {
	if _, _err := os.Stat(_agentPath); _err == nil {
		return nil
	} else if !os.IsNotExist(_err) {
		return _err
	}

	_bytes, _err := os.ReadFile(_samplePath)
	if _err != nil {
		return _err
	}
	return os.WriteFile(_agentPath, _bytes, 0644)
}

// -------------------------------------------------------------------------------------
func main() {
	if len(os.Args) > 1 && os.Args[1] == "--desktop" {
		cleanup, err := desktop.Prepare()
		if err != nil {
			fmt.Fprintln(os.Stderr, "安裝版啟動失敗:", err)
			os.Exit(1)
		}
		defer cleanup()
	}
	runtime.GOMAXPROCS(runtime.NumCPU())

	Tools.EnableUncaughtExceptionHandler("Load Balance Provider", 3, func() { Tools.Log.Print(Tools.LL_Info, "System Error !!") })
	Tools.Log.SetDisplayLevel(Tools.LL_Info)

	RunService()
}

// -------------------------------------------------------------------------------------
