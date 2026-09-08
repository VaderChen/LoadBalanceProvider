package balancer

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"LoadBalanceProvider/src/domain"
)

func TestEightAccountUsageProbeRecoverySmoke(t *testing.T) {
	config := &domain.ProxyConfig{}
	for i := 0; i < 8; i++ {
		config.Providers = append(config.Providers, domain.LLMProviderConfig{ID: fmt.Sprintf("usage-%d", i), Enabled: true, Kind: "openai-codex", Type: "openai-codex"})
	}
	b := NewLoadBalancer(config)
	for _, p := range b.ProvidersSnapshot() {
		var admitted atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if p.BeginAccountUsageProbe(time.Now(), true) {
					admitted.Add(1)
				}
			}()
		}
		wg.Wait()
		if admitted.Load() != 1 {
			t.Fatalf("同帳號用量查詢重複送出: %d", admitted.Load())
		}
		p.EndAccountUsageProbe(errors.New("temporary failure"))
	}
	fresh := b.ConfigSnapshot()
	b.ReloadConfig(&fresh)
	for _, p := range b.ProvidersSnapshot() {
		failed := p.UsageProbeStatus()
		if failed["running"] != false || failed["error"] == "" || p.BeginAccountUsageProbe(time.Now(), false) {
			t.Fatalf("失敗查詢的鎖或冷卻未保留: %+v", failed)
		}
		if !p.BeginAccountUsageProbe(failed["next_due"].(time.Time), false) {
			t.Fatal("查詢到期後不能恢復")
		}
		if !p.RecordCodexAccountUsage(http.Header{"X-Codex-Primary-Used-Percent": {"30"}}, AccountUsageFreshness, p.Config.ID) {
			t.Fatal("恢復後用量未更新")
		}
		p.EndAccountUsageProbe(nil)
		success := p.UsageProbeStatus()
		if success["running"] != false || success["error"] != "" || success["last_success"].(time.Time).IsZero() || p.UsageSnapshot().OverallRemainingPercent() != 70 {
			t.Fatalf("成功查詢未恢復狀態: %+v", success)
		}
		if success["next_due"].(time.Time).Sub(success["last_success"].(time.Time)) != AccountUsageRefreshInterval {
			t.Fatal("成功查詢沒有重設下一次輪詢時間")
		}
	}
}
