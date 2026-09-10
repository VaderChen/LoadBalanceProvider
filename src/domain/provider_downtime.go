package domain

import (
	"errors"
	"fmt"
	"time"
	_ "time/tzdata"
)

var ErrProviderScheduledDowntime = errors.New("來源目前為排程停機時段（Asia/Taipei）")

// 停機排程獨立於 Enabled，不會在時段結束時誤啟用手動停用的來源。
type ProviderDowntime struct {
	Enabled bool   `json:"enabled"`
	Start   string `json:"start"`
	End     string `json:"end"`
}

var providerScheduleLocation = func() *time.Location {
	location, err := time.LoadLocation("Asia/Taipei")
	if err != nil {
		panic(err)
	}
	return location
}()

func (d ProviderDowntime) Validate() error {
	start, err := time.Parse("15:04", d.Start)
	if err != nil || start.Format("15:04") != d.Start {
		return fmt.Errorf("停機開始時間須為 HH:MM")
	}
	end, err := time.Parse("15:04", d.End)
	if err != nil || end.Format("15:04") != d.End {
		return fmt.Errorf("停機結束時間須為 HH:MM")
	}
	if d.Start == d.End {
		return fmt.Errorf("停機開始與結束時間不可相同")
	}
	return nil
}

func (p *LLMProviderConfig) InScheduledDowntime(now time.Time) bool {
	if p == nil || p.Downtime == nil || !p.Downtime.Enabled {
		return false
	}
	d := p.Downtime
	// 設定檔若被外部寫入無效排程，保守停止派送。
	if d.Validate() != nil {
		return true
	}
	clock := now.In(providerScheduleLocation).Format("15:04")
	if d.Start < d.End {
		return clock >= d.Start && clock < d.End
	}
	return clock >= d.Start || clock < d.End
}

func (p *LLMProviderConfig) AvailableNow() bool {
	return p != nil && p.Enabled && !p.InScheduledDowntime(time.Now())
}

func (p *LLMProviderConfig) CheckScheduledDowntime() error {
	if p.InScheduledDowntime(time.Now()) {
		return ErrProviderScheduledDowntime
	}
	return nil
}
