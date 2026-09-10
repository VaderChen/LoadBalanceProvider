package domain

import (
	"testing"
	"time"
)

func TestDowntimeSmokeBoundaries(t *testing.T) {
	for _, tc := range []struct {
		start, end, instant string
		want                bool
	}{
		{"04:00", "05:00", "2026-09-09T19:59:59Z", false},
		{"04:00", "05:00", "2026-09-09T20:00:00Z", true},
		{"04:00", "05:00", "2026-09-09T20:59:59Z", true},
		{"04:00", "05:00", "2026-09-09T21:00:00Z", false},
		{"23:00", "01:00", "2026-09-10T22:59:59+08:00", false},
		{"23:00", "01:00", "2026-09-10T23:00:00+08:00", true},
		{"23:00", "01:00", "2026-09-11T00:30:00+08:00", true},
		{"23:00", "01:00", "2026-09-11T01:00:00+08:00", false},
	} {
		t.Run(tc.start+"/"+tc.instant, func(t *testing.T) {
			p := &LLMProviderConfig{Enabled: true, Downtime: &ProviderDowntime{Enabled: true, Start: tc.start, End: tc.end}}
			now, err := time.Parse(time.RFC3339, tc.instant)
			if err != nil {
				t.Fatal(err)
			}
			if got := p.InScheduledDowntime(now); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
			p.Downtime.Enabled = false
			if p.InScheduledDowntime(now) {
				t.Fatal("關閉排程仍停機")
			}
		})
	}
}

func TestDowntimeSmokeValidationAndManualDisable(t *testing.T) {
	for _, d := range []ProviderDowntime{
		{Start: "04:00", End: "04:00"}, {Start: "24:00", End: "05:00"},
		{Start: "4:00", End: "05:00"}, {Start: "04:00", End: ""},
	} {
		if d.Validate() == nil {
			t.Fatalf("接受無效排程: %+v", d)
		}
	}
	p := &LLMProviderConfig{Enabled: false, Downtime: &ProviderDowntime{Start: "04:00", End: "05:00"}}
	if p.AvailableNow() {
		t.Fatal("排程關閉後誤啟用手動停用來源")
	}
	p.Enabled = true
	p.Downtime = nil
	if !p.AvailableNow() {
		t.Fatal("舊設定不相容")
	}
}
