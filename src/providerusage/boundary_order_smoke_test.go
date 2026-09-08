package providerusage

import (
	"testing"
	"time"
)

func TestDailyBoundaryOrderingSmoke(t *testing.T) {
	r := NewRecorder(t.TempDir())
	t.Cleanup(func() { _ = r.Flush() })
	at := time.Date(2026, 9, 8, 23, 58, 0, 0, time.Local)
	if err := r.RecordDayStart("p1", 100, at); err != nil {
		t.Fatal(err)
	}
	if err := r.RecordDayEnd("p1", 80, at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	for _, delayedBoundary := range []bool{false, true} {
		if delayedBoundary {
			if err := r.RecordDayEnd("p1", 90, at.Add(30*time.Second)); err != nil {
				t.Fatal(err)
			}
		} else if err := r.Record("p1", 30, 70, at.Add(90*time.Second)); err != nil {
			t.Fatal(err)
		}
		stats, err := r.LoadMonth([]string{"p1"}, "2026-09")
		if err != nil || len(stats.Days) != 1 {
			t.Fatalf("讀取日統計失敗: %+v, %v", stats, err)
		}
		day := stats.Days[0]
		if day.UsagePercent != 30 || day.RemainingPercent != 70 || !day.Completed {
			t.Fatalf("日終快照蓋過較新觀測 (延遲=%t): %+v", delayedBoundary, day)
		}
	}
}
