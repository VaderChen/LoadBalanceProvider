package providerusage

import (
	"math"
	"path/filepath"
	"testing"
	"time"
)

// -------------------------------------------------------------------------------------
func TestRecorderAggregatesCompletedDailyWindows(t *testing.T) {
	_recorder := NewRecorder(filepath.Join(t.TempDir(), "provider_usage"))
	_dayStart := time.Date(2026, time.August, 10, 0, 0, 0, 0, time.Local)
	_dayEnd := time.Date(2026, time.August, 10, 23, 59, 0, 0, time.Local)

	if _err := _recorder.RecordDayStart("provider-a", 90, _dayStart); _err != nil {
		t.Fatalf("record provider-a start: %v", _err)
	}
	if _err := _recorder.RecordDayEnd("provider-a", 52, _dayEnd); _err != nil {
		t.Fatalf("record provider-a end: %v", _err)
	}
	if _err := _recorder.RecordDayStart("provider-b", 100, _dayStart); _err != nil {
		t.Fatalf("record provider-b start: %v", _err)
	}
	if _err := _recorder.RecordDayEnd("provider-b", 70, _dayEnd); _err != nil {
		t.Fatalf("record provider-b end: %v", _err)
	}
	if _err := _recorder.Flush(); _err != nil {
		t.Fatalf("flush: %v", _err)
	}

	_stats, _err := _recorder.LoadMonth([]string{"provider-a", "provider-b"}, "2026-08")
	if _err != nil {
		t.Fatalf("load month: %v", _err)
	}
	if _stats.ProviderCount != 2 || len(_stats.Days) != 1 {
		t.Fatalf("stats = %#v", _stats)
	}
	_day := _stats.Days[0]
	if _day.Date != "2026-08-10" || _day.ProviderCount != 2 {
		t.Fatalf("day = %#v", _day)
	}
	if math.Abs(_day.UsagePercent-34) > 0.001 || math.Abs(_day.RemainingPercent-61) > 0.001 {
		t.Fatalf("aggregate percentages = %.1f / %.1f; want 34.0 / 61.0", _day.UsagePercent, _day.RemainingPercent)
	}
}

// -------------------------------------------------------------------------------------
func TestRecorderExcludesProvidersWithoutCompletedDayFromDailyAverage(t *testing.T) {
	_recorder := NewRecorder(filepath.Join(t.TempDir(), "provider_usage"))
	_start := time.Date(2026, time.February, 28, 0, 0, 0, 0, time.Local)
	_end := time.Date(2026, time.February, 28, 23, 59, 0, 0, time.Local)
	if _err := _recorder.RecordDayStart("provider-a", 100, _start); _err != nil {
		t.Fatalf("record start: %v", _err)
	}
	if _err := _recorder.RecordDayEnd("provider-a", 85, _end); _err != nil {
		t.Fatalf("record end: %v", _err)
	}

	_stats, _err := _recorder.LoadMonth([]string{"provider-a", "provider-missing"}, "2026-02")
	if _err != nil {
		t.Fatalf("load month: %v", _err)
	}
	if len(_stats.Days) != 1 || _stats.Days[0].ProviderCount != 1 || _stats.Days[0].UsagePercent != 15 {
		t.Fatalf("stats = %#v", _stats)
	}
}

// -------------------------------------------------------------------------------------
func TestRecorderUsesStartEndBoundariesAndAvoidsNegativeUsageAfterReset(t *testing.T) {
	_recorder := NewRecorder(filepath.Join(t.TempDir(), "provider_usage"))
	_dayOneStart := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.Local)
	_dayOneEnd := time.Date(2026, time.August, 1, 23, 59, 0, 0, time.Local)
	_dayTwoStart := time.Date(2026, time.August, 2, 0, 0, 0, 0, time.Local)
	_dayTwoEnd := time.Date(2026, time.August, 2, 23, 59, 0, 0, time.Local)
	_dayThreeEnd := time.Date(2026, time.August, 3, 23, 59, 0, 0, time.Local)

	for _, _record := range []struct {
		start bool
		at    time.Time
		value float64
	}{
		{true, _dayOneStart, 90},
		{false, _dayOneEnd, 72},
		{true, _dayTwoStart, 72},
		{false, _dayTwoEnd, 88},   // Quota reset during the day: 72 - 88 must become 0.
		{false, _dayThreeEnd, 40}, // 缺少日初基準，不推測已消耗 60%。
	} {
		var _err error
		if _record.start {
			_err = _recorder.RecordDayStart("provider-a", _record.value, _record.at)
		} else {
			_err = _recorder.RecordDayEnd("provider-a", _record.value, _record.at)
		}
		if _err != nil {
			t.Fatalf("record boundary: %v", _err)
		}
	}

	_stats, _err := _recorder.LoadMonth([]string{"provider-a"}, "2026-08")
	if _err != nil {
		t.Fatalf("load month: %v", _err)
	}
	if len(_stats.Days) != 3 {
		t.Fatalf("days = %#v", _stats.Days)
	}

	_usageByDate := map[string]float64{}
	for _, _day := range _stats.Days {
		_usageByDate[_day.Date] = _day.UsagePercent
	}
	if _usageByDate["2026-08-01"] != 18 {
		t.Fatalf("August 1 usage = %.1f, want 18.0 from its own day boundaries", _usageByDate["2026-08-01"])
	}
	if _usageByDate["2026-08-02"] != 0 {
		t.Fatalf("August 2 usage = %.1f, want 0.0 after quota reset", _usageByDate["2026-08-02"])
	}
	if _usageByDate["2026-08-03"] != 0 {
		t.Fatalf("缺少基準卻回填消耗: %.1f", _usageByDate["2026-08-03"])
	}
}

// -------------------------------------------------------------------------------------
func TestRecorderReportsLiveDailyUsageBeforeDayEnd(t *testing.T) {
	_recorder := NewRecorder(filepath.Join(t.TempDir(), "provider_usage"))
	_start := time.Date(2026, time.August, 11, 0, 0, 0, 0, time.Local)
	_noon := time.Date(2026, time.August, 11, 12, 0, 0, 0, time.Local)

	if _err := _recorder.RecordDayStart("provider-a", 90, _start); _err != nil {
		t.Fatalf("record day start: %v", _err)
	}
	if _err := _recorder.Record("provider-a", 15, 75, _noon); _err != nil {
		t.Fatalf("record live usage: %v", _err)
	}

	_stats, _err := _recorder.LoadMonth([]string{"provider-a"}, "2026-08")
	if _err != nil {
		t.Fatalf("load month: %v", _err)
	}
	if len(_stats.Days) != 1 {
		t.Fatalf("days = %#v", _stats.Days)
	}
	_day := _stats.Days[0]
	if _day.UsagePercent != 15 || _day.RemainingPercent != 75 || _day.Completed {
		t.Fatalf("live day = %#v", _day)
	}
}

// -------------------------------------------------------------------------------------
func TestRecorderDayStartReplacesMidnightProbeWithBoundaryBaseline(t *testing.T) {
	_recorder := NewRecorder(filepath.Join(t.TempDir(), "provider_usage"))
	_start := time.Date(2026, time.August, 11, 0, 0, 0, 0, time.Local)

	// Daily accounting refreshes the provider first; that refresh records a live sample
	// before RecordDayStart captures the explicit midnight boundary.
	if _err := _recorder.Record("provider-a", 20, 80, _start); _err != nil {
		t.Fatalf("record midnight probe: %v", _err)
	}
	if _err := _recorder.RecordDayStart("provider-a", 80, _start); _err != nil {
		t.Fatalf("record day start: %v", _err)
	}
	if _err := _recorder.Record("provider-a", 25, 75, _start.Add(time.Hour)); _err != nil {
		t.Fatalf("record live usage: %v", _err)
	}

	_stats, _err := _recorder.LoadMonth([]string{"provider-a"}, "2026-08")
	if _err != nil {
		t.Fatalf("load month: %v", _err)
	}
	if len(_stats.Days) != 1 || _stats.Days[0].UsagePercent != 5 || _stats.Days[0].RemainingPercent != 75 {
		t.Fatalf("stats = %#v", _stats)
	}
}

// -------------------------------------------------------------------------------------
func TestRecorderAccumulatesUsageAcrossSameDayQuotaReset(t *testing.T) {
	_recorder := NewRecorder(filepath.Join(t.TempDir(), "provider_usage"))
	_start := time.Date(2026, time.August, 11, 0, 0, 0, 0, time.Local)

	context := ObservationContext{Series: "account:api:window", ResetAt: _start.Add(10 * time.Hour).Unix()}
	if _err := _recorder.RecordBoundaryObservation("provider-a", 100, _start, _start, context, true); _err != nil {
		t.Fatalf("record day start: %v", _err)
	}
	if _err := _recorder.RecordObservation("provider-a", 20, 80, _start.Add(8*time.Hour), context); _err != nil {
		t.Fatalf("record first live usage: %v", _err)
	}
	context.ResetAt = _start.Add(24 * time.Hour).Unix()
	if _err := _recorder.RecordObservation("provider-a", 10, 90, _start.Add(12*time.Hour), context); _err != nil {
		t.Fatalf("record reset live usage: %v", _err)
	}
	if _err := _recorder.RecordObservation("provider-a", 25, 75, _start.Add(16*time.Hour), context); _err != nil {
		t.Fatalf("record usage after reset: %v", _err)
	}

	_stats, _err := _recorder.LoadMonth([]string{"provider-a"}, "2026-08")
	if _err != nil {
		t.Fatalf("load month: %v", _err)
	}
	if len(_stats.Days) != 1 || _stats.Days[0].UsagePercent != 35 || _stats.Days[0].RemainingPercent != 75 {
		t.Fatalf("stats = %#v", _stats)
	}
}

// -------------------------------------------------------------------------------------
func TestRecorderPreservesAccumulatedUsageAcrossReloadAndMultipleResets(t *testing.T) {
	_root := filepath.Join(t.TempDir(), "provider_usage")
	_start := time.Date(2026, time.August, 12, 0, 0, 0, 0, time.Local)
	_recorder := NewRecorder(_root)

	context := ObservationContext{Series: "account:api:window", ResetAt: _start.Add(5 * time.Hour).Unix()}
	if _err := _recorder.RecordBoundaryObservation("provider-a", 100, _start, _start, context, true); _err != nil {
		t.Fatalf("record day start: %v", _err)
	}
	if _err := _recorder.RecordObservation("provider-a", 20, 80, _start.Add(4*time.Hour), context); _err != nil {
		t.Fatalf("record first segment: %v", _err)
	}
	context.ResetAt = _start.Add(10 * time.Hour).Unix()
	if _err := _recorder.RecordObservation("provider-a", 0, 100, _start.Add(5*time.Hour), context); _err != nil {
		t.Fatalf("record first reset: %v", _err)
	}
	if _err := _recorder.Flush(); _err != nil {
		t.Fatalf("flush: %v", _err)
	}

	_reloaded := NewRecorder(_root)
	if _err := _reloaded.RecordObservation("provider-a", 10, 90, _start.Add(8*time.Hour), context); _err != nil {
		t.Fatalf("record second segment: %v", _err)
	}
	context.ResetAt = _start.Add(15 * time.Hour).Unix()
	if _err := _reloaded.RecordObservation("provider-a", 0, 100, _start.Add(10*time.Hour), context); _err != nil {
		t.Fatalf("record second reset: %v", _err)
	}
	if _err := _reloaded.RecordObservation("provider-a", 5, 95, _start.Add(12*time.Hour), context); _err != nil {
		t.Fatalf("record third segment: %v", _err)
	}

	_stats, _err := _reloaded.LoadMonth([]string{"provider-a"}, "2026-08")
	if _err != nil {
		t.Fatalf("load month: %v", _err)
	}
	if len(_stats.Days) != 1 || _stats.Days[0].UsagePercent != 35 || _stats.Days[0].RemainingPercent != 95 {
		t.Fatalf("stats = %#v", _stats)
	}
}

// -------------------------------------------------------------------------------------
// 今日用量查詢：沒有觀測時必須回報「不知道」，不能回 0 ——
// 它會被當成降級偵測的啟動門檻，0 與「未知」的後果完全相反。
func TestTodayUsagePercentReportsUnknown(t *testing.T) {
	_recorder := NewRecorder(filepath.Join(t.TempDir(), "provider-usage"))
	_now := time.Now()

	if _, _known := _recorder.TodayUsagePercent([]string{"p1"}, _now); _known {
		t.Fatalf("no observation yet must report unknown")
	}

	if _err := _recorder.RecordDayStart("p1", 100, _now); _err != nil {
		t.Fatal(_err)
	}
	if _err := _recorder.Record("p1", 0, 78, _now); _err != nil {
		t.Fatal(_err)
	}

	_usage, _known := _recorder.TodayUsagePercent([]string{"p1"}, _now)
	if !_known {
		t.Fatalf("observation exists, should be known")
	}
	if _usage < 21.9 || _usage > 22.1 {
		t.Fatalf("today usage = %v, want ~22", _usage)
	}
}

// -------------------------------------------------------------------------------------
// 跨 provider 取平均，語意與 LoadMonth 的當日數值一致。
func TestTodayUsagePercentMatchesLoadMonth(t *testing.T) {
	_recorder := NewRecorder(filepath.Join(t.TempDir(), "provider-usage"))
	_now := time.Now()
	for _id, _remaining := range map[string]float64{"p1": 70, "p2": 90} {
		if _err := _recorder.RecordDayStart(_id, 100, _now); _err != nil {
			t.Fatal(_err)
		}
		if _err := _recorder.Record(_id, 0, _remaining, _now); _err != nil {
			t.Fatal(_err)
		}
	}

	_usage, _known := _recorder.TodayUsagePercent([]string{"p1", "p2"}, _now)
	if !_known {
		t.Fatalf("should be known")
	}

	_stats, _err := _recorder.LoadMonth([]string{"p1", "p2"}, _now.Format("2006-01"))
	if _err != nil {
		t.Fatal(_err)
	}
	_today := _now.Format("2006-01-02")
	for _, _day := range _stats.Days {
		if _day.Date != _today {
			continue
		}
		if _usage != _day.UsagePercent {
			t.Fatalf("lightweight query disagrees with LoadMonth: %v != %v", _usage, _day.UsagePercent)
		}
		return
	}
	t.Fatalf("today missing from LoadMonth: %+v", _stats.Days)
}
