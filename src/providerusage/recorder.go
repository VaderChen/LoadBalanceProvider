package providerusage

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultUsageRoot = "data/provider_usage"
	flushDelay       = 2 * time.Second
)

var (
	defaultRecorder = NewRecorder(defaultUsageRoot)
	monthPattern    = regexp.MustCompile(`^\d{4}-\d{2}$`)
	providerIDSafe  = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)
)

// -------------------------------------------------------------------------------------
type Recorder struct {
	Root       string
	lock       sync.Mutex
	months     map[string]MonthFile
	dirty      map[string]bool
	flushTimer *time.Timer
}

// -------------------------------------------------------------------------------------
type DayUsage struct {
	MeasurementSource      string  `json:"measurement_source,omitempty"`
	MeasurementAccount     string  `json:"measurement_account,omitempty"`
	Incomplete             bool    `json:"incomplete,omitempty"`
	Series                 string  `json:"series,omitempty"`
	ResetAt                int64   `json:"reset_at,omitempty"`
	LowWaterRemaining      float64 `json:"low_water_remaining,omitempty"`
	LowWaterKnown          bool    `json:"low_water_known,omitempty"`
	MeasurementVersion     int     `json:"measurement_version,omitempty"`
	UsedPercent            float64 `json:"used_percent"`
	RemainingPercent       float64 `json:"remaining_percent"`
	Observations           int64   `json:"observations"`
	AccumulatedUsedPercent float64 `json:"accumulated_used_percent,omitempty"`
	AccumulatedKnown       bool    `json:"accumulated_known,omitempty"`
	QuotaResetCount        int64   `json:"quota_reset_count,omitempty"`
	CurrentRemaining       float64 `json:"current_remaining_percent,omitempty"`
	CurrentKnown           bool    `json:"current_known,omitempty"`
	CurrentCapturedAt      string  `json:"current_captured_at,omitempty"`
	StartRemainingPercent  float64 `json:"start_remaining_percent,omitempty"`
	EndRemainingPercent    float64 `json:"end_remaining_percent,omitempty"`
	StartKnown             bool    `json:"start_known,omitempty"`
	EndKnown               bool    `json:"end_known,omitempty"`
	StartCapturedAt        string  `json:"start_captured_at,omitempty"`
	EndCapturedAt          string  `json:"end_captured_at,omitempty"`
	UpdatedAt              string  `json:"updated_at"`
}

type ObservationContext struct {
	Source  string
	Account string
	Series  string
	ResetAt int64
}

// -------------------------------------------------------------------------------------
type ProviderMonth struct {
	Days map[string]DayUsage `json:"days"`
}

// -------------------------------------------------------------------------------------
type MonthFile struct {
	Month     string                   `json:"month"`
	Providers map[string]ProviderMonth `json:"providers"`
	UpdatedAt string                   `json:"updated_at"`
}

// -------------------------------------------------------------------------------------
type DayStat struct {
	QualityWarning   bool    `json:"quality_warning,omitempty"`
	Date             string  `json:"date"`
	UsagePercent     float64 `json:"usage_percent"`
	RemainingPercent float64 `json:"remaining_percent"`
	ProviderCount    int     `json:"provider_count"`
	Completed        bool    `json:"completed"`
}

// -------------------------------------------------------------------------------------
type MonthStats struct {
	Month         string    `json:"month"`
	Days          []DayStat `json:"days"`
	ProviderCount int       `json:"provider_count"`
	ObservedDays  int       `json:"observed_days"`
	UpdatedAt     string    `json:"updated_at,omitempty"`
}

// -------------------------------------------------------------------------------------
func DefaultRecorder() *Recorder {
	return defaultRecorder
}

// -------------------------------------------------------------------------------------
func NewRecorder(_root string) *Recorder {
	_root = strings.TrimSpace(_root)
	if _root == "" {
		_root = defaultUsageRoot
	}
	return &Recorder{
		Root:   _root,
		months: map[string]MonthFile{},
		dirty:  map[string]bool{},
	}
}

// 缺少來源與額度窗口的舊呼叫仍可記錄，但會標示資料不完整。
// -------------------------------------------------------------------------------------
func (_r *Recorder) Record(_providerID string, _usedPercent float64, _remainingPercent float64, _at time.Time) error {
	return _r.RecordObservation(_providerID, _usedPercent, _remainingPercent, _at, ObservationContext{})
}

func (_r *Recorder) RecordObservation(_providerID string, _usedPercent float64, _remainingPercent float64, _at time.Time, context ObservationContext) error {
	if _r == nil {
		return nil
	}
	_providerID = safeProviderID(_providerID)
	if _providerID == "" {
		return nil
	}
	if _at.IsZero() {
		_at = time.Now()
	}
	_at = _at.Local()
	_month := _at.Format("2006-01")
	_day := _at.Format("2006-01-02")
	_usedPercent = clampPercent(_usedPercent)
	_remainingPercent = clampPercent(_remainingPercent)

	_r.lock.Lock()
	defer _r.lock.Unlock()

	_file, _err := _r.loadMonthLocked(_month)
	if _err != nil {
		return _err
	}
	if _file.Providers == nil {
		_file.Providers = map[string]ProviderMonth{}
	}
	_provider := _file.Providers[_providerID]
	if _provider.Days == nil {
		_provider.Days = map[string]DayUsage{}
	}
	_dayUsage := _provider.Days[_day]
	if previous, err := time.Parse(time.RFC3339Nano, _dayUsage.CurrentCapturedAt); err == nil && _at.Before(previous) {
		return nil
	}
	if !_dayUsage.CurrentKnown && !_dayUsage.StartKnown && !_dayUsage.EndKnown && _dayUsage.Observations == 0 {
		_dayUsage.MeasurementVersion = 2
	}
	_dayUsage.UsedPercent = _usedPercent
	_dayUsage.RemainingPercent = _remainingPercent
	recordContextObservation(&_dayUsage, _remainingPercent, _at.Format(time.RFC3339Nano), context)
	if _dayUsage.EndKnown {
		_dayUsage.EndRemainingPercent = _dayUsage.CurrentRemaining
		_dayUsage.EndCapturedAt = _dayUsage.CurrentCapturedAt
	}
	_dayUsage.Observations++
	_dayUsage.UpdatedAt = time.Now().Format(time.RFC3339)
	_provider.Days[_day] = _dayUsage
	_file.Providers[_providerID] = _provider
	_file.Month = _month
	_file.UpdatedAt = _dayUsage.UpdatedAt
	_r.months[_month] = _file
	_r.dirty[_month] = true
	_r.scheduleFlushLocked()
	return nil
}

// -------------------------------------------------------------------------------------
func (_r *Recorder) RecordDayStart(_providerID string, _remainingPercent float64, _at time.Time) error {
	return _r.RecordBoundaryObservation(_providerID, _remainingPercent, _at, _at, ObservationContext{}, true)
}

// -------------------------------------------------------------------------------------
func (_r *Recorder) RecordDayEnd(_providerID string, _remainingPercent float64, _at time.Time) error {
	return _r.RecordBoundaryObservation(_providerID, _remainingPercent, _at, _at, ObservationContext{}, false)
}

// -------------------------------------------------------------------------------------
func (_r *Recorder) RecordBoundaryObservation(_providerID string, _remainingPercent float64, _at, observedAt time.Time, context ObservationContext, _start bool) error {
	if _r == nil {
		return nil
	}
	_providerID = safeProviderID(_providerID)
	if _providerID == "" {
		return nil
	}
	if _at.IsZero() {
		_at = time.Now()
	}
	_at = _at.Local()
	_month := _at.Format("2006-01")
	_day := _at.Format("2006-01-02")
	_remainingPercent = clampPercent(_remainingPercent)
	if observedAt.IsZero() {
		observedAt = _at
	}
	_capturedAt := observedAt.Format(time.RFC3339Nano)

	_r.lock.Lock()
	defer _r.lock.Unlock()

	_file, _err := _r.loadMonthLocked(_month)
	if _err != nil {
		return _err
	}
	if _file.Providers == nil {
		_file.Providers = map[string]ProviderMonth{}
	}
	_provider := _file.Providers[_providerID]
	if _provider.Days == nil {
		_provider.Days = map[string]DayUsage{}
	}
	_dayUsage := _provider.Days[_day]
	if context.Series == "" {
		context = ObservationContext{Series: _dayUsage.Series, ResetAt: _dayUsage.ResetAt, Source: _dayUsage.MeasurementSource, Account: _dayUsage.MeasurementAccount}
	}
	if !_dayUsage.CurrentKnown && !_dayUsage.StartKnown && !_dayUsage.EndKnown && _dayUsage.Observations == 0 {
		_dayUsage.MeasurementVersion = 2
	}
	if _start {
		// A restart shortly after midnight must not replace the original baseline.
		if _dayUsage.StartKnown {
			return nil
		}
		_dayUsage.StartRemainingPercent = _remainingPercent
		_dayUsage.StartKnown = true
		_dayUsage.StartCapturedAt = _capturedAt
		// 午夜查詢與一般觀測可能交錯；不得歸零已經累積的消耗或覆寫較新的觀測。
		if !_dayUsage.CurrentKnown {
			recordContextObservation(&_dayUsage, _remainingPercent, _capturedAt, context)
		}
	} else {
		recordContextObservation(&_dayUsage, _remainingPercent, _capturedAt, context)
		// 日終擷取與串流標頭可交錯抵達，結算仍採已知最新的觀測。
		_dayUsage.EndRemainingPercent = _dayUsage.CurrentRemaining
		_dayUsage.EndKnown = true
		_dayUsage.EndCapturedAt = _dayUsage.CurrentCapturedAt
	}
	_dayUsage.UpdatedAt = time.Now().Format(time.RFC3339)
	_provider.Days[_day] = _dayUsage
	_file.Providers[_providerID] = _provider
	_file.Month = _month
	_file.UpdatedAt = _dayUsage.UpdatedAt
	_r.months[_month] = _file
	_r.dirty[_month] = true
	_r.scheduleFlushLocked()
	return nil
}

// -------------------------------------------------------------------------------------
// 每日用量為可比較觀測的累積消耗；缺少日初基準或切換窗口時標示不完整。
// 舊版累積值保留，不把缺少的觀測回填為滿額，也不宣稱可還原精確歷史用量。
// -------------------------------------------------------------------------------------
// TodayUsagePercent 回傳今日已消耗的配額百分比（跨 provider 平均），
// 與 LoadMonth 的當日數值採同一套語意。
//
// 它是輕量版：只讀今天那一筆，不複製整個月檔 —— 這條路徑會被每次請求的
// 降級判斷叫到，走 LoadMonth 會付出整月資料的複製成本。
//
// 第二個回傳值表示「今天有沒有可用的觀測」。沒有觀測時不能當成 0%：
// 那是「還不知道」，用它當門檻會在服務剛啟動時錯誤地放行所有偵測。
func (_r *Recorder) TodayUsagePercent(_providerIDs []string, _at time.Time) (float64, bool) {
	if _r == nil {
		return 0, false
	}
	if _at.IsZero() {
		_at = time.Now()
	}
	_at = _at.Local()
	_day := _at.Format("2006-01-02")
	_selected := selectedProviderIDs(_providerIDs)

	_r.lock.Lock()
	defer _r.lock.Unlock()

	_file, _err := _r.loadMonthLocked(_at.Format("2006-01"))
	if _err != nil {
		return 0, false
	}

	_total := 0.0
	_count := 0
	for _providerID := range _selected {
		_provider, _ok := _file.Providers[_providerID]
		if !_ok {
			continue
		}
		_usage, _ok := _provider.Days[_day]
		if !_ok {
			continue
		}
		_currentRemaining := 0.0
		switch {
		case _usage.EndKnown:
			_currentRemaining = _usage.EndRemainingPercent
		case _usage.CurrentKnown:
			_currentRemaining = _usage.CurrentRemaining
		default:
			continue
		}
		_usagePercent := _usage.AccumulatedUsedPercent
		if !_usage.AccumulatedKnown {
			_startRemaining := _currentRemaining
			if _usage.StartKnown {
				_startRemaining = _usage.StartRemainingPercent
			}
			_usagePercent = math.Max(0, _startRemaining-_currentRemaining)
		}
		_total += _usagePercent
		_count++
	}

	if _count == 0 {
		return 0, false
	}
	return roundUsagePercent(_total / float64(_count)), true
}

// -------------------------------------------------------------------------------------
func (_r *Recorder) LoadMonth(_providerIDs []string, _month string) (MonthStats, error) {
	if _r == nil {
		return MonthStats{}, fmt.Errorf("provider usage recorder is not initialized")
	}
	_month = normalizeMonth(_month)
	_selected := selectedProviderIDs(_providerIDs)

	_r.lock.Lock()
	_file, _err := _r.loadMonthLocked(_month)
	if _err == nil {
		_file = cloneMonthFile(_file)
	}
	_r.lock.Unlock()
	if _err != nil {
		return MonthStats{}, _err
	}

	type _daySample struct {
		QualityWarning   bool
		UsagePercent     float64
		RemainingPercent float64
		Completed        bool
	}
	_byDay := map[string][]_daySample{}
	for _providerID := range _selected {
		_provider, _ok := _file.Providers[_providerID]
		if !_ok {
			continue
		}
		for _day, _usage := range _provider.Days {
			if !strings.HasPrefix(_day, _month+"-") {
				continue
			}
			_currentRemaining := 0.0
			_completed := _usage.EndKnown
			if _completed {
				_currentRemaining = _usage.EndRemainingPercent
			} else if _usage.CurrentKnown {
				_currentRemaining = _usage.CurrentRemaining
			} else {
				// Legacy raw observations have no reliable same-day boundary semantics.
				continue
			}
			_usagePercent := _usage.AccumulatedUsedPercent
			if !_usage.AccumulatedKnown {
				_startRemaining := _currentRemaining
				if _usage.StartKnown {
					_startRemaining = _usage.StartRemainingPercent
				}
				_usagePercent = math.Max(0, _startRemaining-_currentRemaining)
			}
			_byDay[_day] = append(_byDay[_day], _daySample{
				QualityWarning:   _usage.MeasurementVersion < 2 || _usage.Incomplete || !_usage.StartKnown,
				UsagePercent:     _usagePercent,
				RemainingPercent: _currentRemaining,
				Completed:        _completed,
			})
		}
	}

	_days := make([]DayStat, 0, len(_byDay))
	for _day, _samples := range _byDay {
		var _usageTotal float64
		var _remainingTotal float64
		_completed := true
		_qualityWarning := false
		for _, _sample := range _samples {
			_qualityWarning = _qualityWarning || _sample.QualityWarning
			_usageTotal += _sample.UsagePercent
			_remainingTotal += _sample.RemainingPercent
			_completed = _completed && _sample.Completed
		}
		_days = append(_days, DayStat{
			QualityWarning:   _qualityWarning,
			Date:             _day,
			UsagePercent:     roundUsagePercent(_usageTotal / float64(len(_samples))),
			RemainingPercent: roundPercent(_remainingTotal / float64(len(_samples))),
			ProviderCount:    len(_samples),
			Completed:        _completed,
		})
	}
	sort.Slice(_days, func(_left int, _right int) bool {
		return _days[_left].Date < _days[_right].Date
	})

	return MonthStats{
		Month:         _month,
		Days:          _days,
		ProviderCount: len(_selected),
		ObservedDays:  len(_days),
		UpdatedAt:     _file.UpdatedAt,
	}, nil
}

func recordRemainingObservation(_usage *DayUsage, _remainingPercent float64, _capturedAt string) {
	if _usage == nil {
		return
	}
	recordContextObservation(_usage, _remainingPercent, _capturedAt, ObservationContext{Series: _usage.Series, ResetAt: _usage.ResetAt, Source: _usage.MeasurementSource, Account: _usage.MeasurementAccount})
}

// 同一序列只累積低水位下降；數值回彈不視為額度重設，來源切換不拼接消耗。
func recordContextObservation(usage *DayUsage, remaining float64, capturedAt string, context ObservationContext) {
	if usage == nil {
		return
	}
	remaining = clampPercent(remaining)
	if previous, err := time.Parse(time.RFC3339Nano, usage.CurrentCapturedAt); err == nil {
		if current, err := time.Parse(time.RFC3339Nano, capturedAt); err == nil && current.Before(previous) {
			return
		}
	}
	if context.Series == "" || usage.MeasurementVersion < 2 {
		usage.Incomplete = true
	}
	// 帳號 API 已建立基準後，標頭僅更新可用量展示，不另累積第二套消耗。
	// API 恢復時回到原本低水位，避免來源交替把同一段消耗算兩次。
	if usage.MeasurementSource == "account_api" && context.Source == "headers" && usage.MeasurementAccount == context.Account {
		usage.Incomplete = true
		usage.CurrentRemaining, usage.CurrentKnown, usage.CurrentCapturedAt = remaining, true, capturedAt
		return
	}
	if usage.CurrentKnown && remaining > usage.CurrentRemaining+0.0001 {
		usage.Incomplete = true
	}
	changed := usage.LowWaterKnown && (usage.Series != context.Series || usage.ResetAt != context.ResetAt)
	if changed {
		usage.Incomplete = true
		if usage.Series == context.Series && usage.ResetAt > 0 && context.ResetAt > usage.ResetAt {
			if at, err := time.Parse(time.RFC3339Nano, capturedAt); err == nil && at.Unix() >= usage.ResetAt {
				usage.QuotaResetCount++
			}
		}
	}
	if !usage.LowWaterKnown || changed {
		usage.LowWaterRemaining = remaining
		usage.LowWaterKnown = true
	} else if remaining < usage.LowWaterRemaining-0.0001 {
		usage.AccumulatedUsedPercent += usage.LowWaterRemaining - remaining
		usage.LowWaterRemaining = remaining
	}
	usage.AccumulatedKnown = true
	usage.Series, usage.ResetAt = context.Series, context.ResetAt
	usage.MeasurementSource, usage.MeasurementAccount = context.Source, context.Account
	usage.CurrentRemaining, usage.CurrentKnown, usage.CurrentCapturedAt = remaining, true, capturedAt
}

// Flush persists all queued records. It is safe to call on service shutdown.
// -------------------------------------------------------------------------------------
func (_r *Recorder) Flush() error {
	if _r == nil {
		return nil
	}
	_r.lock.Lock()
	defer _r.lock.Unlock()
	if _r.flushTimer != nil {
		_r.flushTimer.Stop()
		_r.flushTimer = nil
	}
	for _month := range _r.dirty {
		_file, _ok := _r.months[_month]
		if !_ok {
			delete(_r.dirty, _month)
			continue
		}
		if _err := _r.saveMonthLocked(_file); _err != nil {
			return _err
		}
		delete(_r.dirty, _month)
	}
	return nil
}

// -------------------------------------------------------------------------------------
func (_r *Recorder) loadMonthLocked(_month string) (MonthFile, error) {
	_month = normalizeMonth(_month)
	if _file, _ok := _r.months[_month]; _ok {
		return _file, nil
	}
	_path := _r.monthPath(_month)
	_bytes, _err := os.ReadFile(_path)
	if _err != nil {
		if os.IsNotExist(_err) {
			_file := MonthFile{Month: _month, Providers: map[string]ProviderMonth{}}
			_r.months[_month] = _file
			return _file, nil
		}
		return MonthFile{}, _err
	}

	_file := MonthFile{Month: _month, Providers: map[string]ProviderMonth{}}
	if len(_bytes) > 0 {
		if _err := json.Unmarshal(_bytes, &_file); _err != nil {
			return MonthFile{}, _err
		}
	}
	if _file.Month == "" {
		_file.Month = _month
	}
	if _file.Providers == nil {
		_file.Providers = map[string]ProviderMonth{}
	}
	_r.months[_month] = _file
	return _file, nil
}

// -------------------------------------------------------------------------------------
func (_r *Recorder) scheduleFlushLocked() {
	if _r.flushTimer != nil {
		return
	}
	_r.flushTimer = time.AfterFunc(flushDelay, func() {
		_ = _r.Flush()
	})
}

// -------------------------------------------------------------------------------------
func (_r *Recorder) saveMonthLocked(_file MonthFile) error {
	_path := _r.monthPath(_file.Month)
	if _err := os.MkdirAll(filepath.Dir(_path), 0755); _err != nil {
		return _err
	}
	_bytes, _err := json.MarshalIndent(_file, "", "  ")
	if _err != nil {
		return _err
	}
	_temporary, _err := os.CreateTemp(filepath.Dir(_path), filepath.Base(_path)+".tmp.*")
	if _err != nil {
		return _err
	}
	_temporaryPath := _temporary.Name()
	defer os.Remove(_temporaryPath)
	if _err := _temporary.Chmod(0600); _err != nil {
		_ = _temporary.Close()
		return _err
	}
	if _, _err := _temporary.Write(append(_bytes, '\n')); _err != nil {
		_ = _temporary.Close()
		return _err
	}
	if _err := _temporary.Sync(); _err != nil {
		_ = _temporary.Close()
		return _err
	}
	if _err := _temporary.Close(); _err != nil {
		return _err
	}
	return os.Rename(_temporaryPath, _path)
}

// -------------------------------------------------------------------------------------
func (_r *Recorder) monthPath(_month string) string {
	return filepath.Join(_r.Root, normalizeMonth(_month)+".json")
}

// -------------------------------------------------------------------------------------
func cloneMonthFile(_file MonthFile) MonthFile {
	_clone := MonthFile{
		Month:     _file.Month,
		Providers: map[string]ProviderMonth{},
		UpdatedAt: _file.UpdatedAt,
	}
	for _providerID, _provider := range _file.Providers {
		_days := make(map[string]DayUsage, len(_provider.Days))
		for _day, _usage := range _provider.Days {
			_days[_day] = _usage
		}
		_clone.Providers[_providerID] = ProviderMonth{Days: _days}
	}
	return _clone
}

// -------------------------------------------------------------------------------------
func selectedProviderIDs(_providerIDs []string) map[string]bool {
	_selected := map[string]bool{}
	for _, _providerID := range _providerIDs {
		if _providerID = safeProviderID(_providerID); _providerID != "" {
			_selected[_providerID] = true
		}
	}
	return _selected
}

// -------------------------------------------------------------------------------------
func normalizeMonth(_month string) string {
	_month = strings.TrimSpace(_month)
	if !monthPattern.MatchString(_month) {
		return time.Now().Local().Format("2006-01")
	}
	return _month
}

// -------------------------------------------------------------------------------------
func safeProviderID(_providerID string) string {
	_providerID = providerIDSafe.ReplaceAllString(strings.TrimSpace(_providerID), "_")
	if _providerID == "" || _providerID == "." || _providerID == ".." {
		return ""
	}
	return _providerID
}

// -------------------------------------------------------------------------------------
func clampPercent(_value float64) float64 {
	if math.IsNaN(_value) || math.IsInf(_value, 0) || _value < 0 {
		return 0
	}
	if _value > 100 {
		return 100
	}
	return _value
}

// -------------------------------------------------------------------------------------
func roundPercent(_value float64) float64 {
	return math.Round(clampPercent(_value)*10) / 10
}

// -------------------------------------------------------------------------------------
func roundUsagePercent(_value float64) float64 {
	if math.IsNaN(_value) || math.IsInf(_value, 0) || _value < 0 {
		return 0
	}
	return math.Round(_value*10) / 10
}
