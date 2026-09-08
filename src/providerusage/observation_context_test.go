package providerusage

import (
	"testing"
	"time"
)

func TestQuotaObservationDoesNotCountReboundsOrUnknownBaseline(t *testing.T) {
	usage := DayUsage{MeasurementVersion: 2}
	at := time.Now()
	context := ObservationContext{Series: "account:api:weekly", ResetAt: at.Add(time.Hour).Unix()}
	for i, remaining := range []float64{80, 81, 80, 79} {
		recordContextObservation(&usage, remaining, at.Add(time.Duration(i)*time.Second).Format(time.RFC3339Nano), context)
	}
	if usage.AccumulatedUsedPercent != 1 || usage.QuotaResetCount != 0 {
		t.Fatalf("回彈被重複計入: %+v", usage)
	}
	context.Series = "account:headers:weekly"
	recordContextObservation(&usage, 50, at.Add(time.Minute).Format(time.RFC3339Nano), context)
	if usage.AccumulatedUsedPercent != 1 || !usage.Incomplete {
		t.Fatalf("不可比較的來源被拼接: %+v", usage)
	}
	recordContextObservation(&usage, 10, at.Format(time.RFC3339Nano), context)
	if usage.CurrentRemaining != 50 {
		t.Fatal("較舊的觀測覆蓋了新資料")
	}
}

func TestExplicitQuotaWindowResetKeepsPriorConsumption(t *testing.T) {
	at := time.Now()
	usage := DayUsage{MeasurementVersion: 2}
	context := ObservationContext{Series: "account:api:weekly", ResetAt: at.Add(time.Minute).Unix()}
	recordContextObservation(&usage, 90, at.Format(time.RFC3339Nano), context)
	recordContextObservation(&usage, 80, at.Add(time.Second).Format(time.RFC3339Nano), context)
	context.ResetAt = at.Add(time.Hour).Unix()
	recordContextObservation(&usage, 100, at.Add(time.Minute).Format(time.RFC3339Nano), context)
	recordContextObservation(&usage, 95, at.Add(2*time.Minute).Format(time.RFC3339Nano), context)
	if usage.AccumulatedUsedPercent != 15 || usage.QuotaResetCount != 1 {
		t.Fatalf("重設窗口累計不正確: %+v", usage)
	}
}

func TestAccountAPIFallbackDoesNotDuplicateConsumption(t *testing.T) {
	usage := DayUsage{MeasurementVersion: 2}
	at := time.Now()
	api := ObservationContext{Series: "a:api:weekly", Account: "a", Source: "account_api"}
	header := ObservationContext{Series: "a:headers:weekly", Account: "a", Source: "headers"}
	for i, sample := range []struct {
		remaining float64
		context   ObservationContext
	}{
		{80, api}, {81, header}, {80, header}, {80, api}, {81, header}, {80, header}, {79, api},
	} {
		recordContextObservation(&usage, sample.remaining, at.Add(time.Duration(i)*time.Second).Format(time.RFC3339Nano), sample.context)
	}
	if usage.AccumulatedUsedPercent != 1 || !usage.Incomplete {
		t.Fatalf("備援來源重複計入消耗: %+v", usage)
	}
}
