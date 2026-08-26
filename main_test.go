package main

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/pprof/profile"
)

func TestParseTime(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		in   string
		want time.Time
	}{
		{"now", now},
		{"", now},
		{"-6h", now.Add(-6 * time.Hour)},
		{"6h", now.Add(-6 * time.Hour)}, // bare duration reads as "ago"
		{"-90m", now.Add(-90 * time.Minute)},
		{"2026-08-26T06:00:00Z", time.Date(2026, 8, 26, 6, 0, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		got, err := parseTime(tc.in, now)
		if err != nil {
			t.Errorf("parseTime(%q): %v", tc.in, err)
			continue
		}
		if !got.Equal(tc.want) {
			t.Errorf("parseTime(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
	if _, err := parseTime("yesterday", now); err == nil {
		t.Error("parseTime(\"yesterday\") should fail")
	}
}

func TestParseWindowRejectsInverted(t *testing.T) {
	if _, _, err := parseWindow("now", "-1h"); err == nil {
		t.Error("start after end should be rejected")
	}
}

func TestSelector(t *testing.T) {
	const pt = "parca_agent:samples:count:cpu:nanoseconds:delta"
	tests := []struct{ label, value, extra, want string }{
		{"", "", "", pt},
		{"cluster", "tc", "", pt + `{cluster="tc"}`},
		{"", "", `comm="clickhouse"`, pt + `{comm="clickhouse"}`},
		{"cluster", "tc", `comm="clickhouse"`, pt + `{cluster="tc",comm="clickhouse"}`},
	}
	for _, tc := range tests {
		if got := selector(pt, tc.label, tc.value, tc.extra); got != tc.want {
			t.Errorf("selector(%q,%q,%q) = %q, want %q", tc.label, tc.value, tc.extra, got, tc.want)
		}
	}
}

// The count->duration conversion is the one piece of arithmetic that silently
// scales every number in the report, so pin it down.
func TestScaleToSeconds(t *testing.T) {
	countProfile := &profile.Profile{
		Period:     10_000_000, // 10ms between samples
		PeriodType: &profile.ValueType{Type: "cpu", Unit: "nanoseconds"},
	}
	got, err := scaleToSeconds(countProfile, 100, "count")
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(got-1.0) > 1e-9 {
		t.Errorf("100 samples x 10ms = %v s, want 1", got)
	}

	if _, err := scaleToSeconds(&profile.Profile{}, 100, "count"); err == nil {
		t.Error("counts without a period must error, not silently report zero")
	}

	got, err = scaleToSeconds(&profile.Profile{}, 2_500_000_000, "nanoseconds")
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(got-2.5) > 1e-9 {
		t.Errorf("got %v s, want 2.5", got)
	}
}

func TestCores(t *testing.T) {
	// 30 CPU-seconds over a 60s window is half a core busy on average.
	if got := cores(30, time.Minute); math.Abs(got-0.5) > 1e-9 {
		t.Errorf("cores = %v, want 0.5", got)
	}
	if got := cores(30, 0); got != 0 {
		t.Errorf("zero window must not divide by zero, got %v", got)
	}
}

func TestValueIndexPrefersTimeUnit(t *testing.T) {
	p := &profile.Profile{SampleType: []*profile.ValueType{
		{Type: "samples", Unit: "count"},
		{Type: "cpu", Unit: "nanoseconds"},
	}}
	idx, unit := valueIndex(p)
	if idx != 1 || unit != "nanoseconds" {
		t.Errorf("valueIndex = (%d,%q), want (1,\"nanoseconds\")", idx, unit)
	}
}

func TestTopFunctions(t *testing.T) {
	fnA := &profile.Function{ID: 1, Name: "A"}
	fnB := &profile.Function{ID: 2, Name: "B"}
	locA := &profile.Location{ID: 1, Line: []profile.Line{{Function: fnA}}}
	locB := &profile.Location{ID: 2, Line: []profile.Line{{Function: fnB}}}

	p := &profile.Profile{
		SampleType: []*profile.ValueType{{Type: "samples", Unit: "count"}},
		Period:     10_000_000, // 10ms
		PeriodType: &profile.ValueType{Type: "cpu", Unit: "nanoseconds"},
		Function:   []*profile.Function{fnA, fnB},
		Location:   []*profile.Location{locA, locB},
		// B called A: pprof orders locations leaf-first.
		Sample: []*profile.Sample{{Location: []*profile.Location{locA, locB}, Value: []int64{100}}},
	}

	rows, err := topFunctions(p, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]Row{}
	for _, r := range rows {
		byName[r.Name] = r
	}
	// 100 samples x 10ms = 1 CPU-second over a 1s window = 1.0 cores.
	if math.Abs(byName["A"].Cores-1.0) > 1e-9 || math.Abs(byName["B"].Cores-1.0) > 1e-9 {
		t.Errorf("both frames should be cumulative 1.0: %+v", byName)
	}
	// Only the leaf carries self time.
	if math.Abs(byName["A"].Flat-1.0) > 1e-9 {
		t.Errorf("leaf A flat = %v, want 1.0", byName["A"].Flat)
	}
	if byName["B"].Flat != 0 {
		t.Errorf("caller B flat = %v, want 0", byName["B"].Flat)
	}
}

func TestFuncNameBucketsUnsymbolized(t *testing.T) {
	if got := funcName(profile.Line{}); got != "[unsymbolized]" {
		t.Errorf("got %q", got)
	}
}

func TestShortErrTrimsGRPCBoilerplate(t *testing.T) {
	tests := []struct{ in, want string }{
		{"rpc error: code = DeadlineExceeded desc = context deadline exceeded", "context deadline exceeded"},
		{"merge query \"x\": boom", "boom"},
		{"plain", "plain"},
	}
	for _, tc := range tests {
		if got := shortErr(errors.New(tc.in)); got != tc.want {
			t.Errorf("shortErr(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
