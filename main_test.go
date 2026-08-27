package main

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	qgrpc "buf.build/gen/go/parca-dev/parca/grpc/go/parca/query/v1alpha1/queryv1alpha1grpc"
	qv1 "buf.build/gen/go/parca-dev/parca/protocolbuffers/go/parca/query/v1alpha1"
	"github.com/google/pprof/profile"
	"google.golang.org/grpc"
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

// fakeQuery implements just the two metadata calls the tests exercise.
// Embedding the interface satisfies the rest; anything else panics, which is
// the correct outcome for a call these tests did not intend to make.
type fakeQuery struct {
	qgrpc.QueryServiceClient
	names     []string
	values    map[string][]string
	namesErr  error
	valuesErr error
	block     time.Duration // make Values hang, to exercise the deadline
}

func (f *fakeQuery) Labels(ctx context.Context, _ *qv1.LabelsRequest, _ ...grpc.CallOption) (*qv1.LabelsResponse, error) {
	if f.namesErr != nil {
		return nil, f.namesErr
	}
	return &qv1.LabelsResponse{LabelNames: f.names}, nil
}

func (f *fakeQuery) Values(ctx context.Context, in *qv1.ValuesRequest, _ ...grpc.CallOption) (*qv1.ValuesResponse, error) {
	if f.block > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(f.block):
		}
	}
	if f.valuesErr != nil {
		return nil, f.valuesErr
	}
	return &qv1.ValuesResponse{LabelValues: f.values[in.GetLabelName()]}, nil
}

func testClient(f *fakeQuery, timeout time.Duration) *Client {
	return &Client{q: f, timeout: timeout}
}

// The bug this guards: a label query with no deadline of its own inherited the
// process-wide one and stalled the run for minutes.
func TestMetadataQueriesRespectTimeout(t *testing.T) {
	c := testClient(&fakeQuery{block: time.Hour}, 20*time.Millisecond)
	start := time.Now()
	_, err := c.LabelValues(context.Background(), "cluster", start.Add(-time.Hour), start)
	if err == nil {
		t.Fatal("a hung Values call must fail, not hang")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("took %s; the per-query timeout did not apply", elapsed)
	}
	if !strings.Contains(err.Error(), "timed out") || !strings.Contains(err.Error(), "--timeout") {
		t.Errorf("a deadline should say so and say what to do about it, got: %v", err)
	}
}

// The three readings of an empty values response must not collapse into one
// message. Getting this wrong once meant a transient failure was reported as
// an empty window.
func TestExplainNoValuesDistinguishesTheReasons(t *testing.T) {
	start, end := time.Now().Add(-time.Hour), time.Now()
	tests := []struct {
		name string
		f    *fakeQuery
		want string
	}{
		{
			name: "label is absent from the window",
			f:    &fakeQuery{names: []string{"node", "comm"}},
			want: `no label "cluster"`,
		},
		{
			name: "window holds nothing at all",
			f:    &fakeQuery{names: nil},
			want: "no labels at all",
		},
		{
			// Parca listed the label, so a series carries it -- yet the values
			// query found none. That contradiction is the values query's fault.
			name: "label exists but yielded no values",
			f:    &fakeQuery{names: []string{"cluster", "node"}},
			want: "contradictory",
		},
		{
			name: "cross-check itself failed",
			f:    &fakeQuery{namesErr: errors.New("unavailable")},
			want: "retry before believing it",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := explainNoValues(context.Background(), testClient(tc.f, 0), "cluster", start, end)
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

// `parcareport labels typo` used to print nothing and exit 0.
func TestListLabelsRejectsAnUnknownLabel(t *testing.T) {
	c := testClient(&fakeQuery{names: []string{"node"}}, 0)
	err := listLabels(context.Background(), c, "cluster", time.Now().Add(-time.Hour), time.Now())
	if err == nil {
		t.Fatal("an unknown label must not look like an empty success")
	}
}
