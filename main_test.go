package main

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

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

func TestTruncateCountsRunesNotBytes(t *testing.T) {
	// Ten runes, but twenty bytes. Truncating at 10 must leave it be, which a
	// byte-based implementation would not have done.
	const s = "ФункцияАБВ"
	if got := truncate(s, 10); got != s {
		t.Errorf("truncate(%q, 10) = %q, want it unchanged", s, got)
	}
	// Cutting must land on a rune boundary, not mid-sequence.
	got := truncate(s, 5)
	if utf8.RuneCountInString(got) != 5 {
		t.Errorf("truncate(%q, 5) = %q, want 5 runes, got %d", s, got, utf8.RuneCountInString(got))
	}
	if !utf8.ValidString(got) {
		t.Errorf("truncate produced invalid UTF-8: %q", got)
	}
	// n <= 0 used to slice r[:-1] and panic.
	if got := truncate(s, 0); got != "" {
		t.Errorf("truncate(%q, 0) = %q, want empty", s, got)
	}
	if got := truncate(s, 1); got != "…" {
		t.Errorf("truncate(%q, 1) = %q, want the ellipsis alone", s, got)
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
	types     []*qv1.ProfileType
	typesErr  error
	block     time.Duration // make Values hang, to exercise the deadline
}

func (f *fakeQuery) ProfileTypes(ctx context.Context, _ *qv1.ProfileTypesRequest, _ ...grpc.CallOption) (*qv1.ProfileTypesResponse, error) {
	if f.typesErr != nil {
		return nil, f.typesErr
	}
	return &qv1.ProfileTypesResponse{Types: f.types}, nil
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

// The bug this guards: an explicit --profile-type still went through the
// ProfileTypes lookup, so a slow server killed a run that needed nothing
// discovered. A real run died exactly here with the full selector supplied.
func TestExplicitProfileTypeSurvivesAFailedLookup(t *testing.T) {
	const want = "parca_agent:samples:count:cpu:nanoseconds:delta"
	c := testClient(&fakeQuery{typesErr: errors.New("boom")}, 0)
	got, err := resolveProfileType(context.Background(), c, want)
	if err != nil {
		t.Fatalf("explicit type should survive a failed lookup, got %v", err)
	}
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// A typo must still be caught when the lookup does work, since an unknown
// selector otherwise comes back as an empty merge that reads like idleness.
func TestExplicitProfileTypeStillRejectsATypo(t *testing.T) {
	c := testClient(&fakeQuery{types: []*qv1.ProfileType{{
		Name: "parca_agent", SampleType: "samples", SampleUnit: "count",
		PeriodType: "cpu", PeriodUnit: "nanoseconds", Delta: true,
	}}}, 0)
	if _, err := resolveProfileType(context.Background(), c, "nope:x:y:z:w"); err == nil {
		t.Fatal("want an error for a selector the server does not offer")
	}
}

// Auto-detect keeps failing loudly: with no type asked for, there is nothing
// to fall back to.
func TestAutoDetectStillFailsWhenTheLookupFails(t *testing.T) {
	c := testClient(&fakeQuery{typesErr: errors.New("boom")}, 0)
	if _, err := resolveProfileType(context.Background(), c, ""); err == nil {
		t.Fatal("want an error when the lookup fails and no type was given")
	}
}

func TestPrintFailuresGroupsByCause(t *testing.T) {
	// Eight groups, two causes. A flat list capped at five would have shown
	// only the common one and hidden the other behind "and 3 more".
	var failed []failure
	for _, g := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		failed = append(failed, failure{group: "instance=" + g, msg: "stream terminated by RST_STREAM"})
	}
	failed = append(failed, failure{group: "instance=z", msg: "something else entirely"})

	out := captureStdout(t, func() { printFailures(failed) })

	if !strings.Contains(out, "something else entirely") {
		t.Errorf("the rare cause was hidden:\n%s", out)
	}
	if !strings.Contains(out, "(7 groups:") {
		t.Errorf("the common cause was not collapsed:\n%s", out)
	}
	// RST_STREAM says nothing actionable on its own, so a hint is added.
	if !strings.Contains(out, "narrower --from window") {
		t.Errorf("missing hint for a stream reset:\n%s", out)
	}
}

func TestHintForOnlyFiresWhenItHasSomethingToSay(t *testing.T) {
	if h := hintFor([]string{"no such label"}); h != "" {
		t.Errorf("want no hint, got %q", h)
	}
	if h := hintFor([]string{"context deadline exceeded"}); !strings.Contains(h, "--timeout") {
		t.Errorf("want a timeout hint, got %q", h)
	}
}

// Without the unfiltered merge there is no denominator, so the %TOTAL column
// must be absent rather than showing percentages of a subtotal that omits
// whatever is missing.
func TestGroupTableOmitsPercentagesWhenTheTotalIsUnknown(t *testing.T) {
	rows := []Row{{Name: "tc", Cores: 2}, {Name: "vps", Cores: 1}}

	known := captureStdout(t, func() { printGroupTable("CLUSTER", "CORES", rows, 3, true) })
	if !strings.Contains(known, "%TOTAL") || !strings.Contains(known, "TOTAL") {
		t.Errorf("want percentages when the total is known:\n%s", known)
	}

	unknown := captureStdout(t, func() { printGroupTable("CLUSTER", "CORES", rows, 3, false) })
	if strings.Contains(unknown, "%TOTAL") {
		t.Errorf("percentages must be dropped when the total is unknown:\n%s", unknown)
	}
	if !strings.Contains(unknown, "SUM OF LISTED") {
		t.Errorf("the subtotal must be labelled as such:\n%s", unknown)
	}
	if !strings.Contains(unknown, "2.000") {
		t.Errorf("group values should still be reported:\n%s", unknown)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var b strings.Builder
		io.Copy(&b, r)
		done <- b.String()
	}()
	fn()
	w.Close()
	os.Stdout = saved
	out := <-done
	r.Close()
	return out
}
