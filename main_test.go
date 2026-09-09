package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	qgrpc "buf.build/gen/go/parca-dev/parca/grpc/go/parca/query/v1alpha1/queryv1alpha1grpc"
	qv1 "buf.build/gen/go/parca-dev/parca/protocolbuffers/go/parca/query/v1alpha1"
	"github.com/google/pprof/profile"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

	rows, err := topFunctions(p, time.Second, sortCum)
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
	// valuesErrFor fails only the named labels, so a partial failure can be
	// exercised without failing every query.
	valuesErrFor map[string]error
	// valuesDelay makes each Values call take time, so a bounded fan-out
	// actually has to queue.
	valuesDelay time.Duration
	types       []*qv1.ProfileType
	typesErr    error
	// merges answers MergePprof by selector. A selector with no entry gets an
	// empty pprof, which is what Parca returns for a window with no samples.
	merges    map[string]*profile.Profile
	mergeErrs map[string]error
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
	if f.valuesDelay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(f.valuesDelay):
		}
	}
	if err := f.valuesErrFor[in.GetLabelName()]; err != nil {
		return nil, err
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
	err := listLabels(context.Background(), c, "cluster", time.Now().Add(-time.Hour), time.Now(), 4)
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
	got, verified, err := resolveProfileType(context.Background(), c, want)
	if err != nil {
		t.Fatalf("explicit type should survive a failed lookup, got %v", err)
	}
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	// The caller needs to know the check never happened, so it can say so if
	// the report then comes back empty.
	if verified {
		t.Error("a failed lookup must not report the type as verified")
	}
}

// A typo must still be caught when the lookup does work, since an unknown
// selector otherwise comes back as an empty merge that reads like idleness.
func TestExplicitProfileTypeStillRejectsATypo(t *testing.T) {
	c := testClient(&fakeQuery{types: []*qv1.ProfileType{{
		Name: "parca_agent", SampleType: "samples", SampleUnit: "count",
		PeriodType: "cpu", PeriodUnit: "nanoseconds", Delta: true,
	}}}, 0)
	if _, _, err := resolveProfileType(context.Background(), c, "nope:x:y:z:w"); err == nil {
		t.Fatal("want an error for a selector the server does not offer")
	}
}

// Auto-detect keeps failing loudly: with no type asked for, there is nothing
// to fall back to.
func TestAutoDetectStillFailsWhenTheLookupFails(t *testing.T) {
	c := testClient(&fakeQuery{typesErr: errors.New("boom")}, 0)
	if _, _, err := resolveProfileType(context.Background(), c, ""); err == nil {
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

	out := captureStdout(t, func() { printFailures(failed, mergeQuery) })

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
	if h := hintFor([]string{"no such label"}, mergeQuery); h != "" {
		t.Errorf("want no hint, got %q", h)
	}
	if h := hintFor([]string{"context deadline exceeded"}, mergeQuery); !strings.Contains(h, "--timeout") {
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

// The bug this guards: the table was always ordered by cumulative value, so
// the top rows were runtime and framework frames. A real run put
// `runtime.goexit` first at 76.9% with 0.000 self time, while the largest self
// time anywhere in the top 15 was 0.120 -- the code actually burning CPU was
// below the cutoff and never printed.
func TestTopFunctionsSortsBySelfTimeByDefault(t *testing.T) {
	// goexit calls work: goexit is on every stack but runs no code itself.
	goexit := &profile.Function{ID: 1, Name: "runtime.goexit"}
	work := &profile.Function{ID: 2, Name: "app.work"}
	locGoexit := &profile.Location{ID: 1, Line: []profile.Line{{Function: goexit}}}
	locWork := &profile.Location{ID: 2, Line: []profile.Line{{Function: work}}}

	p := &profile.Profile{
		SampleType: []*profile.ValueType{{Type: "samples", Unit: "count"}},
		Period:     10_000_000,
		PeriodType: &profile.ValueType{Type: "cpu", Unit: "nanoseconds"},
		Function:   []*profile.Function{goexit, work},
		Location:   []*profile.Location{locGoexit, locWork},
		// Leaf-first: work is the leaf, goexit the caller.
		Sample: []*profile.Sample{{Location: []*profile.Location{locWork, locGoexit}, Value: []int64{100}}},
	}

	flat, err := topFunctions(p, time.Second, sortFlat)
	if err != nil {
		t.Fatal(err)
	}
	if flat[0].Name != "app.work" {
		t.Errorf("sorted by self time, want app.work first, got %q", flat[0].Name)
	}

	cum, err := topFunctions(p, time.Second, sortCum)
	if err != nil {
		t.Fatal(err)
	}
	// Both have cumulative 1.0, so the tie-break decides -- and self time is
	// the secondary key, which still puts the frame that ran the code first.
	if cum[0].Cores != cum[1].Cores {
		t.Fatalf("expected a cumulative tie, got %v", cum)
	}
	if cum[0].Name != "app.work" {
		t.Errorf("cumulative ties should fall back to self time, got %q", cum[0].Name)
	}
}

func TestParseSortKey(t *testing.T) {
	for _, in := range []string{"flat", "self"} {
		if got, err := parseSortKey(in); err != nil || got != sortFlat {
			t.Errorf("parseSortKey(%q) = (%v, %v), want sortFlat", in, got, err)
		}
	}
	for _, in := range []string{"cum", "cumulative"} {
		if got, err := parseSortKey(in); err != nil || got != sortCum {
			t.Errorf("parseSortKey(%q) = (%v, %v), want sortCum", in, got, err)
		}
	}
	if _, err := parseSortKey("sideways"); err == nil {
		t.Error("want an error for an unknown sort key")
	}
	// An unset field means the default, so a caller building options directly
	// does not have to know this one is load-bearing.
	if got, err := parseSortKey(""); err != nil || got != sortFlat {
		t.Errorf(`parseSortKey("") = (%v, %v), want sortFlat`, got, err)
	}
}

// The percentage must follow the sorted column, or the table shows rows
// ordered by one number and a percentage derived from another.
func TestFunctionTablePercentageFollowsTheSortedColumn(t *testing.T) {
	rows := []Row{{Name: "app.work", Cores: 1.0, Flat: 0.25}}

	flatOut := captureStdout(t, func() { printFunctionTable(rows, "CORES", 5, 1.0, sortFlat) })
	if !strings.Contains(flatOut, "FLAT*") {
		t.Errorf("the sorted column should be marked:\n%s", flatOut)
	}
	// 0.25 of a 1.0 total.
	if !strings.Contains(flatOut, "25.0") {
		t.Errorf("want the self-time percentage:\n%s", flatOut)
	}

	cumOut := captureStdout(t, func() { printFunctionTable(rows, "CORES", 5, 1.0, sortCum) })
	if !strings.Contains(cumOut, "CUM*") {
		t.Errorf("the sorted column should be marked:\n%s", cumOut)
	}
	if !strings.Contains(cumOut, "100.0") {
		t.Errorf("want the cumulative percentage:\n%s", cumOut)
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
	defer func() {
		os.Stdout = saved
		r.Close()
	}()
	fn()
	w.Close()
	return <-done
}

// cpuProfile builds a parca-agent-shaped CPU profile: sample values are stack
// counts, and the sampling period turns them into time.
func cpuProfile(t *testing.T, samples int64) *profile.Profile {
	t.Helper()
	fn := &profile.Function{ID: 1, Name: "app.work"}
	loc := &profile.Location{ID: 1, Line: []profile.Line{{Function: fn}}}
	return &profile.Profile{
		SampleType: []*profile.ValueType{{Type: "samples", Unit: "count"}},
		Period:     10_000_000, // 10ms
		PeriodType: &profile.ValueType{Type: "cpu", Unit: "nanoseconds"},
		Function:   []*profile.Function{fn},
		Location:   []*profile.Location{loc},
		Sample:     []*profile.Sample{{Location: []*profile.Location{loc}, Value: []int64{samples}}},
	}
}

func (f *fakeQuery) Query(ctx context.Context, in *qv1.QueryRequest, _ ...grpc.CallOption) (*qv1.QueryResponse, error) {
	sel := in.GetMerge().GetQuery()
	if err := f.mergeErrs[sel]; err != nil {
		return nil, err
	}
	p := f.merges[sel]
	if p == nil {
		return &qv1.QueryResponse{}, nil // no samples in this window
	}
	var buf bytes.Buffer
	if err := p.Write(&buf); err != nil {
		return nil, err
	}
	return &qv1.QueryResponse{Report: &qv1.QueryResponse_Pprof{Pprof: buf.Bytes()}}, nil
}

const testType = "parca_agent:samples:count:cpu:nanoseconds:delta"

func testOptions() options {
	return options{by: "cluster", profileType: testType, top: 0, concurrency: 4,
		timeout: time.Minute, sortBy: defaultSortBy}
}

func reportFixture(t *testing.T) *fakeQuery {
	t.Helper()
	return &fakeQuery{
		types: []*qv1.ProfileType{{
			Name: "parca_agent", SampleType: "samples", SampleUnit: "count",
			PeriodType: "cpu", PeriodUnit: "nanoseconds", Delta: true,
		}},
		values:    map[string][]string{"cluster": {"tc", "vps"}},
		names:     []string{"cluster"},
		merges:    map[string]*profile.Profile{},
		mergeErrs: map[string]error{},
	}
}

func runReport(t *testing.T, f *fakeQuery, o options) (string, error) {
	t.Helper()
	var err error
	out := captureStdout(t, func() {
		start := time.Now().Add(-time.Hour)
		err = report(context.Background(), testClient(f, time.Minute), o, start, time.Now())
	})
	return out, err
}

// The bug this guards: the unfiltered merge's failure returned early and threw
// away every group result that had already succeeded, so the main output of
// the command vanished because an auxiliary query failed.
func TestReportKeepsTheBreakdownWhenTheUnfilteredMergeFails(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.mergeErrs[testType] = errors.New("boom")

	out, err := runReport(t, f, testOptions())

	if err == nil {
		t.Error("a failed unfiltered merge must exit non-zero")
	}
	for _, want := range []string{"tc", "vps"} {
		if !strings.Contains(out, want) {
			t.Errorf("the breakdown should survive, missing %q:\n%s", want, out)
		}
	}
	// No denominator, so no percentages and no total that reads as fleet-wide.
	if strings.Contains(out, "%TOTAL") {
		t.Errorf("percentages must be dropped:\n%s", out)
	}
	if !strings.Contains(out, "SUM OF LISTED") {
		t.Errorf("the subtotal must be labelled as such:\n%s", out)
	}
	if !strings.Contains(out, "!! INCOMPLETE") {
		t.Errorf("the shortfall must be on stdout:\n%s", out)
	}
	// The function table is gone too, and saying so is part of being honest.
	if !strings.Contains(out, "hot-function table") {
		t.Errorf("the missing function table should be mentioned:\n%s", out)
	}
}

// The bug this guards: "all N queries failed" was computed from len(failed),
// so a run where one query failed and the rest were genuinely empty claimed
// every query had failed -- inverting the empty/failed distinction the tool
// exists to keep straight.
func TestReportDistinguishesFailedFromEmpty(t *testing.T) {
	f := reportFixture(t)
	f.values["cluster"] = []string{"a", "b", "c", "d"}
	// a, b, c return empty merges; only d fails.
	f.mergeErrs[testType+`{cluster="d"}`] = errors.New("boom")

	out, err := runReport(t, f, testOptions())

	if err == nil {
		t.Fatal("want a non-zero exit")
	}
	if strings.Contains(out, "all 1 cluster queries failed") {
		t.Errorf("must not claim every query failed:\n%s", out)
	}
	if !strings.Contains(out, "1 of 4") {
		t.Errorf("want both counts:\n%s", out)
	}
	if !strings.Contains(out, "no samples") {
		t.Errorf("the empty groups should be accounted for:\n%s", out)
	}
}

// A window that is genuinely empty must not be dressed up as a failure.
func TestReportSaysNoDataWhenNothingFailed(t *testing.T) {
	f := reportFixture(t)
	_, err := runReport(t, f, testOptions())
	if err == nil || !strings.Contains(err.Error(), "no data") {
		t.Errorf("want a plain no-data error, got %v", err)
	}
}

// The bug this guards: an unfiltered merge that came back empty left
// knowTotal true, so the group sum was printed as an authoritative
// fleet-wide TOTAL ... 100.0 that nothing had measured.
func TestReportDoesNotInventATotalFromAnEmptyOverallMerge(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	// The unfiltered selector has no entry, so it returns an empty pprof.

	out, err := runReport(t, f, testOptions())
	if err != nil {
		t.Fatalf("an empty overall merge is not an error: %v", err)
	}
	if strings.Contains(out, "%TOTAL") {
		t.Errorf("an unmeasured total must not carry percentages:\n%s", out)
	}
}

// The happy path still adds up, and the residual is reported rather than
// dropped.
func TestReportReportsTheUnlabeledResidual(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	// The unfiltered merge sees more than the two labelled clusters together.
	f.merges[testType] = cpuProfile(t, 200)

	out, err := runReport(t, f, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "(unlabeled)") {
		t.Errorf("the residual should be shown:\n%s", out)
	}
	if !strings.Contains(out, "%TOTAL") || !strings.Contains(out, "100.0") {
		t.Errorf("want percentages against the measured total:\n%s", out)
	}
}

// The bug this guards: the leaf test compared profile.Line by value, and
// profile.Line is a comparable struct. A recursive inline chain f -> g -> f
// has two identical Line entries in the leaf location, so both matched
// loc.Line[0] and the leaf's self time was added twice -- FLAT came out
// larger than CUM, and as the sorted column and %TOTAL numerator that put an
// inflated row at the top of the table reading 200%.
func TestFlatIsNotDoubleCountedForRecursiveInlining(t *testing.T) {
	f := &profile.Function{ID: 1, Name: "app.f"}
	g := &profile.Function{ID: 2, Name: "app.g"}
	// One location, three inlined frames: f (leaf), g, f again -- the two f
	// entries are identical structs.
	leaf := &profile.Location{ID: 1, Line: []profile.Line{
		{Function: f, Line: 10},
		{Function: g, Line: 20},
		{Function: f, Line: 10},
	}}
	p := &profile.Profile{
		SampleType: []*profile.ValueType{{Type: "samples", Unit: "count"}},
		Period:     10_000_000,
		PeriodType: &profile.ValueType{Type: "cpu", Unit: "nanoseconds"},
		Function:   []*profile.Function{f, g},
		Location:   []*profile.Location{leaf},
		Sample:     []*profile.Sample{{Location: []*profile.Location{leaf}, Value: []int64{100}}},
	}

	rows, err := topFunctions(p, time.Second, sortFlat)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.Flat > r.Cores {
			t.Errorf("%s: flat %v exceeds cum %v", r.Name, r.Flat, r.Cores)
		}
	}
	byName := map[string]Row{}
	for _, r := range rows {
		byName[r.Name] = r
	}
	// 100 samples x 10ms = 1 CPU-second over 1s = 1.0 cores, counted once.
	if math.Abs(byName["app.f"].Flat-1.0) > 1e-9 {
		t.Errorf("app.f flat = %v, want 1.0 counted once", byName["app.f"].Flat)
	}
}

// The bug this guards: a location with no debuginfo has no Line entries at
// all, so ranging over them skipped the frame and its time vanished from both
// columns. For a partially symbolized target every symbolized frame was the
// caller of such a leaf, so FLAT read 0.000 all the way down -- unusable as
// the default sort key.
func TestUnsymbolizedLeafStillCarriesSelfTime(t *testing.T) {
	caller := &profile.Function{ID: 1, Name: "app.caller"}
	locCaller := &profile.Location{ID: 1, Line: []profile.Line{{Function: caller}}}
	// Address only: no Line entries, which is what "no debuginfo" looks like.
	locLeaf := &profile.Location{ID: 2, Address: 0x4010}

	p := &profile.Profile{
		SampleType: []*profile.ValueType{{Type: "samples", Unit: "count"}},
		Period:     10_000_000,
		PeriodType: &profile.ValueType{Type: "cpu", Unit: "nanoseconds"},
		Function:   []*profile.Function{caller},
		Location:   []*profile.Location{locCaller, locLeaf},
		Sample:     []*profile.Sample{{Location: []*profile.Location{locLeaf, locCaller}, Value: []int64{100}}},
	}

	rows, err := topFunctions(p, time.Second, sortFlat)
	if err != nil {
		t.Fatal(err)
	}
	var flatTotal float64
	byName := map[string]Row{}
	for _, r := range rows {
		flatTotal += r.Flat
		byName[r.Name] = r
	}
	// All the self time is the unsymbolized leaf's, and it must not be lost:
	// self time has to sum to the profile's total or every %TOTAL reads 0.0.
	if math.Abs(flatTotal-1.0) > 1e-9 {
		t.Errorf("self time summed to %v, want 1.0", flatTotal)
	}
	if math.Abs(byName["[unsymbolized]"].Flat-1.0) > 1e-9 {
		t.Errorf("[unsymbolized] flat = %v, want 1.0", byName["[unsymbolized]"].Flat)
	}
	if math.Abs(byName["app.caller"].Cores-1.0) > 1e-9 {
		t.Errorf("the caller should still have cumulative 1.0, got %v", byName["app.caller"].Cores)
	}
}

// The default is the point of the change, so pin it. Flipping the flag back to
// "cum" should fail a test, which it previously did not.
func TestDefaultSortIsSelfTime(t *testing.T) {
	got, err := parseSortKey(defaultSortBy)
	if err != nil {
		t.Fatal(err)
	}
	if got != sortFlat {
		t.Errorf("default --sort is %q, want self time", defaultSortBy)
	}
}

// The bug this guards: one label's Values failure aborted the whole summary,
// so every label that did work was lost with it.
func TestListLabelsSurvivesOneFailingLabel(t *testing.T) {
	f := &fakeQuery{
		names: []string{"cluster", "broken", "node"},
		values: map[string][]string{
			"cluster": {"tc", "vps"},
			"node":    {"n1"},
		},
		valuesErrFor: map[string]error{"broken": errors.New("boom")},
	}

	var err error
	out := captureStdout(t, func() {
		err = listLabels(context.Background(), testClient(f, 0), "", time.Now().Add(-time.Hour), time.Now(), 4)
	})

	if err == nil {
		t.Error("a failed label query must not exit 0")
	}
	for _, want := range []string{"cluster", "node", "tc", "vps"} {
		if !strings.Contains(out, want) {
			t.Errorf("labels that worked should still be printed, missing %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "broken") || !strings.Contains(out, "!!") {
		t.Errorf("the failed label should be marked:\n%s", out)
	}
}

// Rows must stay with their labels however the queries interleave, so this
// runs with concurrency 1 against 5 slow labels: the semaphore genuinely
// queues rather than letting every goroutine straight through.
func TestListLabelsKeepsRowsWithTheirLabelsUnderContention(t *testing.T) {
	f := &fakeQuery{
		names: []string{"a", "b", "c", "d", "e"},
		values: map[string][]string{
			"a": {"av"}, "b": {"bv"}, "c": {"cv"}, "d": {"dv"}, "e": {"ev"},
		},
		valuesDelay: 2 * time.Millisecond,
	}
	out := captureStdout(t, func() {
		if err := listLabels(context.Background(), testClient(f, 0), "", time.Now().Add(-time.Hour), time.Now(), 1); err != nil {
			t.Error(err)
		}
	})
	for _, pair := range [][2]string{{"a", "av"}, {"b", "bv"}, {"c", "cv"}, {"d", "dv"}, {"e", "ev"}} {
		// Anchored per line, so a match cannot span two rows.
		re := regexp.MustCompile(`(?m)^` + pair[0] + `[ \t]+1[ \t]+` + pair[1] + `[ \t]*$`)
		if !re.MatchString(out) {
			t.Errorf("label %q should carry value %q:\n%s", pair[0], pair[1], out)
		}
	}
}

// A non-positive --concurrency must still make progress rather than deadlock
// on a zero-capacity semaphore.
func TestListLabelsToleratesZeroConcurrency(t *testing.T) {
	f := &fakeQuery{names: []string{"a", "b"}, values: map[string][]string{"a": {"av"}, "b": {"bv"}}}
	out := captureStdout(t, func() {
		if err := listLabels(context.Background(), testClient(f, 0), "", time.Now().Add(-time.Hour), time.Now(), 0); err != nil {
			t.Error(err)
		}
	})
	if !strings.Contains(out, "av") || !strings.Contains(out, "bv") {
		t.Errorf("want both labels:\n%s", out)
	}
}

// Every line step writes must be the same width. Otherwise a shorter line
// leaves the tail of a longer one behind -- "... 6/20" over "... 16/20"
// rendered as "... 6/200" -- and stop must clear the whole thing.
func TestProgressLinesAreFixedWidth(t *testing.T) {
	var buf strings.Builder
	p := newProgressTo(&buf, "merging", "labels", 20, true)
	for i := 0; i < 20; i++ {
		p.step()
	}
	var widths []int
	for _, seg := range strings.Split(buf.String(), "\r") {
		if seg == "" {
			continue
		}
		widths = append(widths, len(seg))
	}
	for i, w := range widths {
		if w != widths[0] {
			t.Fatalf("line %d is %d chars, first was %d; a shorter line leaves stale characters: %q",
				i, w, widths[0], buf.String())
		}
		if w > p.lineLen() {
			t.Fatalf("line %d is %d chars but stop clears only %d", i, w, p.lineLen())
		}
	}
}

// Concurrent steps must not interleave into a corrupt line, and the count must
// not go backwards.
func TestProgressIsSerializedUnderConcurrency(t *testing.T) {
	var buf strings.Builder
	const total = 50
	p := newProgressTo(&buf, "merging", "groups", total, true)
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); p.step() }()
	}
	wg.Wait()

	last := 0
	for _, seg := range strings.Split(buf.String(), "\r") {
		if seg == "" {
			continue
		}
		var done, tot int
		if _, err := fmt.Sscanf(seg, "merging %d groups... %d/%d", &tot, &done, &tot); err != nil {
			t.Fatalf("unparseable line %q: %v", seg, err)
		}
		if done <= last {
			t.Errorf("count went backwards: %d after %d in %q", done, last, buf.String())
		}
		last = done
	}
	if last != total {
		t.Errorf("final count %d, want %d", last, total)
	}
}

// The verb has to match the work. `labels` runs Values queries; nothing is
// merged, and saying so contradicted both the code and the README.
func TestProgressUsesTheCallersVerb(t *testing.T) {
	var buf strings.Builder
	newProgressTo(&buf, "querying", "labels", 5, true).start()
	if got := buf.String(); !strings.HasPrefix(got, "querying 5 labels") {
		t.Errorf("got %q, want the caller's verb", got)
	}
}

// Off means silent, and must not panic.
func TestProgressWritesNothingWhenOff(t *testing.T) {
	var buf strings.Builder
	p := newProgressTo(&buf, "merging", "groups", 10, false)
	p.start()
	p.step()
	p.stop()
	if buf.String() != "" {
		t.Errorf("want no output when off, got %q", buf.String())
	}
}

// A job of one unit is not a fan-out worth narrating.
func TestProgressIsOffForTrivialJobs(t *testing.T) {
	if newProgress("merging", "x", 1).on {
		t.Error("progress should be off for a single unit")
	}
	if newProgress("merging", "x", 0).on {
		t.Error("progress should be off for an empty job")
	}
}

// The bug this guards: the labels path reused the merge advice, telling the
// reader to narrow --from or use --match after a failed label lookup. Neither
// applies -- listLabels passes no matchers at all and merges nothing.
func TestLabelFailuresGetMetadataAdviceNotMergeAdvice(t *testing.T) {
	f := &fakeQuery{
		names:  []string{"cluster", "slow"},
		values: map[string][]string{"cluster": {"tc"}},
		valuesErrFor: map[string]error{
			"slow": status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
		},
	}
	var err error
	out := captureStdout(t, func() {
		err = listLabels(context.Background(), testClient(f, 0), "", time.Now().Add(-time.Hour), time.Now(), 4)
	})
	if err == nil {
		t.Fatal("want a non-zero exit")
	}
	// The advice that does apply.
	if !strings.Contains(out, "--timeout") {
		t.Errorf("want the timeout advice:\n%s", out)
	}
	// The advice that does not.
	for _, forbidden := range []string{"--match", "merge"} {
		if strings.Contains(out, forbidden) {
			t.Errorf("labels advice must not mention %q:\n%s", forbidden, out)
		}
	}
	// And it belongs above the next-step line, not below it.
	hint := strings.Index(out, "--timeout")
	next := strings.Index(out, "Run `parcareport labels")
	if hint > next {
		t.Errorf("the failure explanation should precede the next-step line:\n%s", out)
	}
}

// A merge failure keeps the merge advice, which is the half that can be acted
// on by changing the query.
func TestMergeFailuresKeepMergeAdvice(t *testing.T) {
	h := hintFor([]string{"stream terminated by RST_STREAM"}, mergeQuery)
	if !strings.Contains(h, "--match") {
		t.Errorf("want merge advice, got %q", h)
	}
	if m := hintFor([]string{"stream terminated by RST_STREAM"}, metadataQuery); strings.Contains(m, "--match") {
		t.Errorf("metadata advice must not suggest --match, got %q", m)
	}
}

func TestAuthHeader(t *testing.T) {
	if got := (Auth{}).header(); got != "" {
		t.Errorf("no credentials should mean no header, got %q", got)
	}
	if got := (Auth{BearerToken: "tok"}).header(); got != "Bearer tok" {
		t.Errorf("got %q", got)
	}
	// "user:pw" base64-encoded.
	if got := (Auth{Username: "user", Password: "pw"}).header(); got != "Basic dXNlcjpwdw==" {
		t.Errorf("got %q", got)
	}
}

// Credentials must never travel in cleartext. gRPC would refuse this too, but
// its error does not say that the flags contradict each other.
func TestDialRefusesCredentialsOverPlaintext(t *testing.T) {
	_, err := Dial("localhost:7070", true, time.Minute, Auth{BearerToken: "tok"})
	if err == nil {
		t.Fatal("want a refusal")
	}
	if !strings.Contains(err.Error(), "plaintext") {
		t.Errorf("the error should name the problem, got %v", err)
	}
}

// grpc.NewClient connects lazily, so these two only prove the option set was
// accepted -- no handshake, no DNS, no certificate verification happens here.
// That is worth pinning anyway, since --insecure=false used to be a hard
// error, but it is not evidence that the TLS config is right.
func TestDialAcceptsBothTransports(t *testing.T) {
	c, err := Dial("localhost:7070", true, time.Minute, Auth{})
	if err != nil {
		t.Fatalf("plaintext, the port-forward case: %v", err)
	}
	c.Close()

	c, err = Dial("parca.example.com:443", false, time.Minute, Auth{BearerToken: "tok"})
	if err != nil {
		t.Fatalf(`TLS is no longer "not implemented": %v`, err)
	}
	c.Close()
}

// This is the property the design rests on, so assert the enforcement rather
// than the one-line method that requests it: gRPC refuses per-RPC credentials
// over an insecure transport, and it refuses eagerly, at construction.
func TestGRPCRefusesOurCredentialsOverAnInsecureTransport(t *testing.T) {
	_, err := grpc.NewClient("localhost:7070",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(authCreds{value: "Bearer tok"}),
	)
	if err == nil {
		t.Fatal("gRPC must refuse credentials over an insecure transport")
	}
	if !strings.Contains(err.Error(), "transport level security") {
		t.Errorf("unexpected refusal: %v", err)
	}
}

func TestAuthCredsCarryTheHeader(t *testing.T) {
	md, err := authCreds{value: "Bearer tok"}.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if md["authorization"] != "Bearer tok" {
		t.Errorf("got %v", md)
	}
}

func TestAuthFromFlags(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	// Token files almost always end in a newline, and a token carrying one
	// fails as an opaque 401.
	if err := os.WriteFile(path, []byte("  tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := options{tokenFile: path}.auth()
	if err != nil {
		t.Fatal(err)
	}
	if got.BearerToken != "tok" {
		t.Errorf("token = %q, want it trimmed", got.BearerToken)
	}

	// The file wins over the flag, because the flag is visible in the process
	// list to anyone on the box.
	got, err = options{tokenFile: path, bearerToken: "fromflag"}.auth()
	if err != nil {
		t.Fatal(err)
	}
	if got.BearerToken != "tok" {
		t.Errorf("the file should win, got %q", got.BearerToken)
	}

	if _, err := (options{tokenFile: filepath.Join(dir, "nope")}).auth(); err == nil {
		t.Error("a missing token file must be an error")
	}

	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (options{tokenFile: empty}).auth(); err == nil {
		t.Error("an empty token file must be an error, not a silent no-auth")
	}

	if _, err := (options{bearerToken: "tok", username: "u"}).auth(); err == nil {
		t.Error("two kinds of credentials at once must be rejected")
	}
}

// A password with no username produces no header at all, so the request would
// go out unauthenticated and come back as a bare 401 saying nothing about the
// flag having been ignored.
func TestAuthRejectsAPasswordWithoutAUsername(t *testing.T) {
	if _, err := (options{password: "pw"}).auth(); err == nil {
		t.Error("a password alone is not a credential")
	}
	// And it really would have produced nothing.
	if got := (Auth{Password: "pw"}).header(); got != "" {
		t.Errorf("expected no header, got %q", got)
	}
}

// RFC 7617 gives the colon to the first separator, so a username containing
// one shifts the split silently.
func TestAuthRejectsAColonInTheUsername(t *testing.T) {
	if _, err := (options{username: "a:b", password: "pw"}).auth(); err == nil {
		t.Error("want a rejection")
	}
}

// Basic auth needs a file form too, or the only way to use it is the process
// list -- the exact channel this change exists to avoid.
func TestAuthReadsThePasswordFromAFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pw")
	if err := os.WriteFile(path, []byte("hunter2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := options{username: "svc", passwordFile: path}.auth()
	if err != nil {
		t.Fatal(err)
	}
	if got.Password != "hunter2" {
		t.Errorf("password = %q, want it trimmed", got.Password)
	}
	if got.header() != "Basic c3ZjOmh1bnRlcjI=" {
		t.Errorf("header = %q", got.header())
	}
}

// A file holding two lines, or a comment, is not one credential. An
// Authorization header carrying a newline is rejected far from the flag that
// caused it.
func TestSecretFileRejectsInteriorWhitespace(t *testing.T) {
	dir := t.TempDir()
	for name, content := range map[string]string{
		"twolines": "tok\nother\n",
		"comment":  "# my token\ntok\n",
		"spaced":   "to ken\n",
	} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readSecretFile("--bearer-token-file", path); err == nil {
			t.Errorf("%s: want a rejection for %q", name, content)
		}
	}
}
