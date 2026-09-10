package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
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
	"google.golang.org/protobuf/types/known/timestamppb"
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
	typeCalls   atomic.Int64
	// mergeWindows records the window every merge asked for, so a test can
	// check that the rows and the total describe the same moment.
	mergeWindows [][2]time.Time
	// extraScrape gives that many series one additional scrape a few seconds
	// after their newest, the way a restart or a backfill does. Their median
	// gap is unchanged, so the window still spans an interval -- and holds
	// both of those samples.
	extraScrape int
	// laggingSeries makes that many of the series stop scraping a couple of
	// intervals before the rest, the way a target that went away does.
	laggingSeries int
	// multiSeries makes one selector look like N scrape targets, each written
	// at its own instant -- the shape that broke the single-profile approach.
	multiSeries int
	// merges answers MergePprof by selector. A selector with no entry gets an
	// empty pprof, which is what Parca returns for a window with no samples.
	merges    map[string]*profile.Profile
	mergeErrs map[string]error
	// maxParallel records the peak number of merges in flight, so a test can
	// see what concurrency was actually used.
	inFlight    atomic.Int64
	maxParallel atomic.Int64
	mergeDelay  time.Duration
	// failFirstN fails that many merge attempts with a transient-looking
	// error before succeeding, the way an overloaded server does.
	failFirstN int64
	failCount  atomic.Int64
	// failSel fails every attempt for one selector, so a test can see what a
	// server that never recovers does to the retry.
	failSel string
	// valuesFailFirst drops that many Values calls per label before
	// answering, guarded by mergeMu.
	valuesFailFirst map[string]int
	// peakAfter starts recording maxParallel only once this many merges have
	// been made, so a test can measure the retry pass on its own rather than
	// the peak of the first fan-out that preceded it.
	peakAfter int64
	// mergeCalls counts every merge by selector, including the ones that
	// fail. failCount only counts the failFirstN path, so it cannot answer
	// "how many times was this group asked".
	mergeMu    sync.Mutex
	mergeCalls map[string]int
	block      time.Duration // make Values hang, to exercise the deadline
}

func (f *fakeQuery) ProfileTypes(ctx context.Context, _ *qv1.ProfileTypesRequest, _ ...grpc.CallOption) (*qv1.ProfileTypesResponse, error) {
	f.typeCalls.Add(1)
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
	// A label whose first lookups drop the stream: the shape of a server
	// briefly out of room, not one that is broken. Checked before
	// valuesErrFor, so a test can make the first attempt transient and the
	// second fail some other way.
	f.mergeMu.Lock()
	dropped := f.valuesFailFirst[in.GetLabelName()] > 0
	if dropped {
		f.valuesFailFirst[in.GetLabelName()]--
	}
	f.mergeMu.Unlock()
	if dropped {
		return nil, status.Error(codes.Internal,
			"stream terminated by RST_STREAM with error code: INTERNAL_ERROR")
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
	err := listLabels(context.Background(), c, "cluster", time.Now().Add(-time.Hour), time.Now(), 4, time.Minute)
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

	out := captureStdout(t, func() { printFailures(failed, mergeQuery, false) })

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
	if h := hintFor([]string{"no such label"}, mergeQuery, false); h != "" {
		t.Errorf("want no hint, got %q", h)
	}
	if h := hintFor([]string{"context deadline exceeded"}, mergeQuery, false); !strings.Contains(h, "--timeout") {
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
	// A merge and a single-profile fetch name their selector in different
	// places, and they fold together different things:
	//
	//   MODE_SINGLE at t -> the ONE series written at t. Not a group total.
	//   MODE_MERGE       -> every series x every profile in the window.
	//
	// A fake that scaled both the same way made the tests unable to tell the
	// approaches apart, which is how a vacuous test slipped through twice.
	sel := in.GetSingle().GetQuery()
	factor := 1
	if m := in.GetMerge(); m.GetQuery() != "" {
		sel = m.GetQuery()
		// Count the profiles that actually fall inside the requested window,
		// per series. Multiplying a per-series count by the number of series
		// assumed every series is always in the window, so a window that
		// excluded one could not be detected.
		factor = 0
		for s := 0; s < f.seriesCount(); s++ {
			factor += len(f.scrapeTimes(s, m.GetStart().AsTime(), m.GetEnd().AsTime()))
		}
		f.mergeMu.Lock()
		f.mergeWindows = append(f.mergeWindows, [2]time.Time{m.GetStart().AsTime(), m.GetEnd().AsTime()})
		f.mergeMu.Unlock()
	}
	f.mergeMu.Lock()
	if f.mergeCalls == nil {
		f.mergeCalls = map[string]int{}
	}
	f.mergeCalls[sel]++
	f.mergeMu.Unlock()
	if err := f.mergeErrs[sel]; err != nil {
		return nil, err
	}
	if f.failSel != "" && f.failSel == sel {
		return nil, status.Error(codes.Internal,
			"stream terminated by RST_STREAM with error code: INTERNAL_ERROR")
	}
	if f.failFirstN > 0 && f.failCount.Add(1) <= f.failFirstN {
		return nil, status.Error(codes.Internal,
			"stream terminated by RST_STREAM with error code: INTERNAL_ERROR")
	}
	n := f.inFlight.Add(1)
	f.mergeMu.Lock()
	total := int64(0)
	for _, c := range f.mergeCalls {
		total += int64(c)
	}
	f.mergeMu.Unlock()
	if total > f.peakAfter {
		for {
			peak := f.maxParallel.Load()
			if n <= peak || f.maxParallel.CompareAndSwap(peak, n) {
				break
			}
		}
	}
	defer f.inFlight.Add(-1)
	if f.mergeDelay > 0 {
		time.Sleep(f.mergeDelay)
	}
	p := f.merges[sel]
	if p == nil {
		return &qv1.QueryResponse{}, nil // no samples in this window
	}
	if factor == 0 {
		return &qv1.QueryResponse{}, nil // window holds no profile for this series
	}
	if factor > 1 {
		p = scaleProfile(p, int64(factor))
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
		err = listLabels(context.Background(), testClient(f, 0), "", time.Now().Add(-time.Hour), time.Now(), 4, time.Minute)
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
		if err := listLabels(context.Background(), testClient(f, 0), "", time.Now().Add(-time.Hour), time.Now(), 1, time.Minute); err != nil {
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

// The eleven types a real Parca offered, in the order it returned them.
var realServerTypes = []string{
	"goroutine:goroutine:count:goroutine:count",
	"parca_agent:samples:count:cpu:nanoseconds:delta",
	"parca_agent:wallclock:nanoseconds:samples:count:delta",
	"mutex:contentions:count:contentions:count",
	"mutex:delay:nanoseconds:contentions:count",
	"block:contentions:count:contentions:count",
	"block:delay:nanoseconds:contentions:count",
	"memory:alloc_objects:count:space:bytes",
	"memory:alloc_space:bytes:space:bytes",
	"memory:inuse_objects:count:space:bytes",
	"memory:inuse_space:bytes:space:bytes",
}

func TestMatchProfileType(t *testing.T) {
	// An exact selector always wins, even though it is also a substring of
	// itself and of nothing else.
	const full = "parca_agent:samples:count:cpu:nanoseconds:delta"
	if got, err := matchProfileType(full, realServerTypes); err != nil || got != full {
		t.Errorf("exact match: got (%q, %v)", got, err)
	}

	// The whole point: "cpu" is what people mean, and it is unambiguous even
	// though a wallclock type also mentions "samples" and "nanoseconds".
	if got, err := matchProfileType("cpu", realServerTypes); err != nil || got != full {
		t.Errorf(`matchProfileType("cpu"): got (%q, %v), want %q`, got, err, full)
	}

	if got, err := matchProfileType("inuse_space", realServerTypes); err != nil ||
		got != "memory:inuse_space:bytes:space:bytes" {
		t.Errorf("got (%q, %v)", got, err)
	}
}

// An ambiguous abbreviation must never be guessed: picking inuse_space over
// alloc_space answers a different question than the one asked.
func TestMatchProfileTypeRefusesToGuess(t *testing.T) {
	_, err := matchProfileType("memory", realServerTypes)
	if err == nil {
		t.Fatal("want an error for an ambiguous abbreviation")
	}
	// The candidates have to be listed, or the error is a dead end.
	for _, want := range []string{"alloc_space", "inuse_space", "matches 4"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q: %v", want, err)
		}
	}
}

// A non-positive --concurrency must still make progress rather than deadlock
// on a zero-capacity semaphore.
func TestListLabelsToleratesZeroConcurrency(t *testing.T) {
	f := &fakeQuery{names: []string{"a", "b"}, values: map[string][]string{"a": {"av"}, "b": {"bv"}}}
	out := captureStdout(t, func() {
		if err := listLabels(context.Background(), testClient(f, 0), "", time.Now().Add(-time.Hour), time.Now(), 0, time.Minute); err != nil {
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
		err = listLabels(context.Background(), testClient(f, 0), "", time.Now().Add(-time.Hour), time.Now(), 4, time.Minute)
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
	h := hintFor([]string{"stream terminated by RST_STREAM"}, mergeQuery, false)
	if !strings.Contains(h, "--match") {
		t.Errorf("want merge advice, got %q", h)
	}
	if m := hintFor([]string{"stream terminated by RST_STREAM"}, metadataQuery, false); strings.Contains(m, "--match") {
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

func TestMatchProfileTypeRejectsNoMatch(t *testing.T) {
	_, err := matchProfileType("sideways", realServerTypes)
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "goroutine:goroutine") {
		t.Errorf("the available types should be listed: %v", err)
	}
}

// The bug this guards: with several types and none asked for, the tool
// refused. A real server offers eleven, so auto-detect never applied and every
// invocation had to paste the full six-part selector.
func TestAutoDetectPrefersTheCPUProfile(t *testing.T) {
	c := testClient(&fakeQuery{types: profileTypesFrom(realServerTypes)}, 0)
	got, verified, err := resolveProfileType(context.Background(), c, "")
	if err != nil {
		t.Fatalf("want a CPU default rather than a refusal: %v", err)
	}
	if got != "parca_agent:samples:count:cpu:nanoseconds:delta" {
		t.Errorf("got %q", got)
	}
	if !verified {
		t.Error("a type read from the server's own list is verified")
	}
}

// Off-CPU time is not what CORES means, so a wallclock profile must not be
// mistaken for the CPU default even though its name contains "samples".
func TestAutoDetectIgnoresWallclock(t *testing.T) {
	only := []string{"parca_agent:wallclock:nanoseconds:samples:count:delta"}
	if got := cpuDeltaTypes(only); len(got) != 0 {
		t.Errorf("wallclock is not a CPU profile, got %v", got)
	}
	// A non-delta cpu profile is not a rate either.
	if got := cpuDeltaTypes([]string{"x:samples:count:cpu:nanoseconds"}); len(got) != 0 {
		t.Errorf("a non-delta profile is not a CPU default, got %v", got)
	}
}

// With no CPU profile at all there is nothing to default to, so refusing is
// still right -- and the message must say why.
func TestAutoDetectStillRefusesWithoutACPUProfile(t *testing.T) {
	noCPU := []string{
		"memory:alloc_space:bytes:space:bytes",
		"memory:inuse_space:bytes:space:bytes",
	}
	c := testClient(&fakeQuery{types: profileTypesFrom(noCPU)}, 0)
	_, _, err := resolveProfileType(context.Background(), c, "")
	if err == nil {
		t.Fatal("want a refusal when there is no CPU profile to default to")
	}
	if !strings.Contains(err.Error(), "--profile-type") {
		t.Errorf("the error should say how to choose: %v", err)
	}
}

// A single type still wins outright, CPU or not.
func TestAutoDetectTakesTheOnlyType(t *testing.T) {
	one := []string{"memory:inuse_space:bytes:space:bytes"}
	c := testClient(&fakeQuery{types: profileTypesFrom(one)}, 0)
	got, _, err := resolveProfileType(context.Background(), c, "")
	if err != nil || got != one[0] {
		t.Errorf("got (%q, %v)", got, err)
	}
}

// profileTypesFrom turns selector strings back into the protobuf the server
// sends, so tests can be written in the form a user actually sees.
func profileTypesFrom(selectors []string) []*qv1.ProfileType {
	out := make([]*qv1.ProfileType, 0, len(selectors))
	for _, s := range selectors {
		p := strings.Split(s, ":")
		t := &qv1.ProfileType{
			Name: p[0], SampleType: p[1], SampleUnit: p[2],
			PeriodType: p[3], PeriodUnit: p[4],
		}
		if len(p) == 6 && p[5] == "delta" {
			t.Delta = true
		}
		out = append(out, t)
	}
	return out
}

// Two agents writing CPU profiles under different names must not be guessed
// between: picking one would report a fraction of the fleet as though it were
// all of it. The error lists exactly the candidates, not every type.
func TestAutoDetectRefusesBetweenTwoCPUProfiles(t *testing.T) {
	two := []string{
		"parca_agent:samples:count:cpu:nanoseconds:delta",
		"otheragent:samples:count:cpu:nanoseconds:delta",
		"memory:inuse_space:bytes:space:bytes",
	}
	c := testClient(&fakeQuery{types: profileTypesFrom(two)}, 0)
	_, _, err := resolveProfileType(context.Background(), c, "")
	if err == nil {
		t.Fatal("want a refusal")
	}
	for _, want := range []string{"2 CPU profiles", "otheragent", "parca_agent"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q: %v", want, err)
		}
	}
	// The heap profile is not a candidate and should not be offered as one.
	if strings.Contains(err.Error(), "inuse_space") {
		t.Errorf("only CPU candidates should be listed: %v", err)
	}
}

// The field order the CPU default depends on. If index 3 were not the PERIOD
// type, the tool would silently default to the wrong profile and every CORES
// number would be meaningless.
func TestCPUDefaultMatchesOnThePeriodType(t *testing.T) {
	// client.go builds name:sampleType:sampleUnit:periodType:periodUnit[:delta].
	if got := cpuDeltaTypes([]string{"parca_agent:samples:count:cpu:nanoseconds:delta"}); len(got) != 1 {
		t.Errorf("the real CPU selector must be recognised, got %v", got)
	}
	// "cpu" in the sample-type position must not count.
	if got := cpuDeltaTypes([]string{"x:cpu:nanoseconds:wall:count:delta"}); len(got) != 0 {
		t.Errorf("index 3 must be the period type, got %v", got)
	}
}

// An empty selector is a substring of everything, so it must not resolve to a
// profile even though nothing routes it here today.
func TestMatchProfileTypeRefusesTheEmptySelector(t *testing.T) {
	if _, err := matchProfileType("", realServerTypes); err == nil {
		t.Error("an empty selector matches every type; it must not resolve")
	}
}

func TestLooksLikeSelector(t *testing.T) {
	full := []string{
		"parca_agent:samples:count:cpu:nanoseconds:delta",
		"memory:inuse_space:bytes:space:bytes",
		"goroutine:goroutine:count:goroutine:count",
	}
	for _, s := range full {
		if !looksLikeSelector(s) {
			t.Errorf("%q is a complete selector", s)
		}
	}
	partial := []string{
		"cpu", "memory", "inuse_space", "",
		"a:b:c:d",                   // too few
		"a:b:c:d:e:f:g",             // too many
		"a:b:c:d:e:notdelta",        // a sixth field can only be delta
		"parca_agent::count:cpu:ns", // an empty field is not a name
	}
	for _, s := range partial {
		if looksLikeSelector(s) {
			t.Errorf("%q is not a complete selector", s)
		}
	}
}

// The bug this guards, seen against a real server: with the type lookup
// failing, an abbreviation was passed through as though it were a selector.
// Parca rejects it outright, so every merge failed -- after minutes of
// waiting, and with an error about selector syntax that pointed nowhere near
// the lookup that had actually gone wrong.
func TestAbbreviationIsNotUsedWhenTheLookupFails(t *testing.T) {
	c := testClient(&fakeQuery{typesErr: errors.New("stream terminated by RST_STREAM")}, 0)
	_, _, err := resolveProfileType(context.Background(), c, "cpu")
	if err == nil {
		t.Fatal("an abbreviation cannot be expanded without the type list")
	}
	for _, want := range []string{"abbreviation", "RST_STREAM", "full selector"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error should mention %q: %v", want, err)
		}
	}
}

// A full selector still survives a failed lookup: it needs nothing from the
// server, which is the whole point of not letting that lookup fail a run.
func TestFullSelectorStillSurvivesAFailedLookup(t *testing.T) {
	const full = "parca_agent:samples:count:cpu:nanoseconds:delta"
	c := testClient(&fakeQuery{typesErr: errors.New("boom")}, 0)
	got, verified, err := resolveProfileType(context.Background(), c, full)
	if err != nil {
		t.Fatalf("a complete selector should be taken on trust: %v", err)
	}
	if got != full || verified {
		t.Errorf("got (%q, %v)", got, verified)
	}
}

func jsonOptions() options {
	o := testOptions()
	o.output = outputJSON
	o.top = 5
	return o
}

func decodeReport(t *testing.T, out string) reportData {
	t.Helper()
	var d reportData
	if err := json.Unmarshal([]byte(out), &d); err != nil {
		t.Fatalf("output is not one JSON document: %v\n%s", err, out)
	}
	return d
}

func TestJSONReportCarriesTheNumbersAndTheirUnit(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 200)

	var err error
	out := captureStdout(t, func() {
		err = report(context.Background(), testClient(f, time.Minute), jsonOptions(),
			time.Now().Add(-time.Hour), time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}
	d := decodeReport(t, out)

	if !d.Complete {
		t.Error("a run where nothing failed is complete")
	}
	if d.Unit != "cores" || !d.Rate {
		t.Errorf("unit = %q rate = %v, want cores as a rate", d.Unit, d.Rate)
	}
	if d.Total == nil {
		t.Fatal("a measured total must be present")
	}
	// tc 100, vps 50, unlabeled 50 of an overall 200.
	var unlabeled *groupJSON
	for i := range d.Groups {
		if d.Groups[i].Unlabeled {
			unlabeled = &d.Groups[i]
		}
		if d.Groups[i].Pct == nil {
			t.Errorf("group %q has no percentage though the total is known", d.Groups[i].Name)
		}
	}
	if unlabeled == nil {
		t.Error("the residual should be marked as such, not just named")
	}
	if d.ProfileType != testType || d.TypeVerified == nil || !*d.TypeVerified {
		t.Errorf("profile type = %q verified = %v", d.ProfileType, d.TypeVerified)
	}
	if d.SortedBy != "flat" {
		t.Errorf("functions_sorted_by = %q", d.SortedBy)
	}
}

// The whole point of the format: a consumer must not have to grep stdout for
// `!!` to notice the numbers are wrong.
func TestJSONMarksAnIncompleteRunAndNamesTheFailures(t *testing.T) {
	f := reportFixture(t)
	f.values["cluster"] = []string{"tc", "vps", "broken"}
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 200)
	f.mergeErrs[testType+`{cluster="broken"}`] = errors.New("boom")

	var err error
	out := captureStdout(t, func() {
		err = report(context.Background(), testClient(f, time.Minute), jsonOptions(),
			time.Now().Add(-time.Hour), time.Now())
	})
	if err == nil {
		t.Error("an incomplete run must still exit non-zero")
	}
	d := decodeReport(t, out)
	if d.Complete {
		t.Error("complete must be false when a query failed")
	}
	if len(d.Failed) != 1 || d.Failed[0].Group != "cluster=broken" {
		t.Errorf("the failure should be named as data: %+v", d.Failed)
	}
	if d.Error == "" {
		t.Error("the shortfall should be stated")
	}
	// The groups that worked are still reported.
	if len(d.Groups) < 2 {
		t.Errorf("surviving groups should still be present: %+v", d.Groups)
	}
}

// Without the unfiltered merge there is no denominator, and null is the honest
// answer -- not the sum of the labelled groups, which omits whatever is
// missing.
func TestJSONLeavesTheTotalNullWhenItIsUnknown(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.mergeErrs[testType] = errors.New("boom")

	out := captureStdout(t, func() {
		_ = report(context.Background(), testClient(f, time.Minute), jsonOptions(),
			time.Now().Add(-time.Hour), time.Now())
	})
	d := decodeReport(t, out)
	if d.Total != nil {
		t.Errorf("total = %v, want null", *d.Total)
	}
	for _, g := range d.Groups {
		if g.Pct != nil {
			t.Errorf("group %q has a percentage with no denominator", g.Name)
		}
	}
	if d.Complete {
		t.Error("complete must be false")
	}
}

// A run that produced nothing still emits a document. Printing only prose
// would make "the window was empty" and "the command broke" indistinguishable
// to a script -- the conflation the tool refuses everywhere else.
func TestJSONEmitsADocumentEvenWhenThereIsNothingToReport(t *testing.T) {
	f := reportFixture(t) // no merges configured: every group comes back empty

	var err error
	out := captureStdout(t, func() {
		err = report(context.Background(), testClient(f, time.Minute), jsonOptions(),
			time.Now().Add(-time.Hour), time.Now())
	})
	if err == nil {
		t.Error("want a non-zero exit")
	}
	d := decodeReport(t, out)
	if d.Complete {
		t.Error("complete must be false")
	}
	if d.Error == "" {
		t.Error("the reason should be in the document")
	}
}

// The human banners must never land beside the document.
func TestJSONOutputIsNotPollutedByBanners(t *testing.T) {
	f := reportFixture(t)
	f.values["cluster"] = []string{"a", "b"}
	f.mergeErrs[testType+`{cluster="a"}`] = errors.New("boom")
	f.mergeErrs[testType+`{cluster="b"}`] = errors.New("boom")

	out := captureStdout(t, func() {
		_ = report(context.Background(), testClient(f, time.Minute), jsonOptions(),
			time.Now().Add(-time.Hour), time.Now())
	})
	if strings.Contains(out, "!!") {
		t.Errorf("banners must not accompany JSON:\n%s", out)
	}
	d := decodeReport(t, out)
	if len(d.Failed) != 2 {
		t.Errorf("both failures should be data: %+v", d.Failed)
	}
}

// Function names are the reason the table cannot be parsed: it cuts them to 60
// characters, which made two different frames render identically.
func TestJSONDoesNotTruncateFunctionNames(t *testing.T) {
	long := "github.com/parquet-go/parquet-go/encoding/thrift.(*structDecoder).decodeSomethingVeryLongIndeed"
	if len(long) <= 60 {
		t.Fatal("the fixture name must be longer than the table's cutoff")
	}
	fn := &profile.Function{ID: 1, Name: long}
	loc := &profile.Location{ID: 1, Line: []profile.Line{{Function: fn}}}
	p := &profile.Profile{
		SampleType: []*profile.ValueType{{Type: "samples", Unit: "count"}},
		Period:     10_000_000,
		PeriodType: &profile.ValueType{Type: "cpu", Unit: "nanoseconds"},
		Function:   []*profile.Function{fn},
		Location:   []*profile.Location{loc},
		Sample:     []*profile.Sample{{Location: []*profile.Location{loc}, Value: []int64{100}}},
	}
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = p
	f.merges[testType] = p

	out := captureStdout(t, func() {
		if err := report(context.Background(), testClient(f, time.Minute), jsonOptions(),
			time.Now().Add(-time.Hour), time.Now()); err != nil {
			t.Fatal(err)
		}
	})
	d := decodeReport(t, out)
	if len(d.Functions) == 0 {
		t.Fatal("want a function")
	}
	if d.Functions[0].Name != long {
		t.Errorf("name was altered:\n got %q\nwant %q", d.Functions[0].Name, long)
	}
}

func TestParseOutput(t *testing.T) {
	for _, in := range []string{"", "table"} {
		if got, err := parseOutput(in); err != nil || got != outputTable {
			t.Errorf("parseOutput(%q) = (%q, %v)", in, got, err)
		}
	}
	if got, err := parseOutput("json"); err != nil || got != outputJSON {
		t.Errorf("got (%q, %v)", got, err)
	}
	if _, err := parseOutput("yaml"); err == nil {
		t.Error("want an error for an unsupported format")
	}
}

// The table is unchanged by the refactor that made JSON possible.
func TestTableOutputStillLooksTheSame(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 150)
	o := testOptions()
	o.top = 3

	out := captureStdout(t, func() {
		if err := report(context.Background(), testClient(f, time.Minute), o,
			time.Now().Add(-time.Hour), time.Now()); err != nil {
			t.Fatal(err)
		}
	})
	for _, want := range []string{testType, "CLUSTER", "CORES", "%TOTAL", "TOTAL", "tc", "vps", "FUNCTION"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q from the table:\n%s", want, out)
		}
	}
}

// The bug this guards: the fallback row is labelled SUM OF LISTED, so it has
// to be the sum of the listed rows. The refactor passed the total instead,
// which is null in exactly the case that row prints, so it always read 0.000
// -- correct rows above an obviously wrong subtotal.
func TestSumOfListedShowsTheActualSum(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.mergeErrs[testType] = errors.New("boom")

	// A ten-second window, so the value is visible at three decimals rather
	// than rounding to 0.000 and making the assertion vacuous.
	end := time.Now()
	out := captureStdout(t, func() {
		_ = report(context.Background(), testClient(f, time.Minute), testOptions(),
			end.Add(-10*time.Second), end)
	})
	// (100 + 50) samples x 10ms = 1.5 CPU-seconds over 10s.
	want := 150 * 0.01 / 10
	line := ""
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "SUM OF LISTED") {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("no subtotal row:\n%s", out)
	}
	if strings.Contains(line, "0.000") {
		t.Errorf("the subtotal must be the sum of the rows, not zero: %q", line)
	}
	if !strings.Contains(line, fmt.Sprintf("%.3f", want)) {
		t.Errorf("subtotal = %q, want %.3f", line, want)
	}
}

// The bug this guards: gathering before rendering moved the heading behind an
// early return, so an idle window produced a completely empty stdout -- no
// record of what was even asked.
func TestTheHeadingPrintsEvenWithNothingToTabulate(t *testing.T) {
	t.Run("empty window", func(t *testing.T) {
		f := reportFixture(t)
		out := captureStdout(t, func() {
			_ = report(context.Background(), testClient(f, time.Minute), testOptions(),
				time.Now().Add(-time.Hour), time.Now())
		})
		if !strings.Contains(out, testType) {
			t.Errorf("stdout should still say what was queried:\n%q", out)
		}
	})
	t.Run("every query failed", func(t *testing.T) {
		f := reportFixture(t)
		f.mergeErrs[testType+`{cluster="tc"}`] = errors.New("boom")
		f.mergeErrs[testType+`{cluster="vps"}`] = errors.New("boom")
		out := captureStdout(t, func() {
			_ = report(context.Background(), testClient(f, time.Minute), testOptions(),
				time.Now().Add(-time.Hour), time.Now())
		})
		if !strings.Contains(out, testType) {
			t.Errorf("stdout should still say what was queried:\n%q", out)
		}
		if !strings.Contains(out, "!! FAILED") {
			t.Errorf("and still carry the banner:\n%q", out)
		}
	})
}

// unit and rate describe the same numbers, so they must not contradict: a CPU
// report that says "cores" cannot also say it is not a rate.
func TestUnitAndRateAgreeWithoutTheUnfilteredMerge(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.mergeErrs[testType] = errors.New("boom")

	out := captureStdout(t, func() {
		_ = report(context.Background(), testClient(f, time.Minute), jsonOptions(),
			time.Now().Add(-time.Hour), time.Now())
	})
	d := decodeReport(t, out)
	if d.Unit != "cores" {
		t.Errorf("unit = %q", d.Unit)
	}
	if !d.Rate {
		t.Error("cores is a rate; unit and rate must not contradict")
	}
}

// A document for a run that produced nothing should still say what it asked
// about, and must not assert a verification that never happened.
func TestJSONErrorSkeletonDoesNotAssertWhatItDoesNotKnow(t *testing.T) {
	// An unknown --by: gatherReport returns nil before anything is measured.
	f := reportFixture(t)
	o := jsonOptions()
	o.by = "nosuchlabel"
	start := time.Now().Add(-time.Hour)
	end := time.Now()

	out := captureStdout(t, func() {
		_ = report(context.Background(), testClient(f, time.Minute), o, start, end)
	})
	d := decodeReport(t, out)
	if d.TypeVerified != nil {
		t.Errorf("verification never happened, so it must be null, got %v", *d.TypeVerified)
	}
	if d.Start.IsZero() || d.End.IsZero() {
		t.Errorf("the window was known and should be reported: %v .. %v", d.Start, d.End)
	}
	if d.WindowSecs == 0 {
		t.Error("window_seconds should be set")
	}
	if d.Error == "" {
		t.Error("the reason should be stated")
	}
}

// window_seconds should be a clean number, not float noise from time.Now().
func TestWindowSecondsIsRounded(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType] = cpuProfile(t, 100)
	start := time.Now().Add(-time.Hour)

	out := captureStdout(t, func() {
		_ = report(context.Background(), testClient(f, time.Minute), jsonOptions(), start, time.Now())
	})
	if strings.Contains(out, "3600.0000") {
		t.Errorf("window_seconds carries float noise:\n%s", out)
	}
}

const heapType = "memory:inuse_space:bytes:space:bytes"

func heapProfile(t *testing.T, bytes int64) *profile.Profile {
	t.Helper()
	fn := &profile.Function{ID: 1, Name: "app.alloc"}
	loc := &profile.Location{ID: 1, Line: []profile.Line{{Function: fn}}}
	return &profile.Profile{
		SampleType: []*profile.ValueType{{Type: "inuse_space", Unit: "bytes"}},
		PeriodType: &profile.ValueType{Type: "space", Unit: "bytes"},
		Function:   []*profile.Function{fn},
		Location:   []*profile.Location{loc},
		Sample:     []*profile.Sample{{Location: []*profile.Location{loc}, Value: []int64{bytes}}},
	}
}

func overviewFixture(t *testing.T) *fakeQuery {
	t.Helper()
	return &fakeQuery{
		types: profileTypesFrom([]string{testType, heapType,
			"parca_agent:wallclock:nanoseconds:samples:count:delta"}),
		names: []string{"cluster", "comm", "instance"},
		values: map[string][]string{
			"cluster":  {"tc", "vps"},
			"comm":     {"parca"},
			"instance": {"10.0.0.1:6060"},
		},
		merges:    map[string]*profile.Profile{},
		mergeErrs: map[string]error{},
	}
}

func runOverview(t *testing.T, f *fakeQuery, o options) (string, error) {
	t.Helper()
	var err error
	out := captureStdout(t, func() {
		end := time.Now()
		err = overview(context.Background(), testClient(f, time.Minute), o, end.Add(-10*time.Second), end)
	})
	return out, err
}

// The point of the command: one run answers the questions you would otherwise
// have to know the answers to before asking.
func TestOverviewReportsEachBreakdownTheServerCanSupport(t *testing.T) {
	f := overviewFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType+`{comm="parca"}`] = cpuProfile(t, 120)
	f.merges[testType] = cpuProfile(t, 150)
	f.merges[heapType+`{instance="10.0.0.1:6060"}`] = heapProfile(t, 1<<30)
	f.merges[heapType] = heapProfile(t, 1<<30)

	o := overviewOptions()
	o.top = 3
	out, err := runOverview(t, f, o)
	if err != nil {
		t.Fatalf("unexpected error: %v\n%s", err, out)
	}
	// It says what the server has before reporting on it.
	for _, want := range []string{"3 profile types", "cluster comm instance"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q:\n%s", want, out)
		}
	}
	// CPU by cluster and by comm, and the heap by instance.
	for _, want := range []string{"CLUSTER", "COMM", "INSTANCE", "CORES", "BYTES"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing section %q:\n%s", want, out)
		}
	}
	// namespace and workload do not exist here, and must not be invented.
	if strings.Contains(out, "NAMESPACE") {
		t.Errorf("a label the server does not have was reported:\n%s", out)
	}
}

// An overview that quietly leaves a section out is worse than one that says it
// could not run it: the reader cannot tell "no heap profile here" from "the
// heap query failed".
func TestOverviewSaysWhatItDidNotReport(t *testing.T) {
	f := overviewFixture(t)
	f.types = profileTypesFrom([]string{testType}) // no heap profile
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType+`{comm="parca"}`] = cpuProfile(t, 120)
	f.merges[testType] = cpuProfile(t, 150)

	o := overviewOptions()
	o.top = 0
	out, err := runOverview(t, f, o)
	if err != nil {
		t.Fatalf("a missing heap profile is not a failure: %v", err)
	}
	if strings.Contains(out, "BYTES") {
		t.Errorf("no heap profile exists, so no heap section:\n%s", out)
	}
	// Nothing was skipped for a reason worth stating here -- the heap simply
	// is not offered, so it never entered the plan.
	if !strings.Contains(out, "CLUSTER") || !strings.Contains(out, "COMM") {
		t.Errorf("the CPU sections should still be there:\n%s", out)
	}
}

// One failing section must not take the whole overview with it.
func TestOverviewSurvivesOneFailingSection(t *testing.T) {
	f := overviewFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 150)
	// Every comm query fails, so that whole section has nothing.
	f.mergeErrs[testType+`{comm="parca"}`] = errors.New("boom")
	f.merges[heapType+`{instance="10.0.0.1:6060"}`] = heapProfile(t, 1<<30)
	f.merges[heapType] = heapProfile(t, 1<<30)

	o := overviewOptions()
	o.top = 0
	out, err := runOverview(t, f, o)
	if err == nil {
		t.Error("an incomplete overview must exit non-zero")
	}
	if !strings.Contains(out, "CLUSTER") {
		t.Errorf("the sections that worked should still print:\n%s", out)
	}
	// The failing section still prints, carrying its own banner, so the
	// reader sees which breakdown is missing and why rather than just noticing
	// that one is absent.
	if !strings.Contains(out, "!! FAILED") || !strings.Contains(out, "comm=parca") {
		t.Errorf("the section that failed should explain itself in place:\n%s", out)
	}
	if !strings.Contains(out, "BYTES") {
		t.Errorf("later sections should still run:\n%s", out)
	}
}

// A section that could not even be attempted is named, since the reader
// otherwise cannot tell it apart from one the server simply does not support.
func TestOverviewNamesASectionItCouldNotAttempt(t *testing.T) {
	f := overviewFixture(t)
	f.merges[testType+`{comm="parca"}`] = cpuProfile(t, 120)
	f.merges[testType] = cpuProfile(t, 150)
	// The heap profile exists but none of the labels it could be grouped by
	// does. `comm` is not one of them: a heap by process name would merge
	// unrelated processes across hosts.
	f.names = []string{"comm"}

	o := overviewOptions()
	o.top = 0
	out, err := runOverview(t, f, o)
	if err != nil {
		t.Fatalf("an unreportable heap is not a failed run: %v", err)
	}
	if !strings.Contains(out, "not reported: live heap") {
		t.Errorf("the skipped section should be named:\n%s", out)
	}
}

// Several CPU profiles means no single one to report on, and guessing would
// misattribute the fleet.
func TestOverviewRefusesToPickBetweenCPUProfiles(t *testing.T) {
	f := overviewFixture(t)
	f.types = profileTypesFrom([]string{
		testType,
		"otheragent:samples:count:cpu:nanoseconds:delta",
	})
	o := overviewOptions()
	o.top = 0
	out, err := runOverview(t, f, o)
	if err == nil {
		t.Error("want a non-zero exit when nothing could be reported")
	}
	if !strings.Contains(out, "2 CPU delta profiles") {
		t.Errorf("the reason should be stated:\n%s", out)
	}
}

func TestOverviewJSON(t *testing.T) {
	f := overviewFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType+`{comm="parca"}`] = cpuProfile(t, 120)
	f.merges[testType] = cpuProfile(t, 150)
	f.merges[heapType+`{instance="10.0.0.1:6060"}`] = heapProfile(t, 1<<30)
	f.merges[heapType] = heapProfile(t, 1<<30)

	o := overviewOptions()
	o.top = 2
	o.output = outputJSON
	out, err := runOverview(t, f, o)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var d overviewData
	if e := json.Unmarshal([]byte(out), &d); e != nil {
		t.Fatalf("not one JSON document: %v\n%s", e, out)
	}
	if !d.Complete {
		t.Error("nothing failed, so it is complete")
	}
	if len(d.Sections) != 3 {
		t.Fatalf("want cluster, comm and instance sections, got %d", len(d.Sections))
	}
	units := map[string]bool{}
	for _, s := range d.Sections {
		units[s.Unit] = true
		if s.GroupBy == "" {
			t.Error("each section should name what it grouped by")
		}
	}
	// The heap section must not be reported in cores.
	if !units["cores"] || !units["bytes"] {
		t.Errorf("each profile should keep its own unit, got %v", units)
	}
	// Banners must never accompany the document.
	if strings.Contains(out, "!!") || strings.Contains(out, "not reported") {
		t.Errorf("prose must not accompany JSON:\n%s", out)
	}
}

// A section with nothing in it must say so. A heading followed by silence
// reads as truncated output, especially beside sections that did produce
// tables.
func TestEmptySectionSaysSo(t *testing.T) {
	f := reportFixture(t) // no merges: every group comes back empty
	out := captureStdout(t, func() {
		_ = report(context.Background(), testClient(f, time.Minute), testOptions(),
			time.Now().Add(-time.Hour), time.Now())
	})
	if !strings.Contains(out, "no data in this window") {
		t.Errorf("an empty result should say so on stdout:\n%q", out)
	}
}

// Every breakdown of one profile type draws its functions from the same
// unfiltered merge, so the table would be identical each time.
func TestOverviewShowsTheFunctionTableOncePerProfileType(t *testing.T) {
	f := overviewFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType+`{comm="parca"}`] = cpuProfile(t, 120)
	f.merges[testType] = cpuProfile(t, 150)
	f.merges[heapType+`{instance="10.0.0.1:6060"}`] = heapProfile(t, 1<<30)
	f.merges[heapType] = heapProfile(t, 1<<30)

	o := overviewOptions()
	o.top = 3
	out, err := runOverview(t, f, o)
	if err != nil {
		t.Fatal(err)
	}
	// Two CPU breakdowns, one CPU function table.
	if n := strings.Count(out, "app.work"); n != 1 {
		t.Errorf("the CPU function table should appear once, appeared %d times:\n%s", n, out)
	}
	// The heap is a different profile, so it keeps its own.
	if !strings.Contains(out, "app.alloc") {
		t.Errorf("the heap function table should still appear:\n%s", out)
	}
}

// overviewOptions is what `parcareport overview` is given: no --by and no
// --profile-type, since the command chooses those per section.
func overviewOptions() options {
	o := testOptions()
	o.profileType, o.by = "", ""
	o.maxGroups = 50
	o.setFlags = map[string]bool{}
	return o
}

// The bug this guards: the "already shown" flag was set before the section
// ran, so it recorded "attempted". If the first section of a profile type
// failed or came back empty, every later one was suppressed too and the
// overview carried no function table at all -- the headline of the tool,
// missing because an unrelated breakdown failed.
func TestFunctionTableSurvivesAFailingFirstSection(t *testing.T) {
	f := overviewFixture(t)
	// The cluster section fails outright; comm works.
	f.mergeErrs[testType+`{cluster="tc"}`] = errors.New("boom")
	f.mergeErrs[testType+`{cluster="vps"}`] = errors.New("boom")
	f.merges[testType+`{comm="parca"}`] = cpuProfile(t, 120)
	f.merges[testType] = cpuProfile(t, 150)

	o := overviewOptions()
	o.top = 3
	out, _ := runOverview(t, f, o)
	if !strings.Contains(out, "app.work") {
		t.Errorf("a failing first section must not suppress the function table:\n%s", out)
	}
}

// Suppression is a table concern. In JSON, duplicate data costs nothing and an
// empty array is indistinguishable from "no hot functions".
func TestJSONKeepsFunctionsForEverySection(t *testing.T) {
	f := overviewFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType+`{comm="parca"}`] = cpuProfile(t, 120)
	f.merges[testType] = cpuProfile(t, 150)
	f.merges[heapType+`{instance="10.0.0.1:6060"}`] = heapProfile(t, 1<<30)
	f.merges[heapType] = heapProfile(t, 1<<30)

	o := overviewOptions()
	o.output, o.top = outputJSON, 3
	out, err := runOverview(t, f, o)
	if err != nil {
		t.Fatal(err)
	}
	var d overviewData
	if e := json.Unmarshal([]byte(out), &d); e != nil {
		t.Fatal(e)
	}
	for _, s := range d.Sections {
		if s.ProfileType == testType && len(s.Functions) == 0 {
			t.Errorf("section %q lost its functions; an empty array reads as 'none hot'", s.GroupBy)
		}
	}
}

// One merge per label value, so a label with hundreds of them would swamp the
// sections worth having.
func TestOverviewSkipsAHighCardinalityBreakdown(t *testing.T) {
	f := overviewFixture(t)
	many := make([]string, 0, 200)
	for i := 0; i < 200; i++ {
		many = append(many, fmt.Sprintf("proc%d", i))
	}
	f.values["comm"] = many
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 150)
	f.merges[heapType+`{instance="10.0.0.1:6060"}`] = heapProfile(t, 1<<30)
	f.merges[heapType] = heapProfile(t, 1<<30)

	o := overviewOptions()
	o.top = 0
	out, err := runOverview(t, f, o)
	if err != nil {
		t.Fatalf("skipping a wide breakdown is not a failure: %v", err)
	}
	if strings.Contains(out, "COMM") {
		t.Errorf("a 200-value breakdown should be skipped:\n%s", out)
	}
	if !strings.Contains(out, "200 values is more than") {
		t.Errorf("and the reason should be stated:\n%s", out)
	}
	// The cheap sections still run.
	if !strings.Contains(out, "CLUSTER") || !strings.Contains(out, "INSTANCE") {
		t.Errorf("the affordable sections should still run:\n%s", out)
	}
}

// The bug this guards: `have` is the union of labels across all profile types,
// so the heap could be paired with `cluster` -- a parca-agent label its own
// series do not carry. The section then merged once per cluster, found
// nothing, and reported "(no data in this window)" for a heap that has plenty.
func TestHeapIsNotGroupedByAnAgentOnlyLabel(t *testing.T) {
	f := overviewFixture(t)
	f.names = []string{"cluster", "comm"} // no instance, no job
	delete(f.values, "instance")
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType+`{comm="parca"}`] = cpuProfile(t, 120)
	f.merges[testType] = cpuProfile(t, 150)

	o := overviewOptions()
	o.top = 0
	out, err := runOverview(t, f, o)
	if err != nil {
		t.Fatalf("an unreportable heap is not a failed run: %v\n%s", err, out)
	}
	if strings.Contains(out, "no data in this window") {
		t.Errorf("the heap has data; it just carries no such label:\n%s", out)
	}
	if !strings.Contains(out, "not reported: live heap") {
		t.Errorf("the skip should be named:\n%s", out)
	}
	if !strings.Contains(out, "scrape targets") {
		t.Errorf("and should say why those labels are the right ones:\n%s", out)
	}
}

// Silently ignoring a flag is worse than refusing it.
func TestOverviewRefusesFlagsItWouldOverwrite(t *testing.T) {
	for _, f := range []string{"by", "profile-type"} {
		o := overviewOptions()
		o.setFlags = map[string]bool{f: true}
		_, err := runOverview(t, overviewFixture(t), o)
		if err == nil {
			t.Errorf("--%s should be refused, not ignored", f)
			continue
		}
		if !strings.Contains(err.Error(), "parcareport report") {
			t.Errorf("the error should point at the command that does take it: %v", err)
		}
	}
}

func TestFindHeapTypeMatchesOnParts(t *testing.T) {
	if got := findHeapType(realServerTypes); got != heapType {
		t.Errorf("got %q", got)
	}
	// A sample type that merely starts the same way must not match.
	if got := findHeapType([]string{"memory:inuse_space_extra:bytes:space:bytes"}); got != "" {
		t.Errorf("prefix match leaked: %q", got)
	}
	if got := findHeapType([]string{"memory:alloc_space:bytes:space:bytes"}); got != "" {
		t.Errorf("alloc_space is not the live heap: %q", got)
	}
}

// The bug this guards: a hidden ten-minute budget capped every run, so
// --timeout=600s was accepted while being exactly the ceiling. It could then
// never fire -- the run's own clock reached every outstanding query first, and
// 26 groups reported "context deadline exceeded" simultaneously. That reads as
// 26 slow queries; it was one clock. Worse, the hint printed alongside said to
// raise --timeout, which could not help.
func TestTimeoutAtOrAboveTheDeadlineIsRejected(t *testing.T) {
	for _, args := range [][]string{
		{"--url=localhost:1", "--timeout=10m", "--deadline=10m"}, // equal
		{"--url=localhost:1", "--timeout=20m", "--deadline=10m"}, // above
		{"--url=localhost:1", "--timeout=600s"},                  // equal to the default
	} {
		err := run(args)
		if err == nil {
			t.Errorf("%v should be refused", args)
			continue
		}
		for _, want := range []string{"--timeout", "--deadline", "never fire"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%v: error should mention %q, got: %v", args, want, err)
			}
		}
	}
}

// runContext is where both clocks are decided, so test it directly rather
// than inferring from run()'s error text -- which would pass even if the
// deadline were never applied, and needs a network round trip to reach.
func TestRunContext(t *testing.T) {
	tests := []struct {
		name            string
		timeout         time.Duration
		deadline        time.Duration
		wantErr         string
		wantHasDeadline bool
	}{
		{name: "normal", timeout: 30 * time.Second, deadline: 10 * time.Minute, wantHasDeadline: true},
		{name: "no budget", timeout: 30 * time.Minute, deadline: 0, wantHasDeadline: false},
		{name: "timeout equals deadline", timeout: 10 * time.Minute, deadline: 10 * time.Minute, wantErr: "never fire"},
		{name: "timeout above deadline", timeout: 20 * time.Minute, deadline: 10 * time.Minute, wantErr: "never fire"},
		// A non-positive timeout is an already-expired per-query context, so
		// every query fails before it is sent.
		{name: "zero timeout", timeout: 0, deadline: 10 * time.Minute, wantErr: "--timeout must be positive"},
		{name: "negative timeout", timeout: -5 * time.Second, deadline: 10 * time.Minute, wantErr: "--timeout must be positive"},
		// A negative deadline must not be a second, undocumented spelling of
		// "no budget".
		{name: "negative deadline", timeout: 30 * time.Second, deadline: -time.Second, wantErr: "cannot be negative"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel, err := runContext(options{timeout: tc.timeout, deadline: tc.deadline})
			defer cancel()
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("want an error mentioning %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("error = %v, want it to mention %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			dl, ok := ctx.Deadline()
			if ok != tc.wantHasDeadline {
				t.Fatalf("ctx has deadline = %v, want %v", ok, tc.wantHasDeadline)
			}
			if ok {
				// Close to now+deadline, not some other duration.
				if d := time.Until(dl); d > tc.deadline || d < tc.deadline-time.Minute {
					t.Errorf("deadline is %s away, want about %s", d, tc.deadline)
				}
			}
		})
	}
}

// The bug this guards: rejecting timeout >= deadline only removes the case
// where the per-query clock is dead from the first query. A run that simply
// USES UP its budget still cut every outstanding query short at once, and
// still advised raising --timeout -- which cannot help, and which reads as N
// slow queries when it was one clock.
func TestRunDeadlineExpiryNamesTheRightClock(t *testing.T) {
	// A run context that has already expired, and a generous per-query
	// timeout: exactly the state a late section of a long run is in.
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)

	c := testClient(&fakeQuery{block: time.Hour}, time.Minute)
	_, err := c.LabelValues(ctx, "cluster", time.Now().Add(-time.Hour), time.Now())
	if err == nil {
		t.Fatal("want a deadline error")
	}
	if strings.Contains(err.Error(), "raise --timeout") {
		t.Errorf("the run's budget expired, so --timeout is the wrong knob: %v", err)
	}
	if !strings.Contains(err.Error(), "--deadline") {
		t.Errorf("error should name --deadline: %v", err)
	}
	// And the hint alongside it must say the same thing.
	h := hintFor([]string{"context deadline exceeded"}, mergeQuery, true)
	if strings.Contains(h, "Raise --timeout") {
		t.Errorf("hint still points at --timeout: %q", h)
	}
	if !strings.Contains(h, "one clock, not many slow queries") {
		t.Errorf("hint should say it was one clock: %q", h)
	}
}

// A genuine per-query timeout still gets the per-query advice.
func TestPerQueryTimeoutStillNamesTimeout(t *testing.T) {
	c := testClient(&fakeQuery{block: time.Hour}, 10*time.Millisecond)
	_, err := c.LabelValues(context.Background(), "cluster", time.Now().Add(-time.Hour), time.Now())
	if err == nil {
		t.Fatal("want a deadline error")
	}
	if !strings.Contains(err.Error(), "raise --timeout") {
		t.Errorf("a per-query timeout should name --timeout: %v", err)
	}
	if strings.Contains(err.Error(), "--deadline") {
		t.Errorf("the run budget was not involved: %v", err)
	}
}

// The bug this guards: overview reads the profile type list once, then picks
// each section's type from it -- but gatherReport re-fetched and re-validated
// the whole list for every section. Four sections meant four redundant
// ProfileTypes calls, and when the lookup failed, four copies of the same
// warning for a value that came from the server's own list.
func TestOverviewResolvesTheProfileTypeOnce(t *testing.T) {
	f := overviewFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType+`{comm="parca"}`] = cpuProfile(t, 120)
	f.merges[testType] = cpuProfile(t, 150)
	f.merges[heapType+`{instance="10.0.0.1:6060"}`] = heapProfile(t, 1<<30)
	f.merges[heapType] = heapProfile(t, 1<<30)

	o := overviewOptions()
	o.top = 0
	if _, err := runOverview(t, f, o); err != nil {
		t.Fatal(err)
	}
	// One call for overview's own list. Anything more is a section re-asking
	// for a list it was handed.
	if n := f.typeCalls.Load(); n != 1 {
		t.Errorf("ProfileTypes called %d times, want 1", n)
	}
}

// A plain report still resolves the type itself -- the shortcut is only for a
// caller that already knows it.
func TestReportStillResolvesItsOwnType(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType] = cpuProfile(t, 100)
	o := testOptions()
	o.top = 0
	out := captureStdout(t, func() {
		end := time.Now()
		if err := report(context.Background(), testClient(f, time.Minute), o,
			end.Add(-10*time.Second), end); err != nil {
			t.Fatal(err)
		}
	})
	if f.typeCalls.Load() == 0 {
		t.Error("report must still check the type it was given")
	}
	if !strings.Contains(out, testType) {
		t.Errorf("missing the heading:\n%s", out)
	}
}

// overview issues more queries than anything else, and a Parca sized for
// ingestion refuses at four abreast: measured on a real server, an 8-group
// heap breakdown failed all 8 at --concurrency=8 and succeeded at 1.
func TestOverviewLowersConcurrencyUnlessAsked(t *testing.T) {
	// Enough groups to exceed the cap, and a delay on each merge so they
	// actually overlap -- without both, the observed peak is 1 whatever the
	// setting, and the assertion means nothing.
	fixture := func() *fakeQuery {
		f := overviewFixture(t)
		f.types = profileTypesFrom([]string{testType})
		f.names = []string{"cluster"}
		f.values["cluster"] = []string{"c1", "c2", "c3", "c4", "c5"}
		for _, c := range f.values["cluster"] {
			f.merges[testType+`{cluster="`+c+`"}`] = cpuProfile(t, 10)
		}
		f.merges[testType] = cpuProfile(t, 50)
		f.mergeDelay = 20 * time.Millisecond
		return f
	}

	// Default: turned down.
	f := fixture()
	o := overviewOptions()
	o.top, o.concurrency = 0, 4
	if _, err := runOverview(t, f, o); err != nil {
		t.Fatal(err)
	}
	if peak := f.maxParallel.Load(); peak > int64(overviewConcurrency) {
		t.Errorf("ran %d queries at once, want at most %d", peak, overviewConcurrency)
	}

	// Explicit: honoured. Someone who has measured their own server knows
	// better than this default.
	f2 := fixture()
	o2 := overviewOptions()
	o2.top, o2.concurrency = 0, 4
	o2.setFlags = map[string]bool{"concurrency": true}
	if _, err := runOverview(t, f2, o2); err != nil {
		t.Fatal(err)
	}
	if peak := f2.maxParallel.Load(); peak <= int64(overviewConcurrency) {
		t.Errorf("an explicit --concurrency=4 was lowered anyway (peak %d)", peak)
	}
}

// A stream reset under load is the server going away, not the query being
// wrong, and it is transient. One retry turns a lost breakdown into a slow
// one.
func (f *fakeQuery) callsFor(sel string) int {
	f.mergeMu.Lock()
	defer f.mergeMu.Unlock()
	return f.mergeCalls[sel]
}

func TestATransientlyFailedGroupIsRetried(t *testing.T) {
	f := overviewFixture(t)
	f.types = profileTypesFrom([]string{testType})
	// One label, so one section: this test is about the retry, not about
	// other breakdowns having no data.
	f.names = []string{"cluster"}
	f.values["cluster"] = []string{"tc"}
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType] = cpuProfile(t, 100)
	// Fail only the first merge, the way an overloaded server drops one
	// stream. Failing two would defeat the single retry as well, which is
	// what this test would then be measuring instead.
	f.failFirstN = 1

	o := overviewOptions()
	o.top = 0
	out, err := runOverview(t, f, o)
	if err != nil {
		t.Fatalf("a transient failure should be retried, not reported: %v\n%s", err, out)
	}
	if !strings.Contains(out, "CLUSTER") {
		t.Errorf("the retried section should appear:\n%s", out)
	}
	if strings.Contains(out, "FAILED") || strings.Contains(out, "INCOMPLETE") {
		t.Errorf("a retried section should not leave a banner behind:\n%s", out)
	}
	if !strings.Contains(out, "asked again") {
		t.Errorf("a run that silently took twice as long is what this avoids:\n%s", out)
	}
}

// The retry exists because the server ran out of room. Re-running the whole
// fan-out would send it the same load again, which is the one thing the
// failure says not to do.
func TestRetryAsksOnlyTheGroupThatFailed(t *testing.T) {
	f := overviewFixture(t)
	f.types = profileTypesFrom([]string{testType})
	f.names = []string{"cluster"}
	f.values["cluster"] = []string{"a", "b", "c", "d", "e"}
	for _, g := range f.values["cluster"] {
		f.merges[testType+`{cluster="`+g+`"}`] = cpuProfile(t, 10)
	}
	f.merges[testType] = cpuProfile(t, 50)
	// One group keeps dropping its stream. It stays failed -- that is not
	// what this test is about; what matters is who else got asked again.
	failing := testType + `{cluster="c"}`
	f.failSel = failing

	o := overviewOptions()
	o.top = 0
	if _, err := runOverview(t, f, o); err == nil {
		t.Fatal("a group that never answers should leave the run incomplete")
	}
	if n := f.callsFor(failing); n != 2 {
		t.Errorf("the failed group was merged %d times, want 2 (once, then one retry)", n)
	}
	for _, g := range []string{"a", "b", "d", "e"} {
		sel := testType + `{cluster="` + g + `"}`
		if n := f.callsFor(sel); n != 1 {
			t.Errorf("group %s was merged %d times, want 1: only the failed group is retried", g, n)
		}
	}
}

// Retrying a query the server rejected on its merits just sends the same
// wrong query again.
func TestPermanentFailuresAreNotRetried(t *testing.T) {
	f := overviewFixture(t)
	f.types = profileTypesFrom([]string{testType})
	f.names = []string{"cluster"}
	f.values["cluster"] = []string{"a", "b"}
	f.merges[testType+`{cluster="a"}`] = cpuProfile(t, 10)
	f.merges[testType] = cpuProfile(t, 10)
	failing := testType + `{cluster="b"}`
	f.mergeErrs[failing] = status.Error(codes.InvalidArgument,
		"profile-type selection must be of the form")

	o := overviewOptions()
	o.top = 0
	out, _ := runOverview(t, f, o)
	if n := f.callsFor(failing); n != 1 {
		t.Errorf("a rejected query was merged %d times, want 1", n)
	}
	if strings.Contains(out, "asked again") {
		t.Errorf("nothing was retried, so nothing should say so:\n%s", out)
	}
}

// "The label is not there" and "the query for it failed" are different
// answers, and saying the first when the second happened sends the reader
// looking for a relabel_config that is not missing.
func TestAFailedLabelLookupIsNotReportedAsAnAbsentLabel(t *testing.T) {
	f := overviewFixture(t)
	f.types = profileTypesFrom([]string{testType})
	f.names = []string{"cluster"}
	f.values["cluster"] = []string{"tc"}
	// The one breakdown the plan could have used cannot be counted.
	f.valuesErrFor = map[string]error{"cluster": errors.New("boom")}

	o := overviewOptions()
	o.top = 0
	out, _ := runOverview(t, f, o)
	if strings.Contains(out, "exists in this window") {
		t.Errorf("the label exists; its lookup failed. Output:\n%s", out)
	}
	if !strings.Contains(out, "label lookups above failed") {
		t.Errorf("the reader should learn the lookups failed:\n%s", out)
	}
}

// Losing a label lookup costs the whole section, not one group, so it gets
// the same single retry.
func TestADroppedLabelLookupIsRetried(t *testing.T) {
	f := overviewFixture(t)
	f.types = profileTypesFrom([]string{testType})
	f.names = []string{"cluster"}
	f.values["cluster"] = []string{"tc"}
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType] = cpuProfile(t, 100)
	f.valuesFailFirst = map[string]int{"cluster": 1}

	o := overviewOptions()
	o.top = 0
	out, err := runOverview(t, f, o)
	if err != nil {
		t.Fatalf("a dropped label lookup should be retried, not lost: %v\n%s", err, out)
	}
	if !strings.Contains(out, "CLUSTER") {
		t.Errorf("the section should have been planned after the retry:\n%s", out)
	}
}

// The retry exists because the server was out of room. Sending the retries
// in parallel would be a smaller version of the same mistake.
func TestTheRetryGoesOneAtATime(t *testing.T) {
	f := overviewFixture(t)
	f.types = profileTypesFrom([]string{testType})
	f.names = []string{"cluster"}
	f.values["cluster"] = []string{"a", "b", "c", "d"}
	for _, g := range f.values["cluster"] {
		f.merges[testType+`{cluster="`+g+`"}`] = cpuProfile(t, 10)
	}
	f.merges[testType] = cpuProfile(t, 40)
	// Every group drops its first attempt, so the retry pass has four
	// queries in it -- enough to run in parallel if it were allowed to.
	f.failFirstN = 4
	f.mergeDelay = 20 * time.Millisecond
	// Ignore the first fan-out's peak; this test is about the retry.
	f.peakAfter = 4

	o := overviewOptions()
	o.top, o.concurrency = 0, 4
	if _, err := runOverview(t, f, o); err != nil {
		t.Fatalf("every group recovered on retry, so the run should succeed: %v", err)
	}
	if peak := f.maxParallel.Load(); peak > 1 {
		t.Errorf("the retry ran %d queries at once, want 1 at a time", peak)
	}
}

// The lookup inside the report is the one that costs the whole report: with
// no label values there is nothing to break anything down by.
func TestTheReportsOwnLabelLookupIsRetried(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType] = cpuProfile(t, 100)
	f.valuesFailFirst = map[string]int{"cluster": 1}

	o := testOptions()
	o.top = 0
	out, err := runReport(t, f, o)
	if err != nil {
		t.Fatalf("a dropped label lookup should be retried, not lost: %v\n%s", err, out)
	}
	if !strings.Contains(out, "CLUSTER") {
		t.Errorf("the report should have been built after the retry:\n%s", out)
	}
}

// Checking the budget once, before the retry pass, is not enough: a section
// with many dropped groups would start a retry that ate the whole --deadline
// and left every later section failing. The retry would then have made the
// run worse than no retry at all.
func TestTheRetryStopsWhenTheBudgetRunsOut(t *testing.T) {
	f := reportFixture(t)
	f.values["cluster"] = []string{"a", "b", "c", "d", "e", "f"}
	for _, g := range f.values["cluster"] {
		f.merges[testType+`{cluster="`+g+`"}`] = cpuProfile(t, 10)
	}
	f.merges[testType] = cpuProfile(t, 60)
	// Every group drops its first attempt. Failures are instant, so the
	// first pass costs nothing; each retry then succeeds and costs
	// mergeDelay, which is what eats the budget.
	f.failFirstN = 6
	f.mergeDelay = 100 * time.Millisecond

	o := testOptions()
	// A timeout large against the deadline, so "is there room for one more"
	// is what stops the pass. With a small one the half-budget cap fires
	// first and this test would pass with the per-retry check deleted --
	// which is what it is named for.
	o.top, o.timeout = 0, 120*time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()

	var out string
	func() {
		out = captureStdout(t, func() {
			start := time.Now().Add(-time.Hour)
			_ = report(ctx, testClient(f, time.Minute), o, start, time.Now())
		})
	}()
	// Six retries at 100ms each cannot fit in 350ms. If the budget is only
	// checked once, all six are attempted anyway.
	if strings.Contains(out, "6 cluster queries were asked again") {
		t.Errorf("every group was retried despite the budget running out:\n%s", out)
	}
	if !strings.Contains(out, "asked again") {
		t.Errorf("some groups should still have been retried:\n%s", out)
	}
}

// `parcareport labels <name>` is one query. Losing it kills the whole command,
// so it is the last place that should go without the retry.
func TestListingOneLabelsValuesIsRetried(t *testing.T) {
	f := reportFixture(t)
	f.valuesFailFirst = map[string]int{"cluster": 1}

	out := captureStdout(t, func() {
		if err := listLabels(context.Background(), testClient(f, time.Minute), "cluster",
			time.Now().Add(-time.Hour), time.Now(), 4, time.Minute); err != nil {
			t.Fatalf("a dropped lookup should be retried, not fatal: %v", err)
		}
	})
	if !strings.Contains(out, "tc") {
		t.Errorf("the values should have been listed after the retry:\n%s", out)
	}
}

// The retry exists to surface the dropped stream. Replacing that error with
// whatever the second attempt hit throws away the diagnostic.
func TestAFailedRetryKeepsTheFirstError(t *testing.T) {
	f := reportFixture(t)
	// Both attempts drop the stream; the second one is what a run whose
	// deadline expired mid-retry would report instead.
	f.valuesFailFirst = map[string]int{"cluster": 1}
	f.valuesErrFor = map[string]error{"cluster": context.DeadlineExceeded}

	_, err := labelValues(context.Background(), testClient(f, time.Minute), time.Minute,
		"cluster", time.Now().Add(-time.Hour), time.Now())
	if err == nil {
		t.Fatal("both attempts failed, so this should be an error")
	}
	if !strings.Contains(err.Error(), "RST_STREAM") {
		t.Errorf("want the first error (the dropped stream), got: %v", err)
	}
}

// Sections share one --deadline. A section with many dropped groups that
// retried until the budget was nearly gone left the later sections nothing,
// so the retry made the run worse than no retry at all.
func TestTheRetryPassLeavesBudgetForWhatComesNext(t *testing.T) {
	f := reportFixture(t)
	f.values["cluster"] = []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for _, g := range f.values["cluster"] {
		f.merges[testType+`{cluster="`+g+`"}`] = cpuProfile(t, 10)
	}
	f.merges[testType] = cpuProfile(t, 80)
	f.failFirstN = 8
	f.mergeDelay = 100 * time.Millisecond

	o := testOptions()
	o.top, o.timeout = 0, 40*time.Millisecond
	// Stand in for one section of an overview: more runs after this under
	// the same deadline, which is what the cap is for.
	o.moreToCome = true
	// A full second: every single retry passes "is there room for one more",
	// so only the cap on the pass as a whole can stop it.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	out := captureStdout(t, func() {
		start := time.Now().Add(-time.Hour)
		_ = report(ctx, testClient(f, time.Minute), o, start, time.Now())
	})
	if strings.Contains(out, "8 cluster queries were asked again") {
		t.Errorf("the retry pass spent the whole budget:\n%s", out)
	}
	if !strings.Contains(out, "asked again") {
		t.Errorf("some groups should still have been retried:\n%s", out)
	}
}

// A retry is another query, and a run with no budget left for one should not
// start it: the retry would only fail on the deadline and the error the user
// sees would be about the deadline rather than the dropped stream.
func TestNoLabelLookupRetryWithoutBudgetForIt(t *testing.T) {
	f := reportFixture(t)
	f.valuesFailFirst = map[string]int{"cluster": 1}

	// Less left than one query is allowed to take, so there is no room.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := labelValues(ctx, testClient(f, time.Minute), 60*time.Millisecond,
		"cluster", time.Now().Add(-time.Hour), time.Now())
	if err == nil {
		t.Fatal("with no budget for a retry the dropped lookup should be reported, not retried")
	}
	if !strings.Contains(err.Error(), "RST_STREAM") {
		t.Errorf("want the dropped stream reported, got: %v", err)
	}
}

// Once, not until it works. A server that stays out of room would otherwise
// get the whole fan-out again and again.
func TestATransientFailureIsRetriedOnlyOnce(t *testing.T) {
	f := overviewFixture(t)
	f.types = profileTypesFrom([]string{testType})
	f.names = []string{"cluster"}
	f.values["cluster"] = []string{"a", "b"}
	f.merges[testType+`{cluster="a"}`] = cpuProfile(t, 10)
	f.merges[testType] = cpuProfile(t, 10)
	failing := testType + `{cluster="b"}`
	f.failSel = failing // never recovers

	o := overviewOptions()
	o.top = 0
	out, _ := runOverview(t, f, o)
	if n := f.callsFor(failing); n != 2 {
		t.Errorf("a group that never recovers was merged %d times, want exactly 2", n)
	}
	if !strings.Contains(out, "INCOMPLETE") && !strings.Contains(out, "FAILED") {
		t.Errorf("a failure that survived the retry is still a failure:\n%s", out)
	}
}

// A genuine error must not be retried -- that just doubles the load that
// caused it.
func TestLooksTransient(t *testing.T) {
	for _, s := range []string{
		"stream terminated by RST_STREAM with error code: INTERNAL_ERROR",
		"error reading from server: EOF",
		`"error reading server preface: connection reset by peer"`,
		"code = Unavailable desc = connection error",
	} {
		if !looksTransient(errors.New(s)) {
			t.Errorf("%q is the server going away", s)
		}
	}
	for _, s := range []string{
		"no data in this window",
		"profile-type selection must be of the form",
		"context deadline exceeded",
		"no label \"clustr\"",
	} {
		if looksTransient(errors.New(s)) {
			t.Errorf("%q is not worth retrying", s)
		}
	}
	if looksTransient(nil) {
		t.Error("nil is not a failure")
	}
}

// snapshotTime is when the fake pretends its non-delta profiles were written.
// A date safely in the past, so a window built from it can never overlap the
// time.Now() windows the other tests use and the two cannot interfere.
var snapshotTime = time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)

// QueryRange answers "when were there profiles for this selector". The real
// server returns one sample per profile; the fake returns one sample for any
// selector it has a profile for, so the snapshot path has a timestamp to fetch.
func (f *fakeQuery) QueryRange(ctx context.Context, in *qv1.QueryRangeRequest, _ ...grpc.CallOption) (*qv1.QueryRangeResponse, error) {
	sel := in.GetQuery()
	if err := f.mergeErrs[sel]; err != nil {
		return nil, err
	}
	if f.merges[sel] == nil {
		// The real Parca answers NotFound here, not an empty response --
		// unlike Merge, which answers OK with an empty pprof. Mirrored so the
		// tests exercise the asymmetry the client has to absorb.
		return nil, status.Error(codes.NotFound,
			"No data found for the query, try a different query or time range or no data has been written to be queried yet.")
	}
	// Timestamps come from the same helper the merge counts, so the two
	// cannot contradict each other. An earlier version reported a single
	// timestamp regardless, which let the client infer "no interval" while
	// the merge still multiplied by the scrape count: the fake contradicted
	// itself and the test that mattered could not fail.
	var series []*qv1.MetricsSeries
	for s := 0; s < f.seriesCount(); s++ {
		times := f.scrapeTimes(s, in.GetStart().AsTime(), in.GetEnd().AsTime())
		if len(times) == 0 {
			continue
		}
		samples := make([]*qv1.MetricsSample, 0, len(times))
		for _, at := range times {
			samples = append(samples, &qv1.MetricsSample{Timestamp: timestamppb.New(at)})
		}
		series = append(series, &qv1.MetricsSeries{Samples: samples})
	}
	if len(series) == 0 {
		return nil, status.Error(codes.NotFound,
			"No data found for the query, try a different query or time range or no data has been written to be queried yet.")
	}
	return &qv1.QueryRangeResponse{Series: series}, nil
}

func (f *fakeQuery) seriesCount() int {
	if f.multiSeries < 1 {
		return 1
	}
	return f.multiSeries
}

// fakeScrapeTimes lists when series s was scraped inside [start, end], oldest
// first.
//
// Targets are staggered ACROSS the interval, the way Parca spreads scrape
// targets by a hash of the target. A fake that put every series within a
// second of the others could not show a merge window dropping half of them,
// which is exactly the bug that hid behind the old one.
func (f *fakeQuery) scrapeTimes(s int, start, end time.Time) []time.Time {
	// Scrapes run up to the end of whatever window is asked for -- what a
	// live server looks like -- except in the snapshot fixtures, whose newest
	// profile is snapshotTime.
	anchor := end
	if !snapshotTime.Before(start) && !snapshotTime.After(end) {
		anchor = snapshotTime
	}
	seriesCount := f.seriesCount()
	offset := time.Duration(s) * fakeScrapeInterval / time.Duration(seriesCount)
	// A target that stopped scraping a while back: its newest sample is
	// several intervals old, so a one-interval window cannot reach it.
	if s >= seriesCount-f.laggingSeries {
		offset += 3 * fakeScrapeInterval
	}
	var out []time.Time
	for at := anchor.Add(-offset); !at.Before(start); at = at.Add(-fakeScrapeInterval) {
		if at.After(end) {
			continue
		}
		out = append(out, at)
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	if s < f.extraScrape && len(out) > 0 {
		extra := out[len(out)-1].Add(-5 * time.Second)
		if !extra.Before(start) {
			out = append(out[:len(out)-1], extra, out[len(out)-1])
		}
	}
	return out
}

// fakeScrapeInterval is how often the fake pretends profiles were written, so
// a merge over a window can sum the right number of them.
const fakeScrapeInterval = time.Minute

// scaleProfile multiplies every sample value, the way summing N profiles of
// the same shape would.
func scaleProfile(p *profile.Profile, n int64) *profile.Profile {
	out := p.Copy()
	for _, s := range out.Sample {
		for i := range s.Value {
			s.Value[i] *= n
		}
	}
	return out
}

func TestIsDeltaType(t *testing.T) {
	for _, s := range []string{
		"parca_agent:samples:count:cpu:nanoseconds:delta",
		"parca_agent:wallclock:nanoseconds:samples:count:delta",
	} {
		if !isDeltaType(s) {
			t.Errorf("%q is a delta", s)
		}
	}
	// Every non-delta type this server offers. Each is a level, not an
	// accumulation, so none may be merged across scrapes.
	for _, s := range []string{
		"memory:inuse_space:bytes:space:bytes",
		"memory:inuse_objects:count:space:bytes",
		"memory:alloc_space:bytes:space:bytes",
		"memory:alloc_objects:count:space:bytes",
		"goroutine:goroutine:count:goroutine:count",
		"mutex:contentions:count:contentions:count",
		"block:contentions:count:contentions:count",
	} {
		if isDeltaType(s) {
			t.Errorf("%q is not a delta", s)
		}
	}
}

// The bug this guards, measured against a real server: inuse_space read
// 20.8 MiB over a 1-minute window and 386.9 MiB over 20 minutes, because the
// merge summed ~19 scrapes of a level. A snapshot must not depend on how wide
// the window is.
func TestSnapshotProfileDoesNotScaleWithWindow(t *testing.T) {
	newFixture := func() *fakeQuery {
		f := reportFixture(t)
		f.types = profileTypesFrom([]string{heapType})
		f.merges[heapType+`{cluster="tc"}`] = heapProfile(t, 20<<20)
		f.merges[heapType] = heapProfile(t, 20<<20)
		return f
	}
	o := testOptions()
	o.profileType, o.top = heapType, 0

	end := snapshotTime.Add(time.Minute)
	var oneMin, twentyMin string
	oneMin = captureStdout(t, func() {
		if err := report(context.Background(), testClient(newFixture(), time.Minute), o,
			end.Add(-1*time.Minute), end); err != nil {
			t.Fatal(err)
		}
	})
	twentyMin = captureStdout(t, func() {
		if err := report(context.Background(), testClient(newFixture(), time.Minute), o,
			end.Add(-20*time.Minute), end); err != nil {
			t.Fatal(err)
		}
	})

	get := func(out string) string {
		for _, l := range strings.Split(out, "\n") {
			if strings.HasPrefix(l, "tc") {
				return strings.Join(strings.Fields(l)[1:], " ")
			}
		}
		return ""
	}
	if get(oneMin) == "" {
		t.Fatalf("no tc row:\n%s", oneMin)
	}
	if get(oneMin) != get(twentyMin) {
		t.Errorf("a snapshot must not depend on window width:\n 1m: %s\n20m: %s",
			get(oneMin), get(twentyMin))
	}
	// And it must be the real value, not a multiple of it.
	if !strings.Contains(get(oneMin), "20.0 MiB") {
		t.Errorf("want the profile's own value, got %s", get(oneMin))
	}
}

// A delta still merges the window -- that is what makes CORES over an hour
// mean anything, and the fix must not break it.
func TestDeltaProfileStillMergesTheWindow(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType] = cpuProfile(t, 100)
	o := testOptions()
	o.top = 0

	// 100 samples x 10ms = 1 CPU-second. Over 10s that is 0.100 cores.
	out := captureStdout(t, func() {
		end := time.Now()
		if err := report(context.Background(), testClient(f, time.Minute), o,
			end.Add(-10*time.Second), end); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(out, "0.100") {
		t.Errorf("delta should still be a rate over the window:\n%s", out)
	}
}

// The report has to say which moment a snapshot describes, or the heading's
// window implies the numbers cover it.
func TestSnapshotReportsItsOwnTimestamp(t *testing.T) {
	f := reportFixture(t)
	f.types = profileTypesFrom([]string{heapType})
	f.merges[heapType+`{cluster="tc"}`] = heapProfile(t, 20<<20)
	f.merges[heapType] = heapProfile(t, 20<<20)
	o := testOptions()
	o.profileType, o.top, o.output = heapType, 0, outputJSON

	end := snapshotTime.Add(5 * time.Minute)
	out := captureStdout(t, func() {
		if err := report(context.Background(), testClient(f, time.Minute), o,
			end.Add(-20*time.Minute), end); err != nil {
			t.Fatal(err)
		}
	})
	d := decodeReport(t, out)
	if d.Delta {
		t.Error("a heap profile is not a delta")
	}
	if d.SnapshotAt == nil {
		t.Fatal("a snapshot must name the profile it came from")
	}
	if !d.SnapshotAt.Equal(snapshotTime) {
		t.Errorf("snapshot_at = %v, want %v", d.SnapshotAt, snapshotTime)
	}
}

// A delta has no single moment, so the field stays null rather than inventing
// one.
func TestDeltaHasNoSnapshotTimestamp(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType] = cpuProfile(t, 100)
	o := testOptions()
	o.top, o.output = 0, outputJSON

	out := captureStdout(t, func() {
		end := time.Now()
		if err := report(context.Background(), testClient(f, time.Minute), o,
			end.Add(-10*time.Second), end); err != nil {
			t.Fatal(err)
		}
	})
	d := decodeReport(t, out)
	if !d.Delta {
		t.Error("the CPU profile is a delta")
	}
	if d.SnapshotAt != nil {
		t.Errorf("a merged window has no single timestamp, got %v", d.SnapshotAt)
	}
}

// The bug this guards: routing snapshots through QueryRange made every group
// without a profile look like a failed query, because QueryRange answers
// NotFound where Merge answers OK-with-nothing. In the run that caught it, 7
// of 8 instances were reported as failures when they simply had no heap
// profile -- the exact empty-vs-failed confusion this tool exists to prevent.
func TestSnapshotGroupsWithNoDataAreEmptyNotFailed(t *testing.T) {
	f := reportFixture(t)
	f.types = profileTypesFrom([]string{heapType})
	f.values["cluster"] = []string{"tc", "vps"}
	// Only tc has a heap profile; vps has none at all.
	f.merges[heapType+`{cluster="tc"}`] = heapProfile(t, 20<<20)
	f.merges[heapType] = heapProfile(t, 20<<20)

	o := testOptions()
	o.profileType, o.top = heapType, 0

	var err error
	end := snapshotTime.Add(time.Minute)
	out := captureStdout(t, func() {
		err = report(context.Background(), testClient(f, time.Minute), o, end.Add(-time.Minute), end)
	})
	if err != nil {
		t.Fatalf("a group with no profile is not a failure: %v\n%s", err, out)
	}
	if strings.Contains(out, "INCOMPLETE") || strings.Contains(out, "FAILED") {
		t.Errorf("no query failed, so no banner:\n%s", out)
	}
	if !strings.Contains(out, "no samples in this window") {
		t.Errorf("the empty group should be counted and omitted:\n%s", out)
	}
	if !strings.Contains(out, "20.0 MiB") {
		t.Errorf("the group that has data should still be reported:\n%s", out)
	}
}

// The bug this guards: a merge sums across BOTH time and series, and fetching
// one profile fixes the first while breaking the second. A selector usually
// matches several series, each written at its own instant, so one instant
// returns one series. Measured: --by=job over eight scrape targets reported
// 3.0 MiB while a single instance in the same period was 18 MiB -- a total
// smaller than one of its parts.
func TestSnapshotSumsAcrossSeriesNotAcrossTime(t *testing.T) {
	// Three series, each scraped once a minute, each holding 10 MiB. The
	// group total must be 30 MiB whatever the window -- never 10 (one
	// series) and never 30 x scrapes.
	f := reportFixture(t)
	f.types = profileTypesFrom([]string{heapType})
	f.multiSeries = 3
	f.merges[heapType+`{cluster="tc"}`] = heapProfile(t, 10<<20)
	f.merges[heapType] = heapProfile(t, 10<<20)

	o := testOptions()
	o.profileType, o.top = heapType, 0

	// The three targets are staggered across the scrape interval, as Parca
	// spreads them, so the window has to reach back past the oldest of the
	// three newest scrapes for all of them to be in it at all.
	for _, window := range []time.Duration{2 * time.Minute, 20 * time.Minute} {
		end := snapshotTime.Add(10 * time.Second)
		out := captureStdout(t, func() {
			if err := report(context.Background(), testClient(f, time.Minute), o,
				end.Add(-window), end); err != nil {
				t.Fatal(err)
			}
		})
		if !strings.Contains(out, "30.0 MiB") {
			t.Errorf("window %s: want 30.0 MiB (3 series x 10 MiB), got:\n%s", window, out)
		}
	}
}

// Every group and the total have to describe the same moment. Letting each
// derive its own window gave rows summing to 150% of a total smaller than its
// own parts: an instance that stopped scraping was reported at its last known
// heap while the total, correctly, left it out.
func TestSnapshotGroupsAndTotalShareOneWindow(t *testing.T) {
	f := reportFixture(t)
	f.types = profileTypesFrom([]string{heapType})
	f.multiSeries = 3
	for _, g := range []string{"tc", "vps"} {
		f.merges[heapType+`{cluster="`+g+`"}`] = heapProfile(t, 10<<20)
	}
	f.merges[heapType] = heapProfile(t, 20<<20)

	o := testOptions()
	o.profileType, o.top = heapType, 0
	end := snapshotTime.Add(10 * time.Second)
	if err := report(context.Background(), testClient(f, time.Minute), o,
		end.Add(-20*time.Minute), end); err != nil {
		t.Fatal(err)
	}

	f.mergeMu.Lock()
	defer f.mergeMu.Unlock()
	if len(f.mergeWindows) < 3 {
		t.Fatalf("expected a merge per group plus the unfiltered one, got %d", len(f.mergeWindows))
	}
	first := f.mergeWindows[0]
	for _, w := range f.mergeWindows[1:] {
		if !w[0].Equal(first[0]) || !w[1].Equal(first[1]) {
			t.Fatalf("merges used different windows: %v and %v -- rows and total would describe different moments",
				first, w)
		}
	}
}

// A series the window missed contributes nothing to the total. Letting it
// vanish silently is the failure this whole change is about, one level down.
func TestSeriesTheWindowMissedAreCounted(t *testing.T) {
	f := reportFixture(t)
	f.types = profileTypesFrom([]string{heapType})
	// Five targets staggered across a 60s interval, but the window is only
	// one interval wide, and the fixture's oldest targets sit outside it.
	f.multiSeries = 5
	// Two of them stopped scraping a few intervals ago, so a one-interval
	// window cannot reach them.
	f.laggingSeries = 2
	f.merges[heapType+`{cluster="tc"}`] = heapProfile(t, 10<<20)
	f.merges[heapType] = heapProfile(t, 10<<20)

	o := testOptions()
	o.profileType, o.top = heapType, 0
	end := snapshotTime.Add(10 * time.Second)
	out := captureStdout(t, func() {
		if err := report(context.Background(), testClient(f, time.Minute), o,
			end.Add(-20*time.Minute), end); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(out, "2 of the series matched had no scrape inside") {
		t.Errorf("the two stopped series should be counted, not dropped silently:\n%s", out)
	}
}

// The interval is a median, so a series that really did scrape twice inside
// one median gap gets counted twice. Rare, but the number is then wrong, and
// a wrong number that looks right is the thing this tool refuses.
func TestSeriesCountedTwiceAreSaidOutLoud(t *testing.T) {
	f := reportFixture(t)
	f.types = profileTypesFrom([]string{heapType})
	f.multiSeries = 3
	// One of them scraped again five seconds after its newest.
	f.extraScrape = 1
	f.merges[heapType+`{cluster="tc"}`] = heapProfile(t, 10<<20)
	f.merges[heapType] = heapProfile(t, 10<<20)

	o := testOptions()
	o.profileType, o.top = heapType, 0
	end := snapshotTime.Add(10 * time.Second)
	out := captureStdout(t, func() {
		if err := report(context.Background(), testClient(f, time.Minute), o,
			end.Add(-20*time.Minute), end); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(out, "1 of the series matched had more than one scrape") {
		t.Errorf("the doubled series should be named, not folded into the total:\n%s", out)
	}
	// Three series at 10 MiB with one of them folded twice: 40 MiB, not 30.
	// Without this the note could be printed over a total that was never
	// doubled, which is what the fake used to do.
	if !strings.Contains(out, "40.0 MiB") {
		t.Errorf("the doubled series should actually be in the total twice:\n%s", out)
	}
}

// A window that reached none of the series is not an idle cluster, and the
// two have to look different. This is the run the counting exists for.
func TestACollapsedWindowStillNamesTheMissedSeries(t *testing.T) {
	f := reportFixture(t)
	f.types = profileTypesFrom([]string{heapType})
	f.multiSeries = 3
	// Two of the three stopped scraping a few intervals ago, so the window
	// cannot reach them. No group has anything, which is the page this test
	// is about -- and the two missed series are why.
	f.laggingSeries = 2
	f.merges[heapType] = heapProfile(t, 10<<20)

	o := testOptions()
	o.profileType, o.top = heapType, 0
	end := snapshotTime.Add(10 * time.Second)
	out := captureStdout(t, func() {
		_ = report(context.Background(), testClient(f, time.Minute), o,
			end.Add(-20*time.Minute), end)
	})
	if !strings.Contains(out, "no scrape inside") {
		t.Errorf("an empty report must still say the window missed the series:\n%s", out)
	}
	if !strings.Contains(out, "newest scrape per series") {
		t.Errorf("the heading should still say what was being asked for:\n%s", out)
	}
}

func TestLatestAndInterval(t *testing.T) {
	base := time.Date(2026, 9, 10, 2, 0, 0, 0, time.UTC)
	// Two series, 60s apart within each, offset from one another -- which is
	// what different scrape targets look like.
	times := [][]time.Time{
		{base, base.Add(60 * time.Second), base.Add(120 * time.Second)},
		{base.Add(7 * time.Second), base.Add(67 * time.Second)},
	}
	latest, interval := latestAndInterval(times)
	if want := base.Add(120 * time.Second); !latest.Equal(want) {
		t.Errorf("latest = %v, want %v", latest, want)
	}
	if interval != 60*time.Second {
		t.Errorf("interval = %v, want 60s", interval)
	}

	// A gap in the middle must not widen the estimate, or the window would
	// pull in two scrapes per series.
	gappy := [][]time.Time{{base, base.Add(60 * time.Second), base.Add(600 * time.Second)}}
	if _, iv := latestAndInterval(gappy); iv != 60*time.Second {
		t.Errorf("interval = %v, want 60s: one long gap must not widen the estimate", iv)
	}

	// One close-together pair -- a restart, a backfill, a scrape that ran
	// early -- must not collapse the window for the whole fleet. Taking the
	// smallest gap outright cut a three-target total to one target.
	outlier := [][]time.Time{
		{base, base.Add(5 * time.Second), base.Add(65 * time.Second), base.Add(125 * time.Second)},
	}
	if _, iv := latestAndInterval(outlier); iv != 60*time.Second {
		t.Errorf("interval = %v, want 60s: one short gap should not set the window", iv)
	}

	// A newly-started target, or one that just restarted, has two timestamps
	// close together: one gap, so its own median IS that gap. Taking the
	// smallest of the per-series medians let it collapse the window for
	// everyone else.
	newTarget := [][]time.Time{
		{base, base.Add(60 * time.Second), base.Add(120 * time.Second)},
		{base.Add(3 * time.Second), base.Add(63 * time.Second), base.Add(123 * time.Second)},
		{base.Add(115 * time.Second), base.Add(120 * time.Second)},
	}
	if _, iv := latestAndInterval(newTarget); iv != 60*time.Second {
		t.Errorf("interval = %v, want 60s: one short series should not set the fleet's window", iv)
	}

	// One profile per series: nothing to infer, and the caller then merges
	// the window, which already holds at most one profile per series.
	single := [][]time.Time{{base}, {base.Add(3 * time.Second)}}
	if _, iv := latestAndInterval(single); iv != 0 {
		t.Errorf("interval = %v, want 0 with nothing to measure", iv)
	}
}
