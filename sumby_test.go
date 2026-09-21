package main

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

// The cheap breakdown and the fan-out must produce the same table.
//
// This is the assertion the whole change rests on. A range query answers in
// raw sample values -- a count of stacks for a CPU profile -- and knows
// nothing about sampling periods, so the numbers only become cores by being
// scaled with a factor taken from the unfiltered merge. If that arithmetic is
// wrong the report is a hundred times faster and silently wrong, which is
// worse than slow.
func TestCheapBreakdownAgreesWithTheFanOut(t *testing.T) {
	build := func() *fakeQuery {
		f := reportFixture(t)
		f.values["cluster"] = []string{"tc", "vps", "p16"}
		f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
		f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
		f.merges[testType+`{cluster="p16"}`] = cpuProfile(t, 25)
		// 210, not 200: a residual of exactly 25 would tie with p16, and a tie
		// tests the sort rather than the arithmetic.
		f.merges[testType] = cpuProfile(t, 210)
		return f
	}

	run := func(noSumBy bool) (*reportData, *fakeQuery) {
		t.Helper()
		f := build()
		f.noSumBy = noSumBy
		o := testOptions()
		o.insecure = true
		o.top = 5
		end := time.Now()
		d, err := gatherReport(context.Background(), testClient(f, time.Minute), o, end.Add(-time.Hour), end)
		if err != nil {
			t.Fatalf("noSumBy=%v: %v", noSumBy, err)
		}
		return d, f
	}

	cheap, cheapFake := run(false)
	slow, slowFake := run(true)

	if cheapFake.rangeCalls.Load() != 1 {
		t.Errorf("the cheap path should ask exactly one range query, asked %d", cheapFake.rangeCalls.Load())
	}
	if slowFake.rangeCalls.Load() != 0 {
		t.Errorf("the fallback should ask no sum_by at all after it is refused once, asked %d",
			slowFake.rangeCalls.Load())
	}

	// The point of the change: no per-group merges at all on the cheap path.
	for _, g := range []string{"tc", "vps", "p16"} {
		sel := testType + `{cluster="` + g + `"}`
		if n := cheapFake.callsFor(sel); n != 0 {
			t.Errorf("the cheap path merged %s %d times; it should merge none", g, n)
		}
		if n := slowFake.callsFor(sel); n == 0 {
			t.Errorf("the fallback should have merged %s", g)
		}
	}

	if len(cheap.Groups) != len(slow.Groups) {
		t.Fatalf("different rows: cheap %d, fan-out %d", len(cheap.Groups), len(slow.Groups))
	}
	for i := range cheap.Groups {
		a, b := cheap.Groups[i], slow.Groups[i]
		if a.Name != b.Name {
			t.Fatalf("row %d: cheap %q, fan-out %q", i, a.Name, b.Name)
		}
		// Exactly, not approximately: both are scaled from the same integers
		// by the same factor, so anything else is an arithmetic difference
		// rather than floating-point noise.
		if math.Abs(a.Value-b.Value) > 1e-9 {
			t.Errorf("row %q: cheap %g, fan-out %g", a.Name, a.Value, b.Value)
		}
	}
	if cheap.Unit != slow.Unit || cheap.Rate != slow.Rate {
		t.Errorf("unit drifted: cheap %s/%v, fan-out %s/%v", cheap.Unit, cheap.Rate, slow.Unit, slow.Rate)
	}
	if cheap.Total == nil || slow.Total == nil || math.Abs(*cheap.Total-*slow.Total) > 1e-9 {
		t.Errorf("totals differ: %v vs %v", cheap.Total, slow.Total)
	}
}

// A server that cannot sum_by must still produce a report. The fallback is not
// a nicety: an older Parca, or one that errors on the range query, would
// otherwise lose the breakdown entirely rather than merely take longer.
func TestBreakdownFallsBackWhenSumByIsRefused(t *testing.T) {
	f := reportFixture(t)
	f.noSumBy = true
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 150)

	o := testOptions()
	o.insecure = true
	end := time.Now()
	d, err := gatherReport(context.Background(), testClient(f, time.Minute), o, end.Add(-time.Hour), end)
	if err != nil {
		t.Fatalf("a refused sum_by must not fail the run: %v", err)
	}
	if len(d.Groups) != 2 {
		t.Fatalf("want both groups from the fallback, got %d", len(d.Groups))
	}
	// And the run is not marked incomplete: nothing was lost, it merely cost
	// more. Reporting a shortfall here would train the reader to ignore them.
	if !d.Complete {
		t.Errorf("falling back is not a shortfall: %+v", d.Failed)
	}
}

// A snapshot profile keeps the fan-out on purpose. The merge path puts every
// group on ONE window so the rows and the total describe the same instant; a
// range query picks each series' newest sample independently and would undo
// exactly that.
func TestSnapshotProfilesKeepTheFanOut(t *testing.T) {
	f := reportFixture(t)
	f.types = profileTypesFrom([]string{heapType})
	f.values["cluster"] = []string{"tc", "vps"}
	f.merges[heapType+`{cluster="tc"}`] = heapProfile(t, 100)
	f.merges[heapType+`{cluster="vps"}`] = heapProfile(t, 50)
	f.merges[heapType] = heapProfile(t, 150)

	o := testOptions()
	o.insecure = true
	o.profileType = heapType
	end := time.Now()
	d, err := gatherReport(context.Background(), testClient(f, time.Minute), o, end.Add(-time.Hour), end)
	if err != nil {
		t.Fatalf("gatherReport: %v", err)
	}
	if n := f.rangeCalls.Load(); n != 0 {
		t.Errorf("a snapshot profile must not be priced with sum_by, asked %d", n)
	}
	if n := f.callsFor(heapType + `{cluster="tc"}`); n == 0 {
		t.Error("a snapshot profile should still merge each group")
	}
	if len(d.Groups) == 0 {
		t.Error("no groups")
	}
}

// The unfiltered merge is now the first thing a report does, and everything
// else is built on it: the denominator, the residual, the function table, and
// -- since the breakdown is priced from its metadata -- the cheap path too.
// It gets one retry on a dropped stream, exactly as a group does.
//
// It could not fail transiently in a test before this: failFirstN deliberately
// skips it and failSel/mergeErrs fail every attempt, so the retry shipped with
// no coverage at all.
func TestTheUnfilteredMergeIsRetried(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 150)
	f.failOverallFirstN = 1

	o := testOptions()
	o.insecure = true
	end := time.Now()
	d, err := gatherReport(context.Background(), testClient(f, time.Minute), o, end.Add(-time.Hour), end)
	if err != nil {
		t.Fatalf("one dropped stream should be re-asked, not fatal: %v", err)
	}
	if d.Total == nil {
		t.Fatal("the retry should have recovered the denominator")
	}
	if d.RetriedOverall != 1 {
		t.Errorf("the re-ask should be recorded, got %d", d.RetriedOverall)
	}
	// Counted apart from the group retries, which are rendered as
	// "N <label> queries were asked again" -- the unfiltered merge carries no
	// matcher and is not one of the groups.
	if d.Retried != 0 {
		t.Errorf("the unfiltered merge must not be counted as a group retry, got %d", d.Retried)
	}
	// And it still took the cheap path afterwards.
	if d.Breakdown != "sum_by" {
		t.Errorf("breakdown = %q, want sum_by", d.Breakdown)
	}
}

// Two drops in a row is not transient any more. The run keeps going -- the
// breakdown is what matters and the fan-out can still produce it -- but
// without a denominator there are no percentages and no notes.
func TestTheUnfilteredMergeIsRetriedOnlyOnce(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 150)
	f.failOverallFirstN = 2

	o := testOptions()
	o.insecure = true
	end := time.Now()
	d, _ := gatherReport(context.Background(), testClient(f, time.Minute), o, end.Add(-time.Hour), end)
	if d == nil {
		t.Fatal("the breakdown should survive a lost unfiltered merge")
	}
	if d.Total != nil {
		t.Error("without the merge there is no denominator to report")
	}
	if len(d.Groups) != 2 {
		t.Errorf("the fan-out should still have produced the rows, got %d", len(d.Groups))
	}
	if d.Breakdown != "merge" {
		t.Errorf("with no metadata to scale by, the breakdown must fall back: %q", d.Breakdown)
	}
}

// The range query and the merge must be shown to have seen the same data.
//
// This is the check that stands in for everything the range API does not
// promise: that a bucket holding several profiles carries their sum, that
// nothing was truncated, that the server summed the same sample-type column
// the merge summed. None of that is knowable from the protocol, and all of it
// would otherwise be a silently wrong number rather than a slow one.
func TestCheapBreakdownRefusesWhenTheTwoQueriesDisagree(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 150)
	// The server answers the range query with less than it merged, the way a
	// truncated or downsampled response would.
	f.sumByScale = 0.5

	o := testOptions()
	o.insecure = true
	end := time.Now()
	d, err := gatherReport(context.Background(), testClient(f, time.Minute), o, end.Add(-time.Hour), end)
	if err != nil {
		t.Fatalf("a disagreement should fall back, not fail: %v", err)
	}
	if d.Breakdown != "merge" {
		t.Errorf("breakdown = %q, want the fan-out after the check refused", d.Breakdown)
	}
	// And the numbers are the fan-out's, not the short ones.
	if len(d.Groups) < 2 {
		t.Fatalf("want the full breakdown, got %d rows", len(d.Groups))
	}
	if d.Groups[0].Value <= 0 {
		t.Error("the fallback should have produced real values")
	}
}

// A point the server says folded several profiles together is refused too: the
// API documents Count alongside Value but never says an aggregated Value is
// their sum, and adding them up assumes exactly that.
func TestCheapBreakdownRefusesAggregatedPoints(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 150)
	f.sumByCount = 4

	o := testOptions()
	o.insecure = true
	end := time.Now()
	d, err := gatherReport(context.Background(), testClient(f, time.Minute), o, end.Add(-time.Hour), end)
	if err != nil {
		t.Fatalf("an aggregated point should fall back, not fail: %v", err)
	}
	if d.Breakdown != "merge" {
		t.Errorf("breakdown = %q, want the fan-out", d.Breakdown)
	}
}

// --fan-out forces the expensive path. It is the only way to price each group
// from its own profile, which is the cross-check that can reveal a fleet whose
// agents sample at different frequencies -- something a shared scale factor
// cannot show, because the rows are scaled to agree with the total whatever
// the factor is.
func TestFanOutFlagForcesTheMergePath(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 150)

	o := testOptions()
	o.insecure = true
	o.fanOut = true
	end := time.Now()
	d, err := gatherReport(context.Background(), testClient(f, time.Minute), o, end.Add(-time.Hour), end)
	if err != nil {
		t.Fatalf("gatherReport: %v", err)
	}
	if d.Breakdown != "merge" {
		t.Errorf("breakdown = %q, want merge under --fan-out", d.Breakdown)
	}
	if n := f.rangeCalls.Load(); n != 0 {
		t.Errorf("--fan-out should ask no range query at all, asked %d", n)
	}
	if n := f.callsFor(testType + `{cluster="tc"}`); n == 0 {
		t.Error("--fan-out should merge each group")
	}
}

// A value the breakdown returned that the label list did not have is counted,
// not folded into the residual. Folding it would make the report say that CPU
// carries no label, when the server just told us its name.
func TestValuesOutsideTheLabelListAreCounted(t *testing.T) {
	f := reportFixture(t)
	f.values["cluster"] = []string{"tc"} // vps exists in the data, not in the list
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 150)

	o := testOptions()
	o.insecure = true
	end := time.Now()
	d, err := gatherReport(context.Background(), testClient(f, time.Minute), o, end.Add(-time.Hour), end)
	if err != nil {
		t.Fatalf("gatherReport: %v", err)
	}
	if d.Breakdown != "sum_by" {
		t.Fatalf("this test is about the cheap path, got %q", d.Breakdown)
	}
	if d.DroppedGroups != 1 {
		t.Errorf("the value outside the list should be counted, got %d", d.DroppedGroups)
	}
	// And the note about the residual must not claim all of it is unlabelled.
	var found bool
	for _, n := range d.Notes {
		if n.Code == "unlabeled_large" {
			found = true
			if !strings.Contains(n.Message, "not all unlabelled") {
				t.Errorf("the note overstates what the residual is:\n%s", n.Message)
			}
		}
	}
	if !found {
		t.Errorf("a third of the total in the residual should be noted, got %v", noteCodes(d.Notes))
	}
}

// A small difference between the two queries is normal and must not cost a
// fan-out.
//
// They are separate calls, and against a window still being written profiles
// for it keep arriving between one and the next. Measured against a real
// server, two calls seconds apart differed by 1.1% -- and an earlier version
// of this code refused that and merged 1486 values instead, which is the
// disaster this feature exists to prevent, triggered by a rounding difference.
//
// The rows are the range query's own proportions carried onto the total the
// merge measured, so they still sum to it exactly.
func TestSmallDisagreementIsNormalisedNotRefused(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 150)
	// The range query saw 2% more than the merge, as a late scrape would.
	f.sumByScale = 1.02

	o := testOptions()
	o.insecure = true
	end := time.Now()
	d, err := gatherReport(context.Background(), testClient(f, time.Minute), o, end.Add(-time.Hour), end)
	if err != nil {
		t.Fatalf("gatherReport: %v", err)
	}
	if d.Breakdown != "sum_by" {
		t.Fatalf("a 2%% difference should be absorbed, not fall back: %q", d.Breakdown)
	}
	if d.Total == nil {
		t.Fatal("no total")
	}
	// The rows still add up to the measured total: none of the 2% leaked in.
	var sum float64
	for _, g := range d.Groups {
		sum += g.Value
	}
	if math.Abs(sum-*d.Total) > *d.Total*1e-9 {
		t.Errorf("rows sum to %g, total is %g: the difference leaked into the rows", sum, *d.Total)
	}
	// And the proportions are the range query's, which are unchanged by a
	// uniform scale: tc twice vps.
	byName := map[string]float64{}
	for _, g := range d.Groups {
		byName[g.Name] = g.Value
	}
	if r := byName["tc"] / byName["vps"]; math.Abs(r-2) > 1e-9 {
		t.Errorf("tc/vps = %g, want 2", r)
	}
}
