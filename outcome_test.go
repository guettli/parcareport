package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/pprof/profile"
)

// headerFor decides the unit of a report with no rows in it. It gets the
// selector and nothing else, so it has to read the selector's PARTS: an
// earlier version matched substrings anywhere in the string and read
// `parca_agent:wallclock:nanoseconds:samples:count:delta` as CPU, because it
// contains ":samples:count:". An empty off-CPU report then announced its unit
// as cores -- which this tool warns elsewhere is emphatically wrong.
func TestHeaderForEveryTypeARealServerOffers(t *testing.T) {
	tests := []struct {
		selector string
		header   string
		rate     bool
	}{
		{"parca_agent:samples:count:cpu:nanoseconds:delta", "CORES", true},
		{"process_cpu:samples:count:cpu:nanoseconds:delta", "CORES", true},
		{"process_cpu:cpu:nanoseconds:cpu:nanoseconds:delta", "CORES", true},
		{"parca_agent:wallclock:nanoseconds:samples:count:delta", "BLOCKED", true},
		{"memory:inuse_space:bytes:space:bytes", "BYTES", false},
		{"memory:alloc_space:bytes:space:bytes:delta", "BYTES", false},
		{"memory:inuse_objects:count:space:bytes", "COUNT", false},
		{"memory:alloc_objects:count:space:bytes:delta", "COUNT", false},
		{"goroutine:goroutine:count:goroutine:count", "COUNT", false},
		{"mutex:delay:nanoseconds:contentions:count", "SECONDS", false},
		{"block:delay:nanoseconds:contentions:count", "SECONDS", false},
		{"mutex:contentions:count:contentions:count", "COUNT", false},
		{"block:contentions:count:contentions:count", "COUNT", false},
		// Not a selector at all: better a defensible default than a panic.
		{"nonsense", "COUNT", false},
	}
	for _, tc := range tests {
		h, r := headerFor(tc.selector)
		if h != tc.header || r != tc.rate {
			t.Errorf("headerFor(%q) = (%s, %v), want (%s, %v)", tc.selector, h, r, tc.header, tc.rate)
		}
		// "cores" that is not a rate is a contradiction, and an empty report
		// was emitting exactly that.
		if (h == "CORES" || h == "BLOCKED") && !r {
			t.Errorf("headerFor(%q): %s must be a rate", tc.selector, h)
		}
	}
}

// A window the server has nothing in at all -- no labels, not merely no
// samples -- is an answered question. Querying last month used to be
// indistinguishable from a server that had stopped responding.
func TestAWindowWithNoLabelsAtAllIsEmptyNotBroken(t *testing.T) {
	f := reportFixture(t)
	f.names = nil                    // the server has no labels in this window
	f.values = map[string][]string{} // ...and therefore no values

	var err error
	out := captureStdout(t, func() {
		err = report(context.Background(), testClient(f, time.Minute), jsonOptions(),
			time.Now().Add(-time.Hour), time.Now())
	})
	if err != nil {
		t.Fatalf("an answered query is not a failure: %v", err)
	}
	d := decodeReport(t, out)
	if d.Outcome != "empty" || !d.Complete {
		t.Errorf("outcome=%q complete=%v, want empty/true", d.Outcome, d.Complete)
	}
	// The unit is a property of the selector, so it is known even here -- and
	// Rate with it.
	if d.Unit != "cores" || !d.Rate {
		t.Errorf("unit=%q rate=%v, want cores/true", d.Unit, d.Rate)
	}
	if d.Breakdown != "none" {
		t.Errorf("breakdown=%q, want none: nothing was priced", d.Breakdown)
	}
	var codes []string
	for _, n := range d.Notes {
		codes = append(codes, n.Code)
		// The sentinel is how the code classifies this; it must not leak into
		// something a person reads.
		if strings.HasPrefix(n.Message, "empty window:") {
			t.Errorf("the error class leaked into the message: %q", n.Message)
		}
	}
	if len(codes) != 1 || codes[0] != "empty_window" {
		t.Errorf("want one empty_window note, got %v", codes)
	}
}

// Every document says which outcome it is, including the ones built before
// anything was gathered. The field is documented as the thing to switch on, so
// an empty string is worse than the behaviour it replaced.
func TestEveryErrorDocumentNamesItsOutcome(t *testing.T) {
	cases := []struct {
		name string
		opts func() options
	}{
		{"bad --sort", func() options { o := jsonOptions(); o.sortBy = "nope"; return o }},
		{"bad --by", func() options { o := jsonOptions(); o.by = "nosuchlabel"; return o }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := reportFixture(t)
			var err error
			out := captureStdout(t, func() {
				err = report(context.Background(), testClient(f, time.Minute), tc.opts(),
					time.Now().Add(-time.Hour), time.Now())
			})
			if err == nil {
				t.Fatal("want a non-zero exit")
			}
			d := decodeReport(t, out)
			if d.Outcome != "incomplete" {
				t.Errorf("outcome = %q, want incomplete", d.Outcome)
			}
			if d.Complete {
				t.Error("complete must be false")
			}
			if d.Error == "" {
				t.Error("the reason belongs in the document")
			}
			// The selector was given, so its unit is knowable even though
			// nothing was gathered.
			if d.Unit == "" {
				t.Error("unit is a property of the selector, not of the rows")
			}
		})
	}
}

// An overview whose label lookup FAILED must not claim it looked and found
// nothing. "empty" is documented as "every query was answered".
func TestOverviewDoesNotCallAFailedLookupEmpty(t *testing.T) {
	f := overviewFixture(t)
	f.types = profileTypesFrom([]string{testType})
	// Two labels: one whose lookup fails, one that works. With only the
	// failing one the plan comes out empty and a different branch sets the
	// outcome, so the test would pass without the fix.
	f.names = []string{"cluster", "comm"}
	f.values["comm"] = []string{"parca"}
	f.merges[testType+`{comm="parca"}`] = cpuProfile(t, 100)
	f.merges[testType] = cpuProfile(t, 100)
	f.valuesErrFor = map[string]error{"cluster": errors.New("stream terminated by RST_STREAM")}

	o := overviewOptions()
	o.top, o.output = 0, outputJSON
	var err error
	out := captureStdout(t, func() {
		err = overview(context.Background(), testClient(f, time.Minute), o,
			time.Now().Add(-time.Hour), time.Now())
	})
	if err == nil {
		t.Error("a failed lookup should exit non-zero")
	}
	d := decodeOverview(t, out)
	if d.Outcome == "empty" {
		t.Errorf("a query that was never answered must not read as empty:\n%s", out)
	}
	if d.Complete {
		t.Error("complete must be false when a lookup failed")
	}
}

// An overview that fails before it has anything to report must still emit a
// document. It used to print nothing at all under --output=json: an empty
// stdout and a non-zero exit, which is the silence this format refuses.
func TestOverviewEmitsADocumentWhenItFailsEarly(t *testing.T) {
	f := overviewFixture(t)
	f.namesErr = errors.New("boom")

	o := overviewOptions()
	o.top, o.output = 0, outputJSON
	var err error
	out := captureStdout(t, func() {
		err = overview(context.Background(), testClient(f, time.Minute), o,
			time.Now().Add(-time.Hour), time.Now())
	})
	if err == nil {
		t.Fatal("want a non-zero exit")
	}
	if strings.TrimSpace(out) == "" {
		t.Fatal("an empty stdout is exactly what the JSON format exists to prevent")
	}
	d := decodeOverview(t, out)
	if d.Outcome != "incomplete" {
		t.Errorf("outcome = %q, want incomplete", d.Outcome)
	}
	if d.Error == "" {
		t.Errorf("the reason belongs in the document:\n%s", out)
	}
}

// The page and the document must explain an empty result in the same words.
// The document said why and the page just said "(no data in this window)".
func TestTheEmptyPagePrintsItsNotes(t *testing.T) {
	f := reportFixture(t)
	out, err := runReport(t, f, testOptions())
	if err != nil {
		t.Fatalf("an idle window is not a failure: %v", err)
	}
	if !strings.Contains(out, "NOTES") || !strings.Contains(out, "empty_window") {
		t.Errorf("the page should carry the same explanation as the document:\n%s", out)
	}
}

// The remedy, not only the diagnosis. The advice for each class of failure
// existed only in the printed banner.
func TestFailuresCarryTheirHintIntoTheDocument(t *testing.T) {
	f := reportFixture(t)
	f.noSumBy = true
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType] = cpuProfile(t, 150)
	f.mergeErrs[testType+`{cluster="vps"}`] = errors.New("context deadline exceeded")

	o := jsonOptions()
	var err error
	out := captureStdout(t, func() {
		err = report(context.Background(), testClient(f, time.Minute), o,
			time.Now().Add(-time.Hour), time.Now())
	})
	if err == nil {
		t.Fatal("a failed group is a non-zero exit")
	}
	d := decodeReport(t, out)
	if len(d.Failed) == 0 {
		t.Fatal("no failures recorded")
	}
	var withHint int
	for _, f := range d.Failed {
		if f.Hint != "" {
			withHint++
			if strings.Contains(f.Hint, "!!") {
				t.Errorf("the banner markers are a convention of the page, not of the field: %q", f.Hint)
			}
		}
	}
	if withHint == 0 {
		t.Errorf("the advice should reach the document too: %+v", d.Failed)
	}
}

// "[unsymbolized]" is a display name sitting in the same field as real
// symbols. A consumer deciding whether a profile is attributable reads a flag.
func TestUnsymbolizedIsAFlagNotASpelling(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = unsymbolizedProfile(t, 150)

	o := jsonOptions()
	o.top = 5
	var err error
	out := captureStdout(t, func() {
		err = report(context.Background(), testClient(f, time.Minute), o,
			time.Now().Add(-time.Hour), time.Now())
	})
	if err != nil {
		t.Fatalf("gatherReport: %v", err)
	}
	d := decodeReport(t, out)
	var flagged, named int
	for _, fn := range d.Functions {
		if fn.Unsymbolized {
			flagged++
		}
		if fn.Name == "[unsymbolized]" {
			named++
		}
	}
	if named == 0 {
		t.Fatalf("the fixture should produce an unsymbolized bucket: %+v", d.Functions)
	}
	if flagged != named {
		t.Errorf("%d rows named [unsymbolized] but %d carry the flag", named, flagged)
	}
}

func decodeOverview(t *testing.T, out string) overviewData {
	t.Helper()
	var d overviewData
	if err := json.Unmarshal([]byte(out), &d); err != nil {
		t.Fatalf("output is not one JSON document: %v\n%s", err, out)
	}
	return d
}

// unsymbolizedProfile is a CPU profile whose frames have no function names,
// the shape a binary without debuginfo produces.
func unsymbolizedProfile(t *testing.T, samples int64) *profile.Profile {
	t.Helper()
	loc := &profile.Location{ID: 1, Address: 0xdeadbeef}
	return &profile.Profile{
		SampleType: []*profile.ValueType{{Type: "samples", Unit: "count"}},
		Period:     10_000_000,
		PeriodType: &profile.ValueType{Type: "cpu", Unit: "nanoseconds"},
		Location:   []*profile.Location{loc},
		Sample:     []*profile.Sample{{Location: []*profile.Location{loc}, Value: []int64{samples}}},
	}
}
