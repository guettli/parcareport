package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// An overview sweeps CPU and live heap. On a server offering eleven profile
// types that is two of them, and the header saying "11 profile types" reads as
// coverage rather than as a menu it sampled twice.
//
// docs/bottlenecks.md has always warned that "the overview found nothing" says
// nothing about contention, blocking, goroutine leaks, allocation churn or
// off-CPU waits. The doc said it; the tool did not.
func TestOverviewNamesEveryTypeItDidNotLookAt(t *testing.T) {
	f := overviewFixture(t)
	f.types = profileTypesFrom(realServerTypes)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 150)
	f.merges[heapType+`{instance="10.0.0.1:6060"}`] = heapProfile(t, 1<<30)
	f.merges[heapType] = heapProfile(t, 1<<30)

	o := overviewOptions()
	o.top, o.output = 0, outputJSON
	var err error
	out := captureStdout(t, func() {
		err = overview(context.Background(), testClient(f, time.Minute), o,
			time.Now().Add(-time.Hour), time.Now())
	})
	if err != nil {
		t.Fatalf("overview: %v", err)
	}
	d := decodeOverview(t, out)

	analyzed := map[string]bool{}
	for _, s := range d.Sections {
		analyzed[s.ProfileType] = true
	}
	named := map[string]bool{}
	for _, s := range d.Skipped {
		named[s.What] = true
	}
	for _, ty := range realServerTypes {
		if analyzed[ty] || named[ty] {
			continue
		}
		t.Errorf("profile type %q was neither analyzed nor declared unanalyzed", ty)
	}
	if len(analyzed) == 0 {
		t.Fatal("the fixture should have analyzed something")
	}
}

// A hint naming a label the series do not carry is worse than none: it reports
// an empty window for a profile with plenty, which is the conflation of
// "absent label" with "no data" this tool refuses everywhere else. The profile
// name says which tier wrote it, so the label is read rather than guessed.
func TestSuggestedBreakdownLabelMatchesTheProfilesTier(t *testing.T) {
	labels := []string{"cluster", "comm", "instance", "job", "namespace", "node"}
	// node is suggestible even though overviewBreakdowns omits it: the agent
	// tier always carries it.
	if got := breakdownLabelFor("parca_agent:samples:count:cpu:nanoseconds:delta", []string{"node"}); got != "node" {
		t.Errorf("agent profiles should fall back to node, got %q", got)
	}
	tests := []struct {
		profType string
		want     string
	}{
		// parca-agent tags its own profiles with the fleet labels.
		{"parca_agent:samples:count:cpu:nanoseconds:delta", "cluster"},
		// Off-CPU is the exception. docs/bottlenecks.md says a wallclock total
		// is mostly idleness and only the stacks are worth judging, so a
		// two-row cluster table is the shape it warns against -- comm at
		// least names what to look inside.
		{"parca_agent:wallclock:nanoseconds:samples:count:delta", "comm"},
		// Everything scraped from /debug/pprof carries job and instance only.
		{"goroutine:goroutine:count:goroutine:count", "instance"},
		{"memory:inuse_space:bytes:space:bytes", "instance"},
		{"mutex:delay:nanoseconds:contentions:count", "instance"},
		{"block:delay:nanoseconds:contentions:count", "instance"},
		{"process_cpu:samples:count:cpu:nanoseconds:delta", "instance"},
	}
	for _, tc := range tests {
		if got := breakdownLabelFor(tc.profType, labels); got != tc.want {
			t.Errorf("breakdownLabelFor(%q) = %q, want %q", tc.profType, got, tc.want)
		}
	}

	// A scrape-tier profile on a server with no instance or job label has
	// nothing to group it by, and gets no command rather than a broken one.
	if got := breakdownLabelFor("goroutine:goroutine:count:goroutine:count", []string{"cluster"}); got != "" {
		t.Errorf("want no label when the tier's labels are absent, got %q", got)
	}
	sk := unanalyzedTypes(overviewOptions(), []string{"goroutine:goroutine:count:goroutine:count"},
		map[string]bool{}, []string{"cluster"})
	if len(sk) != 1 {
		t.Fatalf("want one entry, got %d", len(sk))
	}
	if sk[0].Command != "" {
		t.Errorf("a command that cannot work must not be offered: %q", sk[0].Command)
	}
	if !strings.Contains(sk[0].Reason, "no label it carries") {
		t.Errorf("and the reason should say why: %q", sk[0].Reason)
	}
}

// The command is a field, not a clause. The reasons used to end "run
// `parcareport --by=comm` directly if you want it", which is prose to a person
// and a regex to an agent.
func TestSkipsCarryTheirCommandAsAField(t *testing.T) {
	used := map[string]bool{}
	labels := []string{"instance", "cluster"}
	o := overviewOptions()
	o.setFlags = map[string]bool{"url": true}
	o.addr = "parca.example:7070"
	o.insecure = true

	sk := unanalyzedTypes(o, []string{"mutex:delay:nanoseconds:contentions:count"}, used, labels)
	if len(sk) != 1 {
		t.Fatalf("want one entry, got %d", len(sk))
	}
	want := "parcareport --url parca.example:7070 " +
		"--profile-type mutex:delay:nanoseconds:contentions:count --by instance"
	if sk[0].Command != want {
		t.Errorf("command\n got %s\nwant %s", sk[0].Command, want)
	}
	// It carries the connection flags, so it runs against the same server.
	if !strings.Contains(sk[0].Command, "--url parca.example:7070") {
		t.Errorf("a command that cannot connect is not a suggestion: %q", sk[0].Command)
	}
	// And no command is buried in the prose any more.
	if strings.Contains(sk[0].Reason, "parcareport") {
		t.Errorf("the reason should not carry a command: %q", sk[0].Reason)
	}
}

// Two runs over the same window should print the same page. The server's type
// order is not stable.
func TestUnanalyzedTypesAreSorted(t *testing.T) {
	all := []string{"mutex:delay:nanoseconds:contentions:count", "block:delay:nanoseconds:contentions:count",
		"goroutine:goroutine:count:goroutine:count", "memory:alloc_space:bytes:space:bytes"}
	sk := unanalyzedTypes(overviewOptions(), all, map[string]bool{}, []string{"instance"})
	got := make([]string, len(sk))
	for i, s := range sk {
		got[i] = s.What
	}
	want := append([]string{}, got...)
	sort.Strings(want)
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("not sorted:\n got %v\nwant %v", got, want)
		}
	}
}

// Nine types each repeating the same sentence is a wall a reader skips. The
// grouping is a property of the page; the document keeps one entry per thing.
func TestSkippedPageGroupsRepeatedReasons(t *testing.T) {
	skipped := []skippedJSON{
		{What: "a", Reason: "same reason", Command: "cmd-a"},
		{What: "b", Reason: "same reason", Command: "cmd-b"},
		{What: "c", Reason: "its own reason"},
	}
	out := captureStdout(t, func() { printSkipped(skipped) })
	if n := strings.Count(out, "same reason"); n != 1 {
		t.Errorf("a shared reason should be printed once, printed %d times:\n%s", n, out)
	}
	for _, want := range []string{"a", "b", "cmd-a", "cmd-b", "its own reason", "c"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "2 of them") {
		t.Errorf("the count should say how many share it:\n%s", out)
	}
}

// A type the overview considered and declined already carries its own honest
// reason. Letting it fall through to the unswept list overwrote that with
// "overview sweeps CPU and live heap" -- false of exactly those types, and it
// printed the live-heap type as unswept two lines under a sentence naming live
// heap as swept.
func TestATypeIsNeverBothSweptAndUnswept(t *testing.T) {
	cases := []struct {
		name   string
		setup  func(*fakeQuery)
		absent string // the type that must not appear under "not swept"
	}{
		{
			name: "heap with no label to group it by",
			setup: func(f *fakeQuery) {
				f.types = profileTypesFrom([]string{testType, heapType})
				f.names = []string{"cluster"} // no instance or job
			},
			absent: heapType,
		},
		{
			name: "CPU with none of its breakdown labels",
			setup: func(f *fakeQuery) {
				f.types = profileTypesFrom([]string{testType, heapType})
				f.names = []string{"instance"} // no cluster/namespace/workload/comm
			},
			absent: testType,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := overviewFixture(t)
			tc.setup(f)
			o := overviewOptions()
			o.top, o.output = 0, outputJSON
			var err error
			out := captureStdout(t, func() {
				err = overview(context.Background(), testClient(f, time.Minute), o,
					time.Now().Add(-time.Hour), time.Now())
			})
			_ = err
			d := decodeOverview(t, out)
			// Not merely "not listed as not_swept": listed at ALL under its
			// own selector is the bug. Its skip is spelled in the words the
			// section uses -- "live heap", "CPU breakdowns" -- and a second
			// entry naming the raw type is the same thing said twice, with
			// the second copy giving a different reason.
			for _, s := range d.Skipped {
				if s.What == tc.absent {
					t.Errorf("%q already has its own skip entry; listing it again says the same "+
						"thing twice with a different reason: %+v", tc.absent, s)
				}
			}
			// And it really is accounted for somewhere.
			var accounted bool
			for _, s := range d.Skipped {
				if strings.Contains(strings.ToLower(s.Reason), "label") ||
					strings.Contains(s.What, "CPU") || strings.Contains(s.What, "heap") {
					accounted = true
				}
			}
			for _, sec := range d.Sections {
				if sec.ProfileType == tc.absent {
					accounted = true
				}
			}
			if !accounted {
				t.Errorf("%q vanished from the page entirely: %+v", tc.absent, d.Skipped)
			}
		})
	}
}

// The heading is grouped by reason, not by adjacency. Entries are appended
// from several places and sorted inside one of them, so two sharing a reason
// are not guaranteed to be neighbours.
func TestSkippedPageGroupsNonAdjacentReasons(t *testing.T) {
	skipped := []skippedJSON{
		{What: "a", Reason: "shared", Command: "cmd-a"},
		{What: "b", Reason: "its own"},
		{What: "c", Reason: "shared", Command: "cmd-c"},
	}
	out := captureStdout(t, func() { printSkipped(skipped) })
	if n := strings.Count(out, "shared"); n != 1 {
		t.Errorf("a shared reason should be printed once however the entries are ordered, got %d:\n%s", n, out)
	}
	// And the count is how many share it, not how many happened to be adjacent.
	if !strings.Contains(out, "2 of them") {
		t.Errorf("the count should be 2:\n%s", out)
	}
}

// A suggestion offered *because* the fan-out is expensive must re-run the
// fan-out. Without it the command answers a different question than the one
// that was refused -- the cheap single-query path.
func TestTheCostlySkipCarriesItsCommandAndTheFanOut(t *testing.T) {
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
	o.top, o.output, o.fanOut = 0, outputJSON, true
	var err error
	out := captureStdout(t, func() {
		err = overview(context.Background(), testClient(f, time.Minute), o,
			time.Now().Add(-time.Hour), time.Now())
	})
	_ = err
	d := decodeOverview(t, out)

	var found bool
	for _, s := range d.Skipped {
		if !strings.Contains(s.Reason, "more than --max-group-values") {
			continue
		}
		found = true
		if s.Kind != "too_costly" {
			t.Errorf("a cost refusal is a choice, not an outage: kind=%q", s.Kind)
		}
		if s.Command == "" {
			t.Fatalf("the skip should say how to get it anyway: %+v", s)
		}
		if !strings.Contains(s.Command, "--fan-out") {
			t.Errorf("the command must re-run what was refused, not the cheap path: %q", s.Command)
		}
		if !strings.Contains(s.Command, "--by comm") {
			t.Errorf("and name the label that was skipped: %q", s.Command)
		}
	}
	if !found {
		t.Fatalf("the cardinality skip should be recorded: %+v", d.Skipped)
	}
}

// A run's --match is written for the labels of the type it was run against.
// Copying it onto a different tier's profile selects nothing and reports an
// empty window for a profile with plenty.
func TestCrossTypeSuggestionsDoNotCarryTheMatch(t *testing.T) {
	o := overviewOptions()
	o.match = `cluster="tc"`
	sk := unanalyzedTypes(o, []string{"goroutine:goroutine:count:goroutine:count"},
		map[string]bool{}, []string{"instance", "cluster"})
	if len(sk) != 1 {
		t.Fatalf("want one entry, got %d", len(sk))
	}
	if strings.Contains(sk[0].Command, "--match") {
		t.Errorf("a matcher for another tier's labels must not be carried: %q", sk[0].Command)
	}
	// The same-type suggestion still carries it.
	if got := sectionCommand(o, testType, "comm", true); !strings.Contains(got, `--match 'cluster="tc"'`) {
		t.Errorf("a same-type suggestion should keep the matcher: %q", got)
	}
}

// Every flag that decides WHAT is asked reaches the suggestion.
func TestSectionCommandCarriesTheRunsFlags(t *testing.T) {
	o := overviewOptions()
	o.setFlags = map[string]bool{"url": true, "from": true, "to": true, "sort": true, "bearer-token-file": true}
	o.addr, o.from, o.to, o.sortBy, o.tokenFile = "p:7070", "-6h", "now", "cum", "/etc/tok"
	o.insecure = false

	got := sectionCommand(o, heapType, "instance", false)
	for _, want := range []string{
		"--url p:7070", "--insecure=false", "--bearer-token-file /etc/tok",
		"--profile-type " + heapType, "--from -6h", "--to now", "--sort cum", "--by instance",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
}

// The skips separate a choice from a failure, the same distinction `outcome`
// draws for the run as a whole.
func TestSkipKindSeparatesChoiceFromFailure(t *testing.T) {
	f := overviewFixture(t)
	f.types = profileTypesFrom(realServerTypes)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 150)
	f.merges[heapType+`{instance="10.0.0.1:6060"}`] = heapProfile(t, 1<<30)
	f.merges[heapType] = heapProfile(t, 1<<30)

	o := overviewOptions()
	o.top, o.output = 0, outputJSON
	var err error
	out := captureStdout(t, func() {
		err = overview(context.Background(), testClient(f, time.Minute), o,
			time.Now().Add(-time.Hour), time.Now())
	})
	if err != nil {
		t.Fatalf("overview: %v", err)
	}
	d := decodeOverview(t, out)
	if len(d.Skipped) == 0 {
		t.Fatal("nine types should be unswept")
	}
	for _, s := range d.Skipped {
		switch s.Kind {
		case "not_swept", "too_costly", "unavailable":
		default:
			t.Errorf("skip %q has no usable kind: %q", s.What, s.Kind)
		}
	}
	// Nothing failed here, so nothing should claim it did.
	for _, s := range d.Skipped {
		if s.Kind == "unavailable" {
			t.Errorf("nothing was unavailable in this run: %+v", s)
		}
	}
}
