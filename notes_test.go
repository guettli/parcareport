package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// noteOpts is a run with nothing unusual set, so a suggested command carries
// only what it must.
func noteOpts() options {
	return options{by: "cluster", insecure: true, setFlags: map[string]bool{}}
}

func noteFixture(unit string, total float64) *reportData {
	return &reportData{
		ProfileType: "parca_agent:samples:count:cpu:nanoseconds:delta",
		GroupBy:     "cluster",
		Unit:        unit,
		Total:       &total,
		knowTotal:   true,
		header:      "CORES",
		window:      time.Hour,
	}
}

func noteCodes(notes []noteJSON) []string {
	out := make([]string, 0, len(notes))
	for _, n := range notes {
		out = append(out, n.Code)
	}
	return out
}

func hasCode(notes []noteJSON, code string) bool {
	for _, n := range notes {
		if n.Code == code {
			return true
		}
	}
	return false
}

// Each rule has to fire on the shape it is for and stay quiet either side of
// its threshold. A rule that fires on every report teaches the reader to skip
// the block, which is worse than not having it.
func TestNotesFireOnlyOnTheirShape(t *testing.T) {
	t.Run("unsymbolized", func(t *testing.T) {
		d := noteFixture("cores", 10)
		d.allFunctions = []funcJSON{{Name: unsymbolizedName, Cum: 6, Flat: 6}}
		if !hasCode(notesFor(noteOpts(), d), "unsymbolized_dominates") {
			t.Errorf("60%% unsymbolized should be called out, got %v", noteCodes(notesFor(noteOpts(), d)))
		}

		d.allFunctions = []funcJSON{{Name: unsymbolizedName, Cum: 4, Flat: 4}}
		if hasCode(notesFor(noteOpts(), d), "unsymbolized_dominates") {
			t.Error("40% unsymbolized is normal on a mixed fleet and must stay quiet")
		}

		// A profile with names is the ordinary case and must produce nothing.
		d.allFunctions = []funcJSON{{Name: "main.work", Cum: 9, Flat: 9}}
		if hasCode(notesFor(noteOpts(), d), "unsymbolized_dominates") {
			t.Error("a fully symbolized profile must not be flagged")
		}
	})

	t.Run("unlabeled", func(t *testing.T) {
		d := noteFixture("cores", 10)
		d.Groups = []groupJSON{{Name: "tc", Value: 8}, {Name: "(unlabeled)", Value: 2, Unlabeled: true}}
		if !hasCode(notesFor(noteOpts(), d), "unlabeled_large") {
			t.Error("a fifth of the fleet unattributable should be called out")
		}

		// Below the threshold this is rounding, and the row is already shown.
		d.Groups = []groupJSON{{Name: "tc", Value: 9.5}, {Name: "(unlabeled)", Value: 0.5, Unlabeled: true}}
		if hasCode(notesFor(noteOpts(), d), "unlabeled_large") {
			t.Error("a 5% residual is not a finding")
		}

		// The flag is what identifies the row, not its name: a genuine label
		// value spelled "(unlabeled)" must not trip it.
		d.Groups = []groupJSON{{Name: "(unlabeled)", Value: 8}}
		if hasCode(notesFor(noteOpts(), d), "unlabeled_large") {
			t.Error("only the residual row counts, and it is marked by the flag")
		}
	})

	t.Run("core ceiling", func(t *testing.T) {
		d := noteFixture("cores", 10)
		// A process-scoped label: one core is a claim about one runnable
		// thread, so it only means anything where a group can BE a process.
		d.GroupBy = "comm"
		d.Groups = []groupJSON{{Name: "tc", Value: 1.004}, {Name: "vps", Value: 8.996}}
		notes := notesFor(noteOpts(), d)
		if !hasCode(notes, "single_core_ceiling") {
			t.Errorf("a group at 1.004 cores should be called out, got %v", noteCodes(notes))
		}
		// An inference, and the note must not pretend otherwise -- the fact
		// that would settle it is not in any profile.
		for _, n := range notes {
			if n.Code != "single_core_ceiling" {
				continue
			}
			if !strings.Contains(n.Message, "proof of neither") {
				t.Errorf("the note must not overstate what a profile can show: %s", n.Message)
			}
		}

		d.Groups = []groupJSON{{Name: "tc", Value: 1.5}, {Name: "vps", Value: 8.5}}
		if hasCode(notesFor(noteOpts(), d), "single_core_ceiling") {
			t.Error("1.5 cores is not a one-core ceiling")
		}

		// A cluster at 1.000 CORES is thousands of processes summing to a
		// round number, which means nothing -- and on a report with dozens of
		// groups something lands in the band by chance.
		agg := noteFixture("cores", 10)
		agg.GroupBy = "cluster"
		agg.Groups = []groupJSON{{Name: "tc", Value: 1.0}}
		if hasCode(notesFor(noteOpts(), agg), "single_core_ceiling") {
			t.Error("an aggregate label has no one-thread reading")
		}

		// Bytes are not cores. The rule is not merely wrong on a heap
		// profile, it is not expressible.
		b := noteFixture("bytes", 10)
		b.GroupBy = "comm"
		b.Groups = []groupJSON{{Name: "tc", Value: 1.0}}
		if hasCode(notesFor(noteOpts(), b), "single_core_ceiling") {
			t.Error("a heap profile has no notion of cores")
		}
	})

	t.Run("gc overhead", func(t *testing.T) {
		d := noteFixture("cores", 10)
		d.allFunctions = []funcJSON{
			{Name: "runtime.gcBgMarkWorker", Flat: 2},
			{Name: "runtime.mallocgcSmallScanNoHeader", Flat: 1},
			{Name: "main.work", Flat: 7},
		}
		notes := notesFor(noteOpts(), d)
		if !hasCode(notes, "gc_overhead") {
			t.Errorf("30%% in the collector and allocator should be called out, got %v", noteCodes(notes))
		}
		for _, n := range notes {
			if n.Code == "gc_overhead" && n.Command == "" {
				t.Error("the note should say what to run next")
			}
		}

		d.allFunctions = []funcJSON{{Name: "runtime.gcBgMarkWorker", Flat: 1}, {Name: "main.work", Flat: 9}}
		if hasCode(notesFor(noteOpts(), d), "gc_overhead") {
			t.Error("10% GC is ordinary for a Go service")
		}

		// A frame that merely starts with "runtime." is not GC. This is the
		// case a looser prefix list would get wrong.
		d.allFunctions = []funcJSON{{Name: "runtime.memmove", Flat: 9}, {Name: "main.work", Flat: 1}}
		if hasCode(notesFor(noteOpts(), d), "gc_overhead") {
			t.Error("runtime.memmove is not garbage collection")
		}
	})
}

// Without a denominator there are no shares. Substituting the sum of the
// listed groups would quietly omit whatever the failed queries held, and every
// note would be a percentage of the wrong number.
func TestNotesStaySilentWithoutATotal(t *testing.T) {
	d := noteFixture("cores", 0)
	d.Total = nil
	d.allFunctions = []funcJSON{{Name: unsymbolizedName, Cum: 99, Flat: 99}}
	d.Groups = []groupJSON{{Name: "(unlabeled)", Value: 99, Unlabeled: true}}
	if n := notesFor(noteOpts(), d); len(n) != 0 {
		t.Errorf("no total means no shares, got %v", noteCodes(n))
	}
}

// Every anchor a note cites has to exist, or the tool sends its reader to the
// top of a long document. Anchors are explicit <a id> tags precisely so this
// can be checked rather than hoped for.
func TestNoteDocLinksResolve(t *testing.T) {
	doc, err := os.ReadFile("docs/bottlenecks.md")
	if err != nil {
		t.Fatalf("read bottlenecks.md: %v", err)
	}

	// Build one report that trips every rule at once, so the list of cited
	// anchors comes from the code rather than from a copy of it here.
	total := 10.0
	d := noteFixture("cores", total)
	// comm is process-scoped (so the ceiling rule applies) and carried by
	// every series (so a residual is anomalous) -- the one label all four
	// rules can fire on at once.
	d.GroupBy = "comm"
	d.allFunctions = []funcJSON{
		{Name: unsymbolizedName, Cum: 6, Flat: 6},
		{Name: "runtime.gcBgMarkWorker", Flat: 3},
	}
	d.Groups = []groupJSON{{Name: "tc", Value: 1.0}, {Name: "(unlabeled)", Value: 2, Unlabeled: true}}

	cited := map[string]bool{}
	notes := notesFor(noteOpts(), d)
	if len(notes) != 4 {
		t.Fatalf("want all four rules to fire, got %v", noteCodes(notes))
	}
	for _, n := range notes {
		if len(n.Docs) == 0 {
			t.Errorf("note %s cites no documentation", n.Code)
			continue
		}
		for _, link := range n.Docs {
			path, anchor, ok := strings.Cut(link, "#")
			if !ok {
				t.Errorf("note %s links %q with no anchor", n.Code, link)
				continue
			}
			if path != "docs/bottlenecks.md" {
				t.Errorf("note %s links outside the bottleneck doc: %s", n.Code, link)
			}
			if !strings.Contains(string(doc), `<a id="`+anchor+`">`) {
				t.Errorf("note %s cites %q, which does not exist in %s", n.Code, anchor, path)
			}
			cited[anchor] = true
		}
	}
}

// The notes have to reach a reader. Built by hand, every rule test above would
// pass with notesFor never called and printNotes never reached.
func TestNotesReachTheOutput(t *testing.T) {
	total := 10.0
	d := noteFixture("cores", total)
	d.Groups = []groupJSON{{Name: "tc", Value: 8}, {Name: "(unlabeled)", Value: 2, Unlabeled: true}}
	d.allFunctions = []funcJSON{{Name: unsymbolizedName, Cum: 9, Flat: 9}}
	d.Notes = notesFor(noteOpts(), d)

	table := captureStdout(t, func() { renderTable(d) })
	if !strings.Contains(table, "NOTES") {
		t.Errorf("renderTable printed no notes block:\n%s", table)
	}
	if !strings.Contains(table, "unsymbolized_dominates") {
		t.Errorf("renderTable dropped a note:\n%s", table)
	}
	if !strings.Contains(table, "docs/bottlenecks.md#symbolization") {
		t.Errorf("renderTable dropped the documentation link:\n%s", table)
	}

	out := captureStdout(t, func() {
		if err := renderJSON(d); err != nil {
			t.Fatalf("renderJSON: %v", err)
		}
	})
	var got struct {
		Notes []noteJSON `json:"notes"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("emitted invalid JSON: %v\n%s", err, out)
	}
	if !hasCode(got.Notes, "unsymbolized_dominates") || !hasCode(got.Notes, "unlabeled_large") {
		t.Errorf("JSON lost notes, got %v", noteCodes(got.Notes))
	}
}

// Notes are wrapped because a terminal is not infinitely wide, and the tables
// in this tool are already careful about that.
func TestNotesWrapToTheTerminal(t *testing.T) {
	long := strings.Repeat("word ", 60)
	out := captureStdout(t, func() {
		printNotes([]noteJSON{{Code: "x", Message: long,
			Docs:    []string{"docs/bottlenecks.md#coverage"},
			Command: "parcareport " + strings.Repeat("--flag value ", 12)}})
	})
	for _, line := range strings.Split(out, "\n") {
		if len(line) > noteWidth+4 {
			t.Errorf("line of %d bytes is too wide for a terminal: %q", len(line), line)
		}
	}
}

// gatherReport must derive the notes. Everything above builds reportData by
// hand and so would pass with notesFor never called -- the same way the
// drill-down tests would have passed with that feature switched off.
func TestGatherReportDerivesNotes(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	// The unfiltered merge is larger than the groups together, so a quarter of
	// the fleet carries no cluster label at all.
	f.merges[testType] = cpuProfile(t, 200)

	o := testOptions()
	o.insecure = true
	o.top = 5

	end := time.Now()
	d, err := gatherReport(context.Background(), testClient(f, time.Minute), o, end.Add(-time.Hour), end)
	if err != nil {
		t.Fatalf("gatherReport: %v", err)
	}
	if !hasCode(d.Notes, "unlabeled_large") {
		t.Fatalf("a quarter of the total unattributable should be noted, got %v", noteCodes(d.Notes))
	}
	for _, n := range d.Notes {
		if n.Message == "" {
			t.Errorf("note %s has no message", n.Code)
		}
	}
}

// A large residual under a pod-scoped label is the normal state of a node --
// kernel threads, the kubelet, sshd, anything on the host. Flagging it would
// fire on nearly every `--by=namespace` run, and this tool's own README says
// the row is "expected and correct" there. A note contradicting the
// documentation is worse than no note.
func TestUnlabeledIsNotAFindingForPodScopedLabels(t *testing.T) {
	for _, by := range []string{"namespace", "workload", "container", "workload_kind", "pod"} {
		d := noteFixture("cores", 10)
		d.GroupBy = by
		d.Groups = []groupJSON{{Name: "x", Value: 4}, {Name: "(unlabeled)", Value: 6, Unlabeled: true}}
		if hasCode(notesFor(noteOpts(), d), "unlabeled_large") {
			t.Errorf("--by=%s: a large residual is every process outside a pod, not a finding", by)
		}
	}

	// A label every series ought to carry is a different matter.
	for _, by := range []string{"cluster", "node", "comm", "instance"} {
		d := noteFixture("cores", 10)
		d.GroupBy = by
		d.Groups = []groupJSON{{Name: "x", Value: 4}, {Name: "(unlabeled)", Value: 6, Unlabeled: true}}
		if !hasCode(notesFor(noteOpts(), d), "unlabeled_large") {
			t.Errorf("--by=%s: every series should carry this, so a residual is anomalous", by)
		}
	}
}

// The residual is the measured total minus the groups that came back, so a
// group whose query FAILED is inside it. Saying that time "carries no label"
// would be a plain misstatement.
func TestUnlabeledNoteOwnsUpToFailedGroups(t *testing.T) {
	d := noteFixture("cores", 10)
	d.Groups = []groupJSON{{Name: "tc", Value: 4}, {Name: "(unlabeled)", Value: 6, Unlabeled: true}}
	d.Failed = []failJSON{{Group: "vps", Error: "deadline exceeded"}}

	notes := notesFor(noteOpts(), d)
	for _, n := range notes {
		if n.Code != "unlabeled_large" {
			continue
		}
		if !strings.Contains(n.Message, "not all unlabelled") {
			t.Errorf("with failed groups the note must not present the residual as fact:\n%s", n.Message)
		}
		return
	}
	t.Fatalf("expected the note to fire, got %v", noteCodes(notes))
}

// --top asks for a shorter function TABLE. It says nothing about whether the
// question "can this profile be attributed at all" should be asked, and the
// rule that answers it must not be silenced by the display flag.
func TestNotesSurviveTopTruncation(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 200)

	for _, top := range []int{0, 1, 15} {
		o := testOptions()
		o.insecure = true
		o.top = top

		end := time.Now()
		d, err := gatherReport(context.Background(), testClient(f, time.Minute), o, end.Add(-time.Hour), end)
		if err != nil {
			t.Fatalf("--top=%d: %v", top, err)
		}
		if len(d.allFunctions) == 0 {
			t.Errorf("--top=%d: the rules have no function data to read", top)
		}
		if top == 0 && len(d.Functions) != 0 {
			t.Errorf("--top=0 must still print no function table, got %d", len(d.Functions))
		}
		if !hasCode(d.Notes, "unlabeled_large") {
			t.Errorf("--top=%d: notes should not depend on the table size, got %v", top, noteCodes(d.Notes))
		}
	}
}

// An absent finding and an uncomputed one are different claims, which is why
// this struct avoids omitempty. `null` would read as "notes were not computed"
// when it means "nothing fired".
func TestNotesRenderAsAnEmptyListNotNull(t *testing.T) {
	total := 10.0
	d := noteFixture("cores", total)
	d.Groups = []groupJSON{{Name: "tc", Value: 10}}
	d.Notes = notesFor(noteOpts(), d)
	if len(d.Notes) != 0 {
		t.Fatalf("this fixture should trip no rule, got %v", noteCodes(d.Notes))
	}

	out := captureStdout(t, func() {
		if err := renderJSON(d); err != nil {
			t.Fatalf("renderJSON: %v", err)
		}
	})
	if strings.Contains(out, `"notes": null`) {
		t.Errorf("no notes must render as [], matching failed and groups:\n%s", out)
	}
	if !strings.Contains(out, `"notes": []`) {
		t.Errorf("expected an empty list:\n%s", out)
	}

	// And the documented order: the verdict before the data it qualifies.
	if ni, gi := strings.Index(out, `"notes"`), strings.Index(out, `"groups"`); ni < 0 || ni > gi {
		t.Errorf("notes should precede groups in the document:\n%s", out)
	}
}
