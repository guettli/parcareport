package main

import (
	"fmt"
	"strings"
)

// A sorted table is not a pointer. The tool called itself a bottleneck report
// and then left "which of these is a bottleneck" entirely to the reader --
// every row printed the same way, with the only threshold in the codebase
// being the 0.1% cutoff that decides whether the residual row is worth showing
// at all.
//
// Notes are the missing half: plain rules, no heuristics beyond thresholds,
// each naming what it saw, why it matters, and where to read more. They are
// deliberately few. A report that flags everything flags nothing, and a note
// that only restates the top row of the table it sits under is noise -- which
// is why "the biggest group is big" is not among them. The drill-down block
// already names the largest rows and what to run against them.
//
// Every note is a statement about the data, not a diagnosis of the system. Two
// of them say so explicitly, because the underlying fact -- a cgroup quota, a
// thread count -- is not in any profile and the shape is only consistent with
// it. Overstating that would be the same failure as a flamegraph percentage
// presented as an absolute.

// noteJSON is one finding. Code is the stable identifier a consumer switches
// on; Message is for a person. Doc points into docs/bottlenecks.md, which is
// where the reasoning lives and which the tool otherwise never mentioned.
type noteJSON struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	// Docs are the sections of docs/bottlenecks.md that explain this. More
	// than one where the finding genuinely spans them: a group at one core is
	// discussed both as a parallelism ceiling and as cgroup throttling, and
	// citing only the first would send the reader past the other.
	Docs []string `json:"docs,omitempty"`
	// Command is what to run next, when there is something to run. Not every
	// note has one: a symbolization gap is fixed at the agent, not by another
	// query.
	Command string `json:"command,omitempty"`
}

// Thresholds. Each is a judgement about when a number stops being incidental
// and starts being the story, and each is deliberately generous -- a note that
// fires on a 5% residual would fire on almost every report and teach the
// reader to skip the block.
const (
	// Above this, the profile is more unattributed than attributed and the
	// function table below it is not describing the workload.
	unsymbolizedShare = 50.0
	// A tenth of the fleet unaccounted for is past rounding: it means a whole
	// agent or scrape tier is missing the label.
	unlabeledShare = 10.0
	// How close to exactly one core counts as "pinned at one core".
	coreCeilingTolerance = 0.03
	// Runtime GC and allocation frames costing this much of the on-CPU time
	// means the program is spending its CPU on memory management.
	gcOverheadShare = 20.0
)

// gcFrames are the Go runtime frames that are garbage collection or the
// allocator, by prefix. Deliberately short: these are unambiguous, where
// something like runtime.memclrNoHeapPointers is also plain memset and would
// make the rule fire on programs doing nothing wrong.
var gcFrames = []string{
	// The doc this note links to singles out gcAssistAlloc: it is where GC
	// pressure stops being a background cost and starts being request latency,
	// because the allocating goroutine is made to pay for the collection.
	"runtime.gcAssistAlloc",
	"runtime.gcBgMarkWorker",
	"runtime.gcDrain",
	"runtime.mallocgc",
	"runtime.scanobject",
	// bgsweep and (*mspan).sweep are where sweep time actually lands;
	// "runtime.sweep" alone matches only sweepone.
	"runtime.bgsweep",
	"runtime.(*mspan).sweep",
	"runtime.sweep",
}

// notesFor derives the notes for a finished report. It reads only what is
// already in d, so it costs nothing and cannot fail.
func notesFor(o options, d *reportData) []noteJSON {
	notes := []noteJSON{}
	if d.Total == nil || *d.Total <= 0 {
		// Every rule below is a share of the total. Without a denominator
		// there are no shares, and inventing one from the sum of the listed
		// groups would silently omit whatever is missing.
		return notes
	}
	total := *d.Total

	if n := unsymbolizedNote(d, total); n != nil {
		notes = append(notes, *n)
	}
	if n := unlabeledNote(d, total); n != nil {
		notes = append(notes, *n)
	}
	// The remaining rules are about CPU time. On a byte or count profile
	// "pinned at one core" and "GC overhead" are not merely wrong, they are
	// not expressible.
	if d.Unit == "cores" {
		if n := coreCeilingNote(d); n != nil {
			notes = append(notes, *n)
		}
		if n := gcOverheadNote(o, d, total); n != nil {
			notes = append(notes, *n)
		}
	}
	return notes
}

// unsymbolizedNote fires when most of the profile has no function names. This
// is the one note that is not about the workload at all: it says the report
// cannot answer the question it was asked.
func unsymbolizedNote(d *reportData, total float64) *noteJSON {
	for _, f := range d.allFunctions {
		if f.Name != unsymbolizedName {
			continue
		}
		// Flat, not cum. Cum counts a sample whose stack contains a nameless
		// frame ANYWHERE, so a target with unsymbolized roots and symbolized
		// leaves scores high while its CPU is perfectly attributable. Flat is
		// the claim the note actually makes: the sample's own leaf has no
		// name, so that time cannot be blamed on any code.
		share := f.Flat / total * 100
		if share < unsymbolizedShare {
			return nil
		}
		return &noteJSON{
			Code: "unsymbolized_dominates",
			Message: fmt.Sprintf(
				"%.0f%% of this profile has no function names. That is a tooling gap, "+
					"not a finding: the CPU was measured but cannot be attributed to code. "+
					"Native binaries need debuginfo uploaded or a debuginfod the server can reach; "+
					"Go binaries are symbolized by the agent and are unaffected.",
				share),
			Docs: []string{"docs/bottlenecks.md#symbolization"},
		}
	}
	return nil
}

// podScopedLabels only exist for processes inside a Kubernetes pod. A large
// residual under one of them is the normal state of a node -- kernel threads,
// the kubelet, sshd, anything on the host -- not a finding, and flagging it
// would fire on almost every report run this way.
//
// The tool already says so in its own README; a note contradicting the
// documentation would be worse than no note at all.
var podScopedLabels = map[string]bool{
	"namespace": true, "workload": true, "workload_kind": true,
	"container": true, "pod": true,
}

// unlabeledNote fires when a large share of the profile carries no value for
// the group-by label AND the label is one every series ought to carry. Those
// series are real work the breakdown cannot place, so every row above is an
// underestimate by an unknown amount.
func unlabeledNote(d *reportData, total float64) *noteJSON {
	if podScopedLabels[d.GroupBy] {
		return nil
	}
	for _, g := range d.Groups {
		if !g.Unlabeled {
			continue
		}
		share := g.Value / total * 100
		if share < unlabeledShare {
			return nil
		}
		msg := fmt.Sprintf(
			"%.0f%% of the total carries no %q label, so it cannot be attributed to any row "+
				"and every row above understates its share. Every series ought to carry %s, "+
				"so this usually means an agent or scrape tier was deployed without it.",
			share, d.GroupBy, d.GroupBy)
		if d.DroppedGroups > 0 {
			// The breakdown query named these values and the report did not
			// show them, so their samples are in the total, in no row, and
			// therefore inside this residual. Calling all of it unlabelled
			// would be a plain misstatement.
			msg += fmt.Sprintf(
				" Note that %d values the breakdown returned were not in the label list "+
					"and are inside this residual too -- it is not all unlabelled.", d.DroppedGroups)
		}
		if len(d.Failed) > 0 {
			// The residual is the measured total minus the groups that came
			// back, so a group whose query FAILED is inside it. Reporting that
			// as "carries no label" would be a plain misstatement.
			msg += fmt.Sprintf(
				" Note that %d group queries failed in this run, and their samples are "+
					"inside this residual too -- it is not all unlabelled.", len(d.Failed))
		}
		return &noteJSON{
			Code:    "unlabeled_large",
			Message: msg,
			Docs:    []string{"docs/bottlenecks.md#coverage"},
		}
	}
	return nil
}

// coreCeilingNote fires on a group sitting at almost exactly one core.
//
// This is an inference and the note says so. One core of CPU time is what a
// single runnable thread produces, and it is also what a container with a
// 1-core quota produces once it is throttled. Neither fact is in a profile:
// Parca has no notion of work queued and no access to cgroup accounting, so
// the shape is consistent with both and proves neither.
// processScopedLabels group by something that can plausibly BE one runnable
// thread. A cluster or a namespace at 1.000 CORES is thousands of processes
// summing to a round number, which means nothing -- and on a report with
// dozens of groups something lands in a +/-0.03 band by chance. The doc frames
// this as a property of a workload, and so does the rule.
var processScopedLabels = map[string]bool{
	"comm": true, "workload": true, "container": true,
	"instance": true, "pod": true,
}

func coreCeilingNote(d *reportData) *noteJSON {
	if !processScopedLabels[d.GroupBy] {
		return nil
	}
	var hits []string
	for _, g := range d.Groups {
		if g.Unlabeled {
			continue
		}
		if diff := g.Value - 1.0; diff > -coreCeilingTolerance && diff < coreCeilingTolerance {
			hits = append(hits, g.Name)
		}
	}
	if len(hits) == 0 {
		return nil
	}
	// Capped: the message is prose, and an unbounded join would put a hundred
	// group names in the middle of a sentence.
	subject, verb := strings.Join(hits, ", "), "sits"
	if len(hits) > 3 {
		subject = fmt.Sprintf("%s and %d more", strings.Join(hits[:3], ", "), len(hits)-3)
	}
	if len(hits) > 1 {
		verb = "sit"
	}
	return &noteJSON{
		Code: "single_core_ceiling",
		Message: fmt.Sprintf(
			"%s %s at almost exactly 1.000 CORES. That is the shape of a single-threaded "+
				"ceiling or of a container throttled at a 1-core quota -- consistent with both, "+
				"and proof of neither: whether work was queuing behind it is not in any profile. "+
				"Check cgroup throttling in your metrics stack before concluding anything.",
			subject, verb),
		Docs: []string{"docs/bottlenecks.md#amdahl", "docs/bottlenecks.md#throttling"},
	}
}

// gcOverheadNote fires when Go's collector and allocator are a large share of
// the on-CPU time: the program is spending its CPU managing memory rather than
// doing the work.
//
// The denominator is the whole report, which on a fleet-wide run is every
// process on every node. Go's GC reaching a fifth of that would be
// extraordinary, so in practice this fires on a report already narrowed with
// --match -- which is the only scope where "this workload is GC-bound" is even
// a meaningful claim. That is the intended behaviour, not a dead rule.
func gcOverheadNote(o options, d *reportData, total float64) *noteJSON {
	var sum float64
	for _, f := range d.allFunctions {
		for _, p := range gcFrames {
			if strings.HasPrefix(f.Name, p) {
				sum += f.Flat
				break
			}
		}
	}
	share := sum / total * 100
	if share < gcOverheadShare {
		return nil
	}
	return &noteJSON{
		Code: "gc_overhead",
		Message: fmt.Sprintf(
			"%.0f%% of on-CPU time is in Go's collector and allocator, so this workload is "+
				"spending its CPU on memory management. Look for allocation churn in "+
				"memory:alloc_space; on Kubernetes, a GOMAXPROCS larger than the container's "+
				"CPU limit produces exactly this, and the value is not in any profile.",
			share),
		Docs: []string{"docs/bottlenecks.md#gc-pressure"},
		// Built from the same parts as the drill-down commands, so it carries
		// the url, TLS and credential-file flags this run was given. A
		// suggestion that cannot connect is not a suggestion.
		Command: strings.Join(append(baseCommandArgs(o, "alloc_space"),
			"--by "+shellQuote(d.GroupBy)+matchArg(d.Match)), " "),
	}
}

// matchArg renders an existing --match for a suggested command, or nothing.
func matchArg(match string) string {
	if match == "" {
		return ""
	}
	return " --match " + shellQuote(match)
}

// printNotes renders the notes under everything they qualify. They come last
// in the table and first in the JSON document: a person has just read the
// numbers and needs the caveat, while a consumer wants the verdict before the
// data it is about.
func printNotes(notes []noteJSON) {
	if len(notes) == 0 {
		return
	}
	fmt.Print("\nNOTES\n")
	for _, n := range notes {
		// The code on its own line: it is what a consumer greps for, and
		// inlining it pushed the first line of prose past every other line's
		// wrap, which is the one thing the tables here are careful about.
		fmt.Printf("  [%s]\n", n.Code)
		fmt.Printf("    %s\n", wrapIndent(n.Message, noteWidth, "    "))
		for _, doc := range n.Docs {
			fmt.Printf("    see %s\n", doc)
		}
		if n.Command != "" {
			// Wrapped like the prose: this is the longest line a note emits --
			// a narrowed --match easily pushes it past 130 columns -- and it
			// was the one line the width budget did not cover.
			fmt.Printf("    %s\n", wrapIndent(n.Command, noteWidth, "      "))
		}
	}
}

// noteWidth leaves room for the four-space indent inside an 80-column
// terminal, which is what the tables here already assume.
const noteWidth = 76

// wrapIndent wraps text to width, indenting every line after the first. Notes
// are prose and a terminal is not infinitely wide; the tables here are already
// careful about that and the notes should not be the thing that forces a
// horizontal scroll.
func wrapIndent(s string, width int, indent string) string {
	words := strings.Fields(s)
	if len(words) == 0 {
		return ""
	}
	var b strings.Builder
	// The caller prints `indent` before the first word, so the first line is
	// already that many columns in. Starting the count at zero made every
	// first line wider than its own continuations.
	line := len(indent)
	for i, w := range words {
		switch {
		case i == 0:
			b.WriteString(w)
			line += len(w)
		case line+1+len(w) > width:
			b.WriteString("\n" + indent + w)
			line = len(indent) + len(w)
		default:
			b.WriteString(" " + w)
			line += 1 + len(w)
		}
	}
	return b.String()
}
