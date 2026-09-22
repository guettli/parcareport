package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"
)

// overviewBreakdowns are the label names worth breaking CPU down by, in the
// order a person would ask about them: which cluster, then which workload
// inside it, then which process.
//
// Only the ones the server actually has are used. A label list is one cheap
// query, so asking is better than making the user know in advance which of
// these their agents were configured to emit -- namespace, container and
// workload only exist if the agents were given relabel_configs.
var overviewBreakdowns = []string{"cluster", "namespace", "workload", "comm"}

// overviewConcurrency is how many queries a section runs at once, when the
// caller has not said. See the reasoning where it is applied.
const overviewConcurrency = 2

// overviewData is one overview: the same report run a few ways.
type overviewData struct {
	Start        time.Time     `json:"start"`
	End          time.Time     `json:"end"`
	WindowSecs   float64       `json:"window_seconds"`
	ProfileTypes []string      `json:"profile_types"`
	Labels       []string      `json:"labels"`
	Sections     []*reportData `json:"sections"`
	Skipped      []skippedJSON `json:"skipped"`
	// Outcome and Complete mean what they do on a report: "found" when a
	// section produced rows, "empty" when every query was answered and none
	// did, "incomplete" when something was not answered. An overview is the
	// least complete of its sections -- one broken breakdown makes the whole
	// picture partial -- but an overview of an idle server is a successful
	// measurement, not a failure.
	Outcome  string `json:"outcome"`
	Complete bool   `json:"complete"`
	// Error is why the overview could not be produced at all. The report
	// document has had one since it existed; this one reached stderr only, so
	// a consumer parsing the JSON saw an overview with no sections and no
	// stated reason.
	Error string `json:"error,omitempty"`

	// Kept as a Duration so the heading rounds rather than truncates.
	window time.Duration
}

// emitOverviewFailure writes the one-document-whatever-happened form for a
// failure that happened before there was anything to put in it.
func emitOverviewFailure(out string, start, end time.Time, err error) error {
	if out != outputJSON {
		return err
	}
	_ = emitOverviewJSON(&overviewData{
		Start:        start.UTC(),
		End:          end.UTC(),
		WindowSecs:   math.Round(end.Sub(start).Seconds()*1000) / 1000,
		ProfileTypes: []string{},
		Labels:       []string{},
		Sections:     []*reportData{},
		Skipped:      []skippedJSON{},
		Outcome:      outcomeIncomplete,
		Error:        err.Error(),
		window:       end.Sub(start),
	})
	return err
}

// mergeDerivedNotes are the note codes computed from the unfiltered merge,
// which is identical across every section sharing a profile type. Repeating
// them per section is noise; the others genuinely differ by --by.
var mergeDerivedNotes = map[string]bool{
	"unsymbolized_dominates": true,
	"gc_overhead":            true,
}

// notesFromGroups drops the notes a sibling section has already stated.
func notesFromGroups(notes []noteJSON) []noteJSON {
	out := []noteJSON{}
	for _, n := range notes {
		if !mergeDerivedNotes[n.Code] {
			out = append(out, n)
		}
	}
	return out
}

// Why something was left out. The same distinction the report's `outcome`
// draws: a choice is not a failure, and one heading covering both put "we do
// not sweep this type" beside "this query died" with nothing to tell them
// apart.
const (
	// skipNotSwept: this command has no policy for it. Nothing went wrong.
	skipNotSwept = "not_swept"
	// skipTooCostly: it would have been run, but it was refused on cost. A
	// choice, and a recoverable one -- the command says how.
	skipTooCostly = "too_costly"
	// skipUnavailable: it could not be run. A query failed, or nothing exists
	// to group it by.
	skipUnavailable = "unavailable"
)

type skippedJSON struct {
	What string `json:"what"`
	// Kind is skipNotSwept, skipTooCostly or skipUnavailable.
	Kind   string `json:"kind"`
	Reason string `json:"reason"`
	// Command is what to run to get the thing that was skipped, when there is
	// such a thing to run. Separate from Reason because a consumer should not
	// have to pull a command out of an English sentence -- the reasons used to
	// end "run `parcareport --by=comm` directly if you want it", which is a
	// hint an agent has to regex.
	Command string `json:"command,omitempty"`
}

// overview answers the questions a person meeting a new Parca would ask,
// without making them know the answers first.
//
// Getting oriented otherwise meant running the tool once per question, and
// each run needed a selector and a --by label chosen in advance. Against a
// real server each of those took minutes, so the cost of guessing wrong was
// high. This asks the server what it has and reports on that.
func overview(ctx context.Context, c *Client, o options, start, end time.Time) error {
	out, err := parseOutput(o.output)
	if err != nil {
		return err
	}

	// These are chosen per section, so accepting them would silently ignore
	// them -- and `report` and `types` both reject arguments they cannot use.
	for _, f := range []string{"by", "profile-type"} {
		if o.setFlags[f] {
			return fmt.Errorf("overview picks the profile type and the --by label per section, "+
				"so --%s cannot apply; use `parcareport report --%s=...` for one breakdown", f, f)
		}
	}

	// Both of these fail before any document exists, and returning only an
	// error printed NOTHING at all under --output=json: a consumer got an
	// empty stdout and a non-zero exit, which is the silence this format was
	// written to refuse. They are also the likeliest way an overview fails,
	// being the first two things it asks for.
	types, err := c.ProfileTypeNames(ctx)
	if err != nil {
		err = fmt.Errorf("overview needs the profile type list to know what to report on: %w", err)
		return emitOverviewFailure(out, start, end, err)
	}
	labels, err := c.LabelNames(ctx, start, end)
	if err != nil {
		err = fmt.Errorf("overview needs the label list to know what to break down by: %w", err)
		return emitOverviewFailure(out, start, end, err)
	}
	have := map[string]bool{}
	for _, l := range labels {
		have[l] = true
	}
	sort.Strings(labels)

	d := &overviewData{
		Start:        start.UTC(),
		End:          end.UTC(),
		WindowSecs:   math.Round(end.Sub(start).Seconds()*1000) / 1000,
		window:       end.Sub(start),
		ProfileTypes: types,
		Labels:       labels,
		Sections:     []*reportData{},
		Skipped:      []skippedJSON{},
	}

	cpu := cpuDeltaTypes(types)
	if len(cpu) != 1 {
		// Without exactly one CPU profile there is nothing to default to, and
		// guessing which of several to report would misattribute the fleet.
		d.Skipped = append(d.Skipped, skippedJSON{
			What:   "CPU breakdowns",
			Kind:   skipUnavailable,
			Reason: fmt.Sprintf("the server offers %d CPU delta profiles, so there is no single one to report on", len(cpu)),
		})
	}

	var plan []struct{ profType, by string }
	// Set when a label lookup failed rather than came back empty, so the
	// "nothing to break down by" message can tell the truth about why.
	lookupFailed := false
	if len(cpu) == 1 {
		for _, by := range overviewBreakdowns {
			if !have[by] {
				continue
			}
			// One merge per label value, and merges are the expensive part. A
			// label like comm can have hundreds of values, which on a real
			// server is hours of work and would swamp the sections worth
			// having. Count first -- that is one cheap query -- and skip the
			// ones that are too wide, saying so.
			// The same dropped stream that costs a merge costs a label
			// lookup, and losing one here costs the whole section rather
			// than one group, so labelValues asks again once.
			vals, err := labelValues(ctx, c, o.timeout, by, o.match, start, end)
			if err != nil {
				lookupFailed = true
				d.Skipped = append(d.Skipped, skippedJSON{
					What:   "CPU by " + by,
					Kind:   skipUnavailable,
					Reason: shortErr(err),
				})
				continue
			}
			// The cap exists because each value used to cost a merge. A delta
			// CPU breakdown is now one range query however many values there
			// are, so applying it there would refuse the case it was written
			// to protect -- `comm` has over a thousand values and is the most
			// useful breakdown on the list, being the one that names
			// processes. It still applies under --fan-out, where the old cost
			// is back.
			costsAMerge := o.fanOut || !isDeltaType(cpu[0])
			if costsAMerge && o.maxGroups > 0 && len(vals) > o.maxGroups {
				d.Skipped = append(d.Skipped, skippedJSON{
					What: "CPU by " + by,
					Kind: skipTooCostly,
					Reason: fmt.Sprintf("%d values is more than --max-group-values=%d, and each one costs a merge",
						len(vals), o.maxGroups),
					// Same profile type, so the run's --match still applies.
					Command: sectionCommand(o, cpu[0], by, true),
				})
				continue
			}
			plan = append(plan, struct{ profType, by string }{cpu[0], by})
		}
		if len(plan) == 0 {
			// "The label is not there" and "the query for it failed" are
			// different answers, and saying the first when the second
			// happened sends the reader looking for a missing relabel_config
			// that is not missing.
			reason := "none of " + strings.Join(overviewBreakdowns, ", ") + " exists in this window"
			if lookupFailed {
				reason = "the label lookups above failed, so there was nothing left to break the CPU profile down by"
			}
			d.Skipped = append(d.Skipped, skippedJSON{What: "CPU breakdowns", Kind: skipUnavailable, Reason: reason})
		}
	}
	// Live heap, if the server has it. It is not a rate and says something the
	// CPU profile cannot.
	if heap := findHeapType(types); heap != "" {
		// instance and job only. `labels` is the union of label names across
		// every profile type, so falling back to `cluster` paired the heap
		// with a label that only parca-agent's CPU series carry: the section
		// then merged once per cluster, found nothing, and reported "(no data
		// in this window)" for a heap profile that has plenty -- the same
		// conflation of "absent label" with "no data" the tool refuses
		// everywhere else.
		by := breakdownLabelFor(heap, labels)
		if by == "" {
			d.Skipped = append(d.Skipped, skippedJSON{
				What:   "live heap",
				Kind:   skipUnavailable,
				Reason: "no instance or job label to group by; heap profiles come from scrape targets, which carry those",
			})
		} else {
			plan = append(plan, struct{ profType, by string }{heap, by})
		}
	}

	// Everything the server offers that this command has no policy for.
	//
	// Seeded with the types it DOES have a policy for, not merely the ones it
	// ran. A type the overview considered and declined already carries its own
	// honest skip -- "no instance or job label to group by", "the server offers
	// 2 CPU delta profiles" -- and letting it fall through here overwrote that
	// with "overview sweeps CPU and live heap", which is false of exactly those
	// types. It listed the live-heap type as unswept two lines under a sentence
	// naming live heap as swept.
	used := map[string]bool{}
	for _, p := range plan {
		used[p.profType] = true
	}
	for _, t := range cpu {
		used[t] = true
	}
	if h := findHeapType(types); h != "" {
		used[h] = true
	}
	d.Skipped = append(d.Skipped, unanalyzedTypes(o, types, used, labels)...)

	if len(plan) == 0 {
		err := fmt.Errorf("nothing to report on: the server has %d profile types and %d labels "+
			"in this window, and none of them makes a breakdown this command knows how to build",
			len(types), len(labels))
		if out == outputJSON {
			// The reason, in the document. It used to reach stderr only, so a
			// consumer saw an overview with no sections and nothing saying
			// why -- the same silence the report format refuses.
			d.Outcome, d.Complete, d.Error = outcomeIncomplete, false, err.Error()
			_ = emitOverviewJSON(d)
			return err
		}
		printOverviewHeader(d)
		printSkipped(d.Skipped)
		return err
	}

	// Lower concurrency than a single report, unless asked otherwise.
	//
	// Sections already run one after another, but the queries inside a
	// section went four abreast, and each concurrent merge materialises a
	// profile server-side. A Parca sized for ingestion refuses at that rate:
	// on the server this was measured against, an 8-group heap breakdown at
	// --concurrency=8 failed all 8 with RST_STREAM, at 2 it lost the label
	// lookup, and at 1 it succeeded. overview is the command most likely to
	// meet that wall, because it issues more queries than anything else.
	//
	// Two rather than one: sequential would roughly double an already slow
	// command for no benefit on a server that is coping. An explicit
	// --concurrency always wins -- someone who has measured their own server
	// knows better than this default.
	if !o.setFlags["concurrency"] && o.concurrency > overviewConcurrency {
		o.concurrency = overviewConcurrency
	}

	// Not unconditionally true: a label lookup that FAILED was recorded as a
	// skip above, and starting from true erased it. The overview then claimed
	// outcome "empty" -- documented as "every query was answered, and the
	// answer was nothing" -- for a run where a query was not answered, in the
	// very field this change tells consumers to switch on.
	d.Complete = !lookupFailed
	for _, p := range plan {
		so := o
		// The type came from the list read above, so it needs no lookup and
		// no validation -- passing it as resolvedType skips both.
		so.resolvedType, so.by = p.profType, p.by
		// The label list was read above, so the drill-down hints can name a
		// finer label instead of leaving --by where it is. A plain report has
		// no list and does not fetch one; see nextBreakdown.
		so.knownLabels = labels
		so.moreToCome = true
		so.profileType = p.profType
		sd, serr := gatherReport(ctx, c, so, start, end)
		if sd == nil {
			// One section failing is not the whole overview failing; that is
			// the point of running several.
			d.Complete = false
			d.Skipped = append(d.Skipped, skippedJSON{
				What:   fmt.Sprintf("%s by %s", p.profType, p.by),
				Reason: shortErr(serr),
			})
			continue
		}
		if serr != nil {
			d.Complete = false
		}
		d.Sections = append(d.Sections, sd)
	}

	// The overview is as complete as its least complete section, and as
	// "found" as its most productive one: a single breakdown with rows means
	// the server is not idle, whatever the others came back with.
	d.Outcome = outcomeEmpty
	if !d.Complete {
		d.Outcome = outcomeIncomplete
	} else {
		for _, sd := range d.Sections {
			if sd.Outcome == outcomeFound {
				d.Outcome = outcomeFound
				break
			}
		}
	}

	if out == outputJSON {
		return emitOverviewJSON(d)
	}
	printOverviewHeader(d)
	// The hot functions come from the unfiltered merge, so every breakdown of
	// one profile type yields a byte-identical table. Print it once -- but
	// only mark it shown once one has actually been printed, or a first
	// section that failed or came back empty would suppress the table for
	// every later section and the overview would carry none at all.
	shown := map[string]bool{}
	for i, s := range d.Sections {
		if i > 0 {
			fmt.Println()
		}
		if shown[s.ProfileType] {
			quiet := *s
			quiet.Functions = nil
			// The notes derived from that same unfiltered merge would repeat
			// with it, word for word, once per section. The ones that vary by
			// --by are kept: whether a label leaves a large residual, or which
			// groups sit at one core, is a different answer per section.
			quiet.Notes = notesFromGroups(s.Notes)
			renderTable(&quiet)
			continue
		}
		renderTable(s)
		if len(s.Functions) > 0 {
			shown[s.ProfileType] = true
		}
	}
	printSkipped(d.Skipped)
	if !d.Complete {
		return fmt.Errorf("the overview is incomplete; see the notes above")
	}
	return nil
}

func emitOverviewJSON(d *overviewData) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(d); err != nil {
		return fmt.Errorf("writing JSON: %w", err)
	}
	if !d.Complete {
		return fmt.Errorf("the overview is incomplete")
	}
	return nil
}

func printOverviewHeader(d *overviewData) {
	fmt.Printf("%s .. %s  (%s)\n",
		d.Start.Format("2006-01-02T15:04:05Z"), d.End.Format("2006-01-02T15:04:05Z"),
		d.window.Round(time.Second))
	fmt.Printf("%d profile types, %d labels: %s\n\n", len(d.ProfileTypes), len(d.Labels),
		strings.Join(d.Labels, " "))
}

// printSkipped says what was not reported and why. An overview that quietly
// leaves a section out is worse than one that says it could not run it: the
// reader has no way to tell "no heap profile on this server" from "the heap
// query failed".
func printSkipped(skipped []skippedJSON) {
	if len(skipped) == 0 {
		return
	}
	fmt.Println()
	// Grouped by reason for the page, one entry per thing in the document.
	// Nine profile types each repeating the same sentence is a wall a reader
	// skips; a consumer still wants them separable, so the grouping lives here
	// rather than in the data.
	//
	// By reason, not by adjacency. The entries are appended from several
	// places and sorted inside one of them, so two sharing a reason are not
	// guaranteed to be neighbours -- a run-length version printed the same
	// heading twice and called the second group "2 of them" when five shared
	// it.
	var order []string
	groups := map[string][]skippedJSON{}
	for _, s := range skipped {
		if _, seen := groups[s.Reason]; !seen {
			order = append(order, s.Reason)
		}
		groups[s.Reason] = append(groups[s.Reason], s)
	}

	for _, reason := range order {
		g := groups[reason]
		if len(g) == 1 {
			fmt.Printf("-- not reported: %s\n   %s\n", g[0].What, reason)
			if g[0].Command != "" {
				fmt.Printf("   %s\n", g[0].Command)
			}
			continue
		}
		fmt.Printf("-- not reported, %d of them: %s\n", len(g), reason)
		for _, s := range g {
			fmt.Printf("   %s\n", s.What)
			if s.Command != "" {
				// On its own line, and runnable. It used to be a clause inside
				// the reason, which made it prose to a person and a regex to
				// an agent.
				fmt.Printf("     %s\n", s.Command)
			}
		}
	}

}

// findHeapType picks the live-heap profile by its parts rather than by a
// string prefix, which would also match a sample type merely beginning with
// the same letters.
func findHeapType(types []string) string {
	for _, t := range types {
		p := strings.Split(t, ":")
		if len(p) >= 3 && p[0] == "memory" && p[1] == "inuse_space" && p[2] == "bytes" {
			return t
		}
	}
	return ""
}

// firstPresent returns the first of names that the server actually has, or ""
// if it has none of them. It takes the label list rather than a set so callers
// holding either shape can use it.
func firstPresent(have []string, names ...string) string {
	for _, n := range names {
		for _, h := range have {
			if h == n {
				return n
			}
		}
	}
	return ""
}

// unanalyzedTypes lists the profile types the overview did not look at, with
// the command that would look at each.
//
// The header says "11 profile types", which reads as coverage. It is a menu:
// an overview sweeps CPU and live heap, so on that server it sampled two of
// eleven and said nothing about the other nine. docs/bottlenecks.md warns that
// "the overview found nothing" says nothing about contention, blocking,
// goroutine leaks, allocation churn or off-CPU waits -- the doc said it and the
// tool did not.
//
// This is a coverage disclosure and a drill-down hint at once, which is why it
// is worth more than its size: every capability the overview declined to
// exercise gets a runnable command next to it.
func unanalyzedTypes(o options, all []string, used map[string]bool, labels []string) []skippedJSON {
	// Sorted, because the server's own order is not stable and two runs over
	// the same window should print the same page.
	rest := make([]string, 0, len(all))
	for _, t := range all {
		if !used[t] {
			rest = append(rest, t)
		}
	}
	sort.Strings(rest)

	var out []skippedJSON
	for _, t := range rest {
		by := breakdownLabelFor(t, labels)
		sk := skippedJSON{
			What:   t,
			Kind:   skipNotSwept,
			Reason: "overview sweeps CPU and live heap; this type needs an explicit --profile-type",
		}
		if by == "" {
			// No sensible label to group it by, so no command: one that named
			// a label these series do not carry would report "(no data)" for a
			// profile with plenty, which is the conflation of "absent label"
			// with "no data" this tool refuses everywhere else.
			sk.Kind = skipUnavailable
			sk.Reason += ", and no label it carries is available to group it by"
		} else {
			sk.Command = sectionCommand(o, t, by, false)
		}
		out = append(out, sk)
	}
	return out
}

// breakdownLabelFor picks the label to suggest for a type the overview did not
// run.
//
// Which labels a series carries depends on who wrote it. parca-agent tags its
// own profiles with the fleet labels -- cluster, node, comm -- while everything
// scraped from a /debug/pprof endpoint carries only job and instance. Pairing
// one with the other's labels is a real failure mode and is documented a few
// lines above: the heap section once defaulted to `cluster`, matched nothing,
// and reported an empty window for a profile that had plenty.
//
// The profile name is the first field of the selector and says which tier
// wrote it, so the choice is read rather than guessed.
func breakdownLabelFor(profType string, labels []string) string {
	name, rest, _ := strings.Cut(profType, ":")
	if name != "parca_agent" {
		return firstPresent(labels, "instance", "job")
	}
	if strings.HasPrefix(rest, "wallclock:") {
		// Off-CPU wants the process, not the cluster. docs/bottlenecks.md is
		// explicit that a wallclock total is mostly idleness and that the
		// stacks are the only thing worth judging -- so a two-row cluster
		// table is the shape it warns against, while `comm` at least names
		// what to look inside.
		return firstPresent(labels, "comm", "workload", "container", "node", "cluster")
	}
	// node is in the list even though overviewBreakdowns omits it: the agent
	// tier always carries it, and it is a useful breakdown for a type the
	// overview is not going to run itself.
	return firstPresent(labels, append(append([]string{}, overviewBreakdowns...), "node")...)
}

// sectionCommand is the command that runs one breakdown the overview did not.
// Built from the same parts as every other suggestion this tool prints, so it
// carries the url, TLS and credential-file flags the run was given.
//
// carryMatch is false for a suggestion that changes the profile type. The run's
// --match was written for the labels of the type it was run against, and a
// scrape-tier profile does not carry parca-agent's: copying
// `--match 'cluster="tc"'` onto a goroutine profile selects nothing and reports
// an empty window for a profile with plenty, which is the exact mistake the
// label choice above exists to avoid.
func sectionCommand(o options, profType, by string, carryMatch bool) string {
	args := baseCommandArgs(o, profType)
	args = append(args, "--by "+shellQuote(by))
	if carryMatch && o.match != "" {
		args = append(args, "--match "+shellQuote(o.match))
	}
	return strings.Join(args, " ")
}
