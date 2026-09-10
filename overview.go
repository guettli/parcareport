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

// overviewData is one overview: the same report run a few ways.
type overviewData struct {
	Start        time.Time     `json:"start"`
	End          time.Time     `json:"end"`
	WindowSecs   float64       `json:"window_seconds"`
	ProfileTypes []string      `json:"profile_types"`
	Labels       []string      `json:"labels"`
	Sections     []*reportData `json:"sections"`
	Skipped      []skippedJSON `json:"skipped"`
	Complete     bool          `json:"complete"`

	// Kept as a Duration so the heading rounds rather than truncates.
	window time.Duration
}

type skippedJSON struct {
	What   string `json:"what"`
	Reason string `json:"reason"`
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

	types, err := c.ProfileTypeNames(ctx)
	if err != nil {
		return fmt.Errorf("overview needs the profile type list to know what to report on: %w", err)
	}
	labels, err := c.LabelNames(ctx, start, end)
	if err != nil {
		return fmt.Errorf("overview needs the label list to know what to break down by: %w", err)
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
			Reason: fmt.Sprintf("the server offers %d CPU delta profiles, so there is no single one to report on", len(cpu)),
		})
	}

	var plan []struct{ profType, by string }
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
			vals, err := c.LabelValues(ctx, by, start, end)
			if err != nil {
				d.Skipped = append(d.Skipped, skippedJSON{
					What:   "CPU by " + by,
					Reason: shortErr(err),
				})
				continue
			}
			if o.maxGroups > 0 && len(vals) > o.maxGroups {
				d.Skipped = append(d.Skipped, skippedJSON{
					What: "CPU by " + by,
					Reason: fmt.Sprintf("%d values is more than --max-group-values=%d, and each one costs a merge; "+
						"run `parcareport --by=%s` directly if you want it", len(vals), o.maxGroups, by),
				})
				continue
			}
			plan = append(plan, struct{ profType, by string }{cpu[0], by})
		}
		if len(plan) == 0 {
			d.Skipped = append(d.Skipped, skippedJSON{
				What:   "CPU breakdowns",
				Reason: "none of " + strings.Join(overviewBreakdowns, ", ") + " exists in this window",
			})
		}
	}
	// Live heap, if the server has it. It is not a rate and says something the
	// CPU profile cannot.
	if heap := findHeapType(types); heap != "" {
		// instance and job only. `have` is the union of label names across
		// every profile type, so falling back to `cluster` paired the heap
		// with a label that only parca-agent's CPU series carry: the section
		// then merged once per cluster, found nothing, and reported "(no data
		// in this window)" for a heap profile that has plenty -- the same
		// conflation of "absent label" with "no data" the tool refuses
		// everywhere else.
		by := firstPresent(have, "instance", "job")
		if by == "" {
			d.Skipped = append(d.Skipped, skippedJSON{
				What:   "live heap",
				Reason: "no instance or job label to group by; heap profiles come from scrape targets, which carry those",
			})
		} else {
			plan = append(plan, struct{ profType, by string }{heap, by})
		}
	}

	if len(plan) == 0 {
		err := fmt.Errorf("nothing to report on: the server has %d profile types and %d labels "+
			"in this window, and none of them makes a breakdown this command knows how to build",
			len(types), len(labels))
		if out == outputJSON {
			_ = emitOverviewJSON(d)
			return err
		}
		printOverviewHeader(d)
		printSkipped(d.Skipped)
		return err
	}

	d.Complete = true
	for _, p := range plan {
		so := o
		// The type came from the list read above, so it needs no lookup and
		// no validation -- passing it as resolvedType skips both.
		so.resolvedType, so.by = p.profType, p.by
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
	for _, s := range skipped {
		fmt.Printf("-- not reported: %s (%s)\n", s.What, s.Reason)
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

func firstPresent(have map[string]bool, names ...string) string {
	for _, n := range names {
		if have[n] {
			return n
		}
	}
	return ""
}
