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
	if heap := findType(types, "memory:inuse_space"); heap != "" {
		by := firstPresent(have, "instance", "job", "cluster")
		if by == "" {
			d.Skipped = append(d.Skipped, skippedJSON{
				What:   "live heap",
				Reason: "no instance, job or cluster label to group by",
			})
		} else {
			plan = append(plan, struct{ profType, by string }{heap, by})
		}
	}

	if len(plan) == 0 {
		if out == outputJSON {
			return emitOverviewJSON(d)
		}
		printOverviewHeader(d)
		printSkipped(d.Skipped)
		return fmt.Errorf("nothing to report on: the server has %d profile types and %d labels in this window",
			len(types), len(labels))
	}

	d.Complete = true
	// The hot functions come from the unfiltered merge, so every breakdown of
	// the same profile type produces an identical table. Show it once.
	shownFunctions := map[string]bool{}
	for _, p := range plan {
		so := o
		so.profileType, so.by = p.profType, p.by
		if shownFunctions[p.profType] {
			so.top = 0
		}
		shownFunctions[p.profType] = true
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
	for i, s := range d.Sections {
		if i > 0 {
			fmt.Println()
		}
		renderTable(s)
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

func findType(types []string, prefix string) string {
	for _, t := range types {
		if strings.HasPrefix(t, prefix) {
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
