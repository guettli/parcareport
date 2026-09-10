package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/pprof/profile"
)

// reportData is everything a report found, separated from how it is shown.
//
// The split exists because the table is written for a person -- columns padded
// to a width, names truncated with an ellipsis, shortfalls as prose beginning
// `!!` -- and none of that survives being parsed. Gathering into one value lets
// the JSON renderer emit full symbol names, raw numbers, and the failures as
// data.
type reportData struct {
	ProfileType string `json:"profile_type"`
	// TypeVerified is false when the ProfileTypes lookup failed and the
	// selector was taken on trust. An unverified type is the likeliest
	// explanation for an empty result, so a consumer needs to see it.
	TypeVerified *bool     `json:"profile_type_verified"`
	Start        time.Time `json:"start"`
	End          time.Time `json:"end"`
	WindowSecs   float64   `json:"window_seconds"`
	GroupBy      string    `json:"group_by"`
	Match        string    `json:"match,omitempty"`

	// Unit names what the numbers mean; Rate says whether they were divided by
	// the window. Bytes and counts are not rates, and dividing them by
	// wall-time would be nonsense.
	Unit string `json:"unit"`
	Rate bool   `json:"rate"`

	Groups      []groupJSON `json:"groups"`
	EmptyGroups int         `json:"empty_groups"`
	// Total is null when the unfiltered merge failed or came back empty. There
	// is then no denominator, and the sum of the groups is not it: it omits
	// every series carrying no group-by label. Percentages are null too.
	Total *float64 `json:"total"`

	Functions []funcJSON `json:"functions"`
	SortedBy  string     `json:"functions_sorted_by,omitempty"`

	// Failed and Complete are the point of the format. A consumer should not
	// have to grep stdout for `!!` to notice the numbers are wrong.
	Failed   []failJSON `json:"failed"`
	Complete bool       `json:"complete"`
	Error    string     `json:"error,omitempty"`

	// Rendering state the table needs and JSON does not: the display heading,
	// whether the total is trustworthy, and the failures in their original
	// form so the banners can be reproduced verbatim.
	header     string
	knowTotal  bool
	failed     []failure
	overallErr error
	groupCount int
	sortKey    sortKey
	noRows     bool
	banner     string
	// runExpired says the run's --deadline is what cut the queries short, so
	// the advice names --deadline instead of --timeout.
	runExpired bool
	// groupsSum is the sum of the labelled groups. It is what the fallback
	// row shows when there is no measured total, and it is NOT the total:
	// it omits every series carrying no group-by label.
	groupsSum float64
	// window is kept as a Duration so the heading can use Round rather than a
	// reimplementation that truncated.
	window time.Duration
}

type groupJSON struct {
	Name  string   `json:"name"`
	Value float64  `json:"value"`
	Pct   *float64 `json:"pct"`
	// Unlabeled marks the residual row: series matching the selector that
	// carry no value for the group-by label at all.
	Unlabeled bool `json:"unlabeled,omitempty"`
}

type funcJSON struct {
	// Never truncated. The table cuts names to 60 characters, which made two
	// different frames render identically and the output impossible to map
	// back to a symbol.
	Name string   `json:"name"`
	Cum  float64  `json:"cum"`
	Flat float64  `json:"flat"`
	Pct  *float64 `json:"pct"`
}

type failJSON struct {
	Group string `json:"group"`
	Error string `json:"error"`
}

// unitName turns a display heading into a stable machine name. The heading is
// a column title and may be reworded; this must not be.
func unitName(header string) string {
	switch header {
	case "CORES":
		return "cores"
	case "BLOCKED":
		return "blocked_threads"
	case "BYTES":
		return "bytes"
	case "COUNT":
		return "count"
	}
	return strings.ToLower(header)
}

// gatherReport runs the queries and assembles the result.
//
// It returns data even when something failed, because the group breakdown is
// the point of the command and is usually still worth having. The error says
// what is missing; data == nil means nothing could be assembled at all.
func gatherReport(ctx context.Context, c *Client, o options, start, end time.Time) (*reportData, error) {
	sortBy, err := parseSortKey(o.sortBy)
	if err != nil {
		return nil, err
	}
	// A caller that already resolved the type passes it in. overview does:
	// it reads the type list once, picks per section from that list, and used
	// to make gatherReport re-fetch and re-validate the list for every
	// section -- four redundant ProfileTypes calls, four copies of the same
	// warning when the lookup failed, and profile_type_verified reported
	// false for a value taken from the server's own list.
	profType, typeVerified := o.resolvedType, true
	if profType == "" {
		var err error
		profType, typeVerified, err = resolveProfileType(ctx, c, o.profileType)
		if err != nil {
			return nil, err
		}
	}
	groups, err := c.LabelValues(ctx, o.by, start, end)
	if err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return nil, explainNoValues(ctx, c, o.by, start, end)
	}
	sort.Strings(groups)

	window := end.Sub(start)
	d := &reportData{
		ProfileType:  profType,
		TypeVerified: &typeVerified,
		Start:        start.UTC(),
		End:          end.UTC(),
		WindowSecs:   math.Round(window.Seconds()*1000) / 1000,
		GroupBy:      o.by,
		Match:        o.match,
		SortedBy:     sortBy.String(),
		window:       window,
		Groups:       []groupJSON{},
		Functions:    []funcJSON{},
		Failed:       []failJSON{},
	}

	type result struct {
		name   string
		value  float64
		header string
		rate   bool
		err    error
	}
	results := make([]result, len(groups))

	// A run can take minutes. With no output at all, "still merging" and
	// "hung" look identical -- and with the default --timeout the first thing
	// you saw could be an error after a minute of silence.
	prog := newProgress("merging", o.by+" groups", len(groups))
	prog.start()

	sem := make(chan struct{}, max(1, o.concurrency))
	var wg sync.WaitGroup
	for i, g := range groups {
		wg.Add(1)
		go func(i int, g string) {
			defer wg.Done()
			defer prog.step()
			sem <- struct{}{}
			defer func() { <-sem }()

			qctx, qcancel := context.WithTimeout(ctx, o.timeout)
			defer qcancel()
			raw, err := c.MergePprof(qctx, selector(profType, o.by, g, o.match), start, end)
			if err != nil {
				results[i] = result{name: g, err: err}
				return
			}
			p, err := parsePprof(raw)
			if err != nil || p == nil {
				results[i] = result{name: g, err: err}
				return
			}
			m, err := interpret(p, window)
			results[i] = result{name: g, value: m.Value, header: m.Header, rate: m.Rate, err: err}
		}(i, g)
	}
	wg.Wait()
	prog.stop()
	// Checked once, right after the fan-out: if the run's budget went while
	// those queries were in flight, every one of them was cut short by it.
	runExpired := ctx.Err() != nil
	d.runExpired = runExpired

	rows := make([]Row, 0, len(results))
	var total float64
	// The groups already know their unit. Keeping one means the fallback below
	// does not have to guess "CORES" for what might be a byte or count profile.
	groupHeader := ""
	groupRate := false
	var failed []failure
	for _, r := range results {
		if r.err != nil {
			// Collected, not just warned about. A warning on stderr vanishes
			// under `2>/dev/null` and leaves a table that looks complete but
			// whose totals and percentages silently omit whatever failed.
			failed = append(failed, failure{group: fmt.Sprintf("%s=%s", o.by, r.name), msg: shortErr(r.err)})
			continue
		}
		total += r.value
		if r.header != "" {
			groupHeader, groupRate = r.header, r.rate
		}
		// A label value with no samples in the window says nothing, and there
		// can be hundreds of them. Count them, do not list them.
		if r.value == 0 {
			d.EmptyGroups++
			continue
		}
		rows = append(rows, Row{Name: r.name, Cores: r.value})
	}
	for _, f := range failed {
		d.Failed = append(d.Failed, failJSON{Group: f.group, Error: f.msg})
	}

	if len(rows) == 0 {
		d.Unit, d.Rate = unitName(groupHeader), groupRate
		banner, err := noRows(o, groups, failed, d.EmptyGroups, profType, typeVerified, ctx.Err() != nil)
		d.noRows, d.banner = true, banner
		d.Error = err.Error()
		return d, err
	}

	// Everything matching the selector, including series carrying no value for
	// the group-by label. Those exist -- an agent deployed without the label,
	// say -- and summing only the labelled groups would let them vanish from
	// the report while still burning CPU.
	//
	// Bounded like every per-group merge: carrying no matcher at all, this is
	// the widest query in the run and the likeliest to be slow.
	octx, ocancel := context.WithTimeout(ctx, o.timeout)
	overallRaw, overallErr := c.MergePprof(octx, selector(profType, "", "", o.match), start, end)
	ocancel()
	var overall *profile.Profile
	if overallErr == nil {
		overall, overallErr = parsePprof(overallRaw)
	}

	d.groupsSum = total
	grand := total
	header := groupHeader
	d.Rate = groupRate
	if header == "" {
		header = "CORES"
	}
	// Without that merge there is no residual and no denominator. An empty
	// response is as unknown as a failed one: parsePprof returns (nil, nil)
	// for no bytes, and the groups having values contradicts it.
	knowTotal := overallErr == nil && overall != nil
	unlabeled := -1
	if overall != nil {
		mt, err := interpret(overall, window)
		if err != nil {
			return nil, err
		}
		header = mt.Header
		d.Rate = mt.Rate
		grand = mt.Value
		if residual := grand - total; residual > grand*0.001 {
			unlabeled = len(rows)
			rows = append(rows, Row{Name: "(unlabeled)", Cores: residual})
		}
	}
	d.Unit = unitName(header)
	if knowTotal {
		d.Total = &grand
		if grand > 0 {
			for i := range rows {
				rows[i].Pct = rows[i].Cores / grand * 100
			}
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Cores > rows[j].Cores })
	for _, r := range rows {
		g := groupJSON{Name: r.Name, Value: r.Cores, Unlabeled: unlabeled >= 0 && r.Name == "(unlabeled)"}
		if knowTotal {
			pct := r.Pct
			g.Pct = &pct
		}
		d.Groups = append(d.Groups, g)
	}

	if o.top > 0 && overall != nil {
		fns, err := topFunctions(overall, window, sortBy)
		if err != nil {
			return nil, err
		}
		if len(fns) > o.top {
			fns = fns[:o.top]
		}
		for _, r := range fns {
			f := funcJSON{Name: r.Name, Cum: r.Cores, Flat: r.Flat}
			if grand > 0 {
				v := r.Flat
				if sortBy == sortCum {
					v = r.Cores
				}
				pct := v / grand * 100
				f.Pct = &pct
			}
			d.Functions = append(d.Functions, f)
		}
	}

	if overallErr != nil {
		d.Failed = append(d.Failed, failJSON{Group: "(overall)", Error: shortErr(overallErr)})
	}
	d.Complete = len(failed) == 0 && overallErr == nil
	// Kept on the struct so the table renderer can reproduce the banners and
	// the JSON renderer can put them in one field.
	d.header, d.knowTotal, d.failed, d.overallErr = header, knowTotal, failed, overallErr
	d.groupCount, d.sortKey = len(groups), sortBy

	err = reportShortfall(o, len(groups), failed, overallErr)
	if err != nil {
		d.Error = err.Error()
	}
	return d, err
}

// noRows explains an empty table without conflating the two reasons for one,
// which is how a broken run gets mistaken for an idle cluster.
//
// It returns the banner rather than printing it. Gathering must not write to
// stdout: in JSON mode a banner beside the document makes the whole output
// unparseable, which is a worse failure than the one it describes.
func noRows(o options, groups []string, failed []failure, empty int, profType string, typeVerified bool, runExpired bool) (string, error) {
	if len(failed) > 0 {
		// They mix: some queries can fail while every survivor comes back
		// genuinely empty. Reporting that as "all N failed" off len(failed)
		// gets it backwards in both directions.
		var b strings.Builder
		if len(failed) == len(groups) {
			fmt.Fprintf(&b, "!! FAILED: all %d %s queries failed, so there is nothing to report.\n"+
				"!! This is not an empty window -- the queries did not come back.\n",
				len(failed), o.by)
		} else {
			fmt.Fprintf(&b, "!! FAILED: %d of %d %s queries failed and the other %d had no samples,\n"+
				"!! so there is nothing to report. Whether this window is idle is unknown:\n"+
				"!! the failed queries were never answered.\n",
				len(failed), len(groups), o.by, empty)
		}
		b.WriteString(formatFailures(failed, mergeQuery, runExpired))
		return b.String(), fmt.Errorf("%d of %d %s queries failed; no results", len(failed), len(groups), o.by)
	}
	if !typeVerified {
		// The warning about this went only to stderr, which is the case the
		// project refuses: under 2>/dev/null a typo'd selector left an empty
		// stdout that reads as an idle cluster.
		return "!! No data, and the profile type was never verified against the\n" +
				"!! server -- that lookup failed. A selector this server does not offer\n" +
				"!! looks exactly like this. Check it with `parcareport types`.\n",
			fmt.Errorf("no data in this window, and %q was never verified", profType)
	}
	// Say so on stdout as well. A section heading followed by silence reads
	// as truncated output, especially in an overview where other sections did
	// produce tables.
	return "(no data in this window)\n", errors.New("no data in this window")
}

// reportShortfall is the non-zero exit, naming what is missing.
func reportShortfall(o options, groupCount int, failed []failure, overallErr error) error {
	switch {
	case len(failed) > 0 && overallErr != nil:
		return fmt.Errorf("%d of %d %s queries failed and so did the unfiltered merge; "+
			"results above are incomplete", len(failed), groupCount, o.by)
	case len(failed) > 0:
		return fmt.Errorf("%d of %d %s queries failed; results above are incomplete",
			len(failed), groupCount, o.by)
	case overallErr != nil:
		return fmt.Errorf("the unfiltered merge failed, so the total, the percentages and "+
			"the function table are missing: %s", shortErr(overallErr))
	}
	return nil
}
