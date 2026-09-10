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
	// Delta says whether the profile type accumulates. When false the numbers
	// come from ONE profile, named by SnapshotAt, not from the whole window --
	// summing snapshots would multiply a level by the number of scrapes.
	Delta bool `json:"delta"`
	// SnapshotAt is the timestamp of the profile the numbers came from, and is
	// null for a delta, where the whole window was merged.
	SnapshotAt *time.Time `json:"snapshot_at"`

	Groups      []groupJSON `json:"groups"`
	EmptyGroups int         `json:"empty_groups"`
	// StaleSeries counts series with no scrape inside the snapshot window --
	// usually because they are scraped less often than the fastest series in
	// the selector, though the code only knows the window missed them. They
	// contribute nothing to these numbers, and a series vanishing from a
	// total without saying so is exactly what this tool exists to point out.
	StaleSeries int `json:"stale_series,omitempty"`
	// DoubledSeries counts series with MORE than one scrape inside the
	// window, which are therefore counted twice. The interval estimate is a
	// median, so a series that genuinely scraped twice in one median gap can
	// land here. Rare, and said out loud rather than left in the number.
	DoubledSeries int `json:"doubled_series,omitempty"`
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

// snapshotWindow picks the window a non-delta profile should be merged over.
//
// A merge sums across BOTH time and series. Only the first is wrong for a
// snapshot -- the second is what makes a group total a total. Fetching one
// profile fixes the first and breaks the second: a selector usually matches
// several series, each written at its own instant, so one instant returns one
// series. Measured: `--by=job` over all eight scrape targets reported 3.0 MiB
// while a single instance in the same period was 18 MiB. A total cannot be
// smaller than one of its parts.
//
// So merge, but over a window narrow enough to hold at most one profile per
// series: one scrape interval, inferred from the spacing between timestamps,
// ending at the newest profile.
//
// The window is (latest-interval, latest], not latest +/- interval/2. latest
// is the newest timestamp anywhere in the selector, so nothing exists after
// it: centring on it would spend half the width on empty space and drop every
// series whose own newest scrape is more than half an interval older. Parca
// staggers scrape targets across the interval, so that is about half of them.
//
// It is still safe against double-counting. interval is the SMALLEST gap in
// any series, so no series can have two samples inside a half-open window of
// that width.
//
// stale counts the series whose newest sample falls outside the window. They
// contribute nothing to the merge, and the caller says so: a series scraped
// far less often than the fastest one would otherwise just vanish from a
// total with no sign that it had.
func snapshotWindow(ctx context.Context, c *Client, selector string, start, end time.Time) (
	from, to, latest time.Time, stale, doubled int, err error) {
	times, err := c.ProfileTimes(ctx, selector, start, end)
	if err != nil {
		return start, end, time.Time{}, 0, 0, err
	}
	if len(times) == 0 {
		return start, end, time.Time{}, 0, 0, nil // nothing in the window
	}
	latest, interval := latestAndInterval(times)
	if interval <= 0 {
		// One profile per series and no spacing to infer -- the window
		// already holds a single scrape, so merging it whole is safe.
		return start, end, latest, 0, 0, nil
	}
	from = latest.Add(-interval).Add(time.Nanosecond)
	to = latest
	if from.Before(start) {
		from = start
	}
	for _, series := range times {
		inWindow := 0
		for _, t := range series {
			if !t.Before(from) && !t.After(to) {
				inWindow++
			}
		}
		switch {
		case inWindow == 0:
			stale++
		case inWindow > 1:
			doubled++
		}
	}
	return from, to, latest, stale, doubled, nil
}

// latestAndInterval finds the newest timestamp in the selector and estimates
// the scrape interval from the spacing between timestamps.
//
// The estimate is each series' median gap, then the smallest of those medians.
// The lower median where the count is even, so an even split errs toward the
// narrower window rather than one wide enough to hold two scrapes.
//
// The smallest gap outright would be safer against double-counting, but it is
// far too easy to break: one close-together pair anywhere -- an agent restart,
// a backfill, a scrape that ran early -- collapses the window for the entire
// fleet, and then almost nothing falls inside it. Measured on synthetic
// series, a single 5-second gap among 60-second scrapes cut a three-target
// total to one target: a third of the truth, reported as if it were all of
// it. A median ignores one outlier gap and keeps the whole fleet in the
// window.
//
// What that costs: a series that really did scrape twice in one median gap
// can have both scrapes inside the window and be counted twice. The caller
// counts those and says so, rather than letting the number be quietly wrong.
func latestAndInterval(times [][]time.Time) (latest time.Time, interval time.Duration) {
	for _, series := range times {
		var gaps []time.Duration
		for i, t := range series {
			if t.After(latest) {
				latest = t
			}
			if i == 0 {
				continue
			}
			if gap := t.Sub(series[i-1]); gap > 0 {
				gaps = append(gaps, gap)
			}
		}
		if len(gaps) == 0 {
			continue
		}
		sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
		if median := gaps[(len(gaps)-1)/2]; interval == 0 || median < interval {
			interval = median
		}
	}
	return latest, interval
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
	profType, typeVerified, err := resolveProfileType(ctx, c, o.profileType)
	if err != nil {
		return nil, err
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
	delta := isDeltaType(profType)

	// For a snapshot type, pick the window ONCE, from the unfiltered
	// selector, and merge every group over that same window. Letting each
	// group infer its own would give the rows and the total different
	// instants: an instance that stopped scraping ten minutes ago would
	// report its last heap at full value against a total that correctly
	// excludes it, and the rows would sum to more than 100%.
	qstart, qend := start, end
	var snapAt time.Time
	staleSeries, doubledSeries := 0, 0
	if !delta {
		wctx, wcancel := context.WithTimeout(ctx, o.timeout)
		f, t, latest, stale, doubled, err := snapshotWindow(wctx, c, selector(profType, "", "", o.match), start, end)
		wcancel()
		if err != nil {
			return nil, err
		}
		qstart, qend, snapAt = f, t, latest
		staleSeries, doubledSeries = stale, doubled
	}
	d := &reportData{
		ProfileType:  profType,
		Delta:        delta,
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
		// Set here rather than after the fan-out: a collapsed window can
		// leave every group empty, and that is exactly the run where the
		// reader most needs to know scrapes existed and the window missed
		// them.
		StaleSeries:   staleSeries,
		DoubledSeries: doubledSeries,
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
			raw, err := c.MergePprof(qctx, selector(profType, o.by, g, o.match), qstart, qend)
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
		banner, err := noRows(o, groups, failed, d.EmptyGroups, profType, typeVerified)
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
	overallRaw, overallErr := c.MergePprof(octx, selector(profType, "", "", o.match), qstart, qend)
	ocancel()
	d.SnapshotAt = nil
	if !snapAt.IsZero() {
		at := snapAt.UTC()
		d.SnapshotAt = &at
	}
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
func noRows(o options, groups []string, failed []failure, empty int, profType string, typeVerified bool) (string, error) {
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
		b.WriteString(formatFailures(failed, mergeQuery))
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
