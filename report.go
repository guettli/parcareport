package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/pprof/profile"
)

// The three outcomes a run can have. docs/bottlenecks.md tells the reader --
// specifically an agent -- to use these to tell "I looked and there is nothing
// there" from "I could not look", and until now the tool could not: an honestly
// empty window and three dead queries both came back complete:false and exit 1,
// separable only by reading an English error string.
//
// The distinction is the difference between "this service is idle" and "your
// monitoring is broken", which is not a subtlety.
const (
	// outcomeFound: rows were produced.
	outcomeFound = "found"
	// outcomeEmpty: every query was answered, and the answer was nothing. A
	// successful measurement, and exit 0.
	outcomeEmpty = "empty"
	// outcomeIncomplete: something was not answered, so what is missing is
	// unknown. Exit non-zero.
	outcomeIncomplete = "incomplete"
)

// breakdownNone is what `breakdown` says when no group table was priced at
// all. One spelling, so a consumer has one case to write.
const breakdownNone = "none"

// reportData is everything a report found, separated from how it is shown.
//
// The split exists because the table is written for a person -- columns padded
// to a width, names truncated with an ellipsis, shortfalls as prose beginning
// `!!` -- and none of that survives being parsed. Gathering into one value lets
// the JSON renderer emit full symbol names, raw numbers, and the failures as
// data.
type reportData struct {
	// ProfileType is the selector the run used. Empty only in a document for
	// a run that failed before resolving one, where profile_type_verified is
	// null beside it: no type was chosen, so none is reported.
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

	// Notes are what the tool noticed, by rule, about the numbers below: a
	// profile that cannot be attributed, a total that does not add up, a
	// workload at a ceiling. Deliberately few; see notes.go.
	//
	// First in the document on purpose. A person reads the tables and then
	// wants the caveat; a consumer wants the verdict before the data it
	// qualifies, and encoding/json emits in declaration order.
	Notes []noteJSON `json:"notes"`

	Groups      []groupJSON `json:"groups"`
	EmptyGroups int         `json:"empty_groups"`
	// ExcludedGroups counts label values that yielded no row while a --match was
	// set. EmptyGroups is the same *shape* of number -- "values that produced no
	// row" -- but a different cause, and conflating them is misleading: "no
	// samples in this window" invites you to widen the window, when under a
	// --match the matcher is the likelier culprit.
	//
	// "Likely" is not "certain": the values come from the server's unfiltered
	// list, so a value that is genuinely idle under the --match lands here too,
	// and nothing cheap tells the two apart. Without a --match the question does
	// not arise and everything that yields nothing is EmptyGroups.
	ExcludedGroups int `json:"excluded_groups,omitempty"`
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
	// Retried is how many groups were asked a second time because the server
	// dropped the first attempt. A run that quietly takes twice as long is
	// the kind of thing this tool says out loud.
	Retried int `json:"retried,omitempty"`
	// RetriedOverall counts re-asks of the unfiltered merge. Separate from
	// Retried, which is rendered as "N <label> queries were asked again": the
	// unfiltered merge carries no matcher and is not one of the groups.
	RetriedOverall int `json:"retried_overall,omitempty"`
	// DroppedGroups counts label values the breakdown query returned that the
	// label list did not have, and which were therefore not shown. Their
	// samples are inside the total but in no row, so the residual overstates
	// how much of the profile is genuinely unlabelled.
	DroppedGroups int `json:"dropped_groups,omitempty"`
	// Breakdown says how the group table was priced: "sum_by" for one range
	// query, "merge" for one merge per value. It decides what the numbers can
	// be trusted to mean -- see sumByGroups on per-target sampling periods --
	// so a consumer should not have to infer it from timing.
	Breakdown string `json:"breakdown"`
	// Total is null when the unfiltered merge failed or came back empty. There
	// is then no denominator, and the sum of the groups is not it: it omits
	// every series carrying no group-by label. Percentages are null too.
	Total *float64 `json:"total"`

	Functions []funcJSON `json:"functions"`
	SortedBy  string     `json:"functions_sorted_by,omitempty"`

	// Failed, Outcome and Complete are the point of the format. A consumer
	// should not have to grep stdout for `!!` to notice the numbers are wrong,
	// nor parse an error string to tell an idle window from a broken run.
	Failed []failJSON `json:"failed"`
	// Outcome is "found", "empty" or "incomplete". Complete is kept as the
	// boolean it always was -- outcome != incomplete -- so existing consumers
	// keep working; it just no longer has to answer a question it cannot.
	Outcome  string `json:"outcome"`
	Complete bool   `json:"complete"`
	Error    string `json:"error,omitempty"`

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
	// allFunctions is the function table before --top truncates it. The notes
	// rules read it: a rule that exists to say "this profile cannot be
	// attributed" must not be silenced by --top=0, which asks for no function
	// TABLE and says nothing about whether the question should be asked.
	allFunctions []funcJSON
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
	//
	// No omitempty: an absent key and a false one are different claims, and a
	// consumer should read a field rather than match "(unlabeled)" against a
	// name this tool is free to reword.
	//
	// Not a perfect oracle. The flag is still derived from that spelling, so a
	// server genuinely carrying a value of "(unlabeled)" collides with the
	// residual row when both are present. Carrying the flag from the row that
	// synthesised it would fix that; until then this is a better key to read
	// than the string, not a guarantee.
	Unlabeled bool `json:"unlabeled"`
	// DrillDown is the command that narrows the report to this row. Null for
	// the unlabeled row, which no matcher can select, and for a row already
	// pinned by --match. See drillFor.
	//
	// No omitempty, for the same reason Pct has none: an absent key and a null
	// one are different claims, and "there is no command for this row" is a
	// fact worth stating rather than leaving to be inferred from a gap.
	DrillDown *drillJSON `json:"drill_down"`
}

type funcJSON struct {
	// Never truncated. The table cuts names to 60 characters, which made two
	// different frames render identically and the output impossible to map
	// back to a symbol.
	Name string   `json:"name"`
	Cum  float64  `json:"cum"`
	Flat float64  `json:"flat"`
	Pct  *float64 `json:"pct"`
	// Unsymbolized marks the bucket every nameless frame lands in, so a
	// consumer deciding whether a profile is attributable reads a field rather
	// than learning how this tool spells it. Derived from that spelling, with
	// the same caveat as Unlabeled.
	Unsymbolized bool `json:"unsymbolized"`
}

type failJSON struct {
	Group string `json:"group"`
	Error string `json:"error"`
	// Hint is the advice the printed banner gives for this class of failure:
	// raise --timeout, narrow the window, and so on. It used to exist only in
	// the text, so a consumer got the diagnosis and not the remedy.
	Hint string `json:"hint,omitempty"`
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
	case "SECONDS":
		return "seconds"
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
// Two counts come back with it, because the interval is an estimate and an
// estimate can miss in either direction:
//
//   - stale: series with NO scrape inside the window. They contribute nothing.
//     Usually they are scraped less often than the rest, though all the code
//     knows is that the window missed them.
//   - doubled: series with MORE than one scrape inside it, and so counted more
//     than once.
//
// Either way the number is wrong, and a series vanishing from a total -- or
// arriving in it twice -- without a word is what this tool exists to prevent.
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
// The estimate is the median of the per-series median gaps, taking the lower
// median where a count is even, so an even split errs toward the narrower
// window rather than one wide enough to hold two scrapes.
//
// Two medians, not one, and neither of them the smallest gap. The smallest gap
// outright is safer against double-counting but far too easy to break: one
// close-together pair anywhere -- an agent restart, a backfill, a scrape that
// ran early -- collapsed the window for the entire fleet, and then almost
// nothing fell inside it. Measured, a single 5-second gap among 60-second
// scrapes cut a three-target total to one target.
//
// A median within each series handles that when the series is long. It does
// not help when the series is SHORT, which is exactly what a restart or a
// newly-appeared target produces: two timestamps five seconds apart have one
// gap, so their median is five seconds. Taking the smallest of the per-series
// medians let that one new target collapse the window again. The median across
// series ignores it, because most targets agree with each other.
//
// What that costs: a series really scraped faster than the fleet median has
// more than one scrape inside the window and is counted more than once. The
// caller counts those and says so, rather than letting the number be quietly
// wrong.
func latestAndInterval(times [][]time.Time) (latest time.Time, interval time.Duration) {
	var medians []time.Duration
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
		medians = append(medians, lowerMedian(gaps))
	}
	if len(medians) == 0 {
		return latest, 0
	}
	return latest, lowerMedian(medians)
}

// lowerMedian sorts a copy and returns the middle value, the lower of the two
// on an even count.
func lowerMedian(d []time.Duration) time.Duration {
	s := slices.Clone(d)
	slices.Sort(s)
	return s[(len(s)-1)/2]
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
	groups, err := labelValues(ctx, c, o.timeout, o.by, o.match, start, end)
	if err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		err := explainNoValues(ctx, c, o.by, start, end)
		if !errors.Is(err, errEmptyWindow) {
			return nil, err
		}
		// A window with nothing in it still gets a document and a zero exit.
		// The alternative -- the error path -- makes querying last month look
		// exactly like a server that stopped answering.
		return emptyWindowReport(o, profType, typeVerified, start, end, err), nil
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
		TypeVerified: &typeVerified,
		Start:        start.UTC(),
		End:          end.UTC(),
		WindowSecs:   math.Round(window.Seconds()*1000) / 1000,
		GroupBy:      o.by,
		Match:        o.match,
		SortedBy:     sortBy.String(),
		window:       window,
		// Empty slices, not nil: these render as [] rather than null, and an
		// absent finding and an uncomputed one are different claims.
		Notes:     []noteJSON{},
		Groups:    []groupJSON{},
		Functions: []funcJSON{},
		Failed:    []failJSON{},
		// Set here rather than after the fan-out: a collapsed window can
		// leave every group empty, and that is exactly the run where the
		// reader most needs to know scrapes existed and the window missed
		// them.
		Delta:         delta,
		StaleSeries:   staleSeries,
		DoubledSeries: doubledSeries,
	}
	if !snapAt.IsZero() {
		at := snapAt.UTC()
		d.SnapshotAt = &at
	}

	// Everything matching the selector, including series carrying no value for
	// the group-by label. Those exist -- an agent deployed without the label,
	// say -- and summing only the labelled groups would let them vanish from
	// the report while still burning CPU.
	//
	// Bounded like every per-group merge: carrying no matcher at all, this is
	// the widest query in the run and the likeliest to be slow.
	//
	// First, not last. It used to run after the fan-out, when it was only a
	// denominator. It now also carries the metadata that lets the breakdown be
	// priced with one range query instead of one merge per value, so the cheap
	// path cannot start until it has answered.
	overallSel := selector(profType, "", "", o.match)
	octx, ocancel := context.WithTimeout(ctx, o.timeout)
	overallRaw, overallErr := c.MergePprof(octx, overallSel, qstart, qend)
	ocancel()
	// Asked again on a dropped stream, exactly as a group is. A dropped stream
	// is the server going away mid-answer, not the query being wrong.
	//
	// This merge is now the first thing the run does, which is why it needs
	// its own retry: it used to run after the fan-out and a transient failure
	// landed on a group, where the retry pass caught it. Losing this one loses
	// the denominator, the residual, the function table AND -- since the
	// breakdown is priced from its metadata -- the cheap path with it.
	if overallErr != nil && looksTransient(overallErr) && budgetLeftFor(ctx, o.timeout) {
		rctx, rcancel := context.WithTimeout(ctx, o.timeout)
		if again, err := c.MergePprof(rctx, overallSel, qstart, qend); err == nil {
			overallRaw, overallErr = again, nil
			d.RetriedOverall++
		}
		rcancel()
	}
	var overall *profile.Profile
	if overallErr == nil {
		overall, overallErr = parsePprof(overallRaw)
	}
	var overallMetric Metric
	if overall != nil {
		var err error
		if overallMetric, err = interpret(overall, window, delta); err != nil {
			return nil, err
		}
	}

	// The cheap path, when it is available, wanted and sound.
	//
	// Delta only. A snapshot profile is a level measured repeatedly, and the
	// merge path goes to some trouble to put every group on ONE window so the
	// rows and the total describe the same instant -- an instance that stopped
	// scraping ten minutes ago must not report its last heap at full value
	// against a total that excludes it. A range query picks each series'
	// newest sample independently and would undo exactly that, so snapshots
	// keep the fan-out.
	var results []groupResult
	var summed bool
	if delta && overall != nil && !o.fanOut {
		valid := make(map[string]bool, len(groups))
		for _, g := range groups {
			valid[g] = true
		}
		rs, drop, err := sumByGroups(ctx, c, o, profType, overallMetric, qstart, qend, valid)
		if err == nil {
			results, summed, d.DroppedGroups = rs, true, drop
		} else {
			// Not fatal: fall back to the fan-out, which is what this run
			// would have done anyway. Said out loud, because a run that
			// quietly takes a hundred times longer is the kind of thing this
			// tool exists to point out.
			fmt.Fprintf(os.Stderr, "parcareport: cheap breakdown unavailable (%v), merging each %s\n",
				shortErr(err), o.by)
		}
	}

	if !summed {
		results = mergeGroups(ctx, c, o, profType, groups, qstart, qend, window)
	}
	d.Breakdown = "merge"
	if summed {
		d.Breakdown = "sum_by"
	}

	// A dropped stream is the server going away mid-answer, not the query
	// being wrong, and it is transient. Ask again -- but only for the groups
	// that failed that way, and one at a time. Re-running the whole fan-out
	// would double the load on a server that has just said it has none to
	// spare, which is what caused the failure in the first place.
	//
	// Once each. A second failure keeps the first error rather than replacing
	// it with a less informative one, and there is no third attempt: past
	// that, retrying is just the same load again.
	if again := transientGroups(results); !summed && len(again) > 0 {
		// Half of what is left, at most, when something comes after this.
		// Checking only "is there room for one more" let a section with
		// fifteen dropped groups keep passing the check until the budget was
		// nearly gone, and every later section then failed on the deadline --
		// the retry made the run worse than no retry.
		//
		// Only when something comes after it. A plain report is the whole
		// run, so holding half the budget back there would forfeit it for
		// nothing.
		var until time.Time
		if dl, ok := ctx.Deadline(); ok && o.moreToCome {
			until = time.Now().Add(time.Until(dl) / 2)
		}
		// Up to --timeout each, one after another: without a line on stderr
		// this is minutes of silence after the merging bar has cleared, and
		// "still retrying" and "hung" look identical.
		prog := newProgress("retrying", o.by+" groups", len(again))
		prog.start()
		for _, idx := range again {
			if !budgetLeftFor(ctx, o.timeout) {
				break
			}
			if !until.IsZero() && time.Now().After(until) {
				break
			}
			if r := mergeOne(ctx, c, o, profType, groups[idx], qstart, qend, window); r.err == nil {
				results[idx] = r
			}
			d.Retried++
			prog.step()
		}
		prog.stop()
	}

	// Checked once, after every attempt: if the run's budget went while those
	// queries were in flight, every one of them was cut short by it.
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
		// A matched series that profiled zero, or a value that matched nothing.
		// Either way it says nothing, and there can be hundreds; count them,
		// do not list them. Which counter it lands in is decided by whether a
		// --match was set, because that decides what an empty answer *means*.
		//
		// With no --match the value came back from a query that had no
		// matcher, so an empty answer did have no samples.
		//
		// With one, the values all came from the server's UNFILTERED list --
		// the Values API ignores --match -- so an empty answer is far more
		// likely to be the matcher pruning the value than the window being
		// idle. Not certainly: an idle value under a --match lands here too,
		// and nothing cheap tells the two apart. The wording is chosen to be
		// true either way, where "no samples in this window" is the one
		// reading that sends the reader to widen a window that was fine.
		if r.empty || r.value == 0 {
			if o.match != "" {
				d.ExcludedGroups++
			} else {
				d.EmptyGroups++
			}
			continue
		}
		rows = append(rows, Row{Name: r.name, Cores: r.value})
	}
	for _, f := range failed {
		d.Failed = append(d.Failed, failJSON{
			Group: f.group,
			Error: f.msg,
			// The same advice the banner prints, so the document carries the
			// remedy and not only the diagnosis.
			Hint: plainHint([]string{f.msg}, mergeQuery, ctx.Err() != nil),
		})
	}

	if len(rows) == 0 {
		// The unit is still known even with nothing to show: it came from the
		// profile type, not from the rows. An empty "unit" would be the
		// instability the field exists to prevent.
		d.Unit, d.Rate = unitName(groupHeader), groupRate
		if d.Unit == "" {
			// Rate alongside Unit, not just Unit: "cores" with rate:false is a
			// contradiction, and an empty report was emitting exactly that.
			h, r := headerFor(profType)
			d.Unit, d.Rate = unitName(h), r
		}
		v := noRows(o, groups, failed, d.EmptyGroups, d.ExcludedGroups, profType, typeVerified, ctx.Err() != nil)
		d.noRows, d.banner = true, v.banner
		d.Outcome, d.Complete = v.outcome, v.outcome != outcomeIncomplete
		d.Notes = append(d.Notes, v.note)
		if v.err != nil {
			d.Error = v.err.Error()
		}
		return d, v.err
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
		mt := overallMetric
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
	// Name breaks ties so the order is decided by the data rather than by
	// whichever query answered first. Two runs over the same window should
	// print the same table, and equal values are not rare: a residual that
	// happens to match a group, or several idle groups at zero.
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Cores != rows[j].Cores {
			return rows[i].Cores > rows[j].Cores
		}
		return rows[i].Name < rows[j].Name
	})
	for _, r := range rows {
		g := groupJSON{Name: r.Name, Value: r.Cores, Unlabeled: unlabeled >= 0 && r.Name == "(unlabeled)"}
		g.DrillDown = drillFor(o, r.Name, g.Unlabeled)
		if knowTotal {
			pct := r.Pct
			g.Pct = &pct
		}
		d.Groups = append(d.Groups, g)
	}

	// Computed whenever there is a profile to compute it from, not only when a
	// function TABLE was asked for: --top=0 suppresses the table, and used to
	// suppress the notes rules that read it -- including the one whose whole
	// job is to say the profile cannot be attributed to code.
	if overall != nil {
		fns, err := topFunctions(overall, window, sortBy, delta)
		if err != nil {
			return nil, err
		}
		for _, r := range fns {
			f := funcJSON{Name: r.Name, Cum: r.Cores, Flat: r.Flat, Unsymbolized: r.Name == unsymbolizedName}
			if grand > 0 {
				v := r.Flat
				if sortBy == sortCum {
					v = r.Cores
				}
				pct := v / grand * 100
				f.Pct = &pct
			}
			d.allFunctions = append(d.allFunctions, f)
		}
		if o.top > 0 {
			d.Functions = d.allFunctions
			if len(d.Functions) > o.top {
				d.Functions = d.Functions[:o.top]
			}
		}
	}
	// After the groups and the functions: every rule is a share of numbers
	// that must already be in place.
	d.Notes = notesFor(o, d)

	if overallErr != nil {
		d.Failed = append(d.Failed, failJSON{
			Group: "(overall)",
			Error: shortErr(overallErr),
			Hint:  plainHint([]string{shortErr(overallErr)}, mergeQuery, ctx.Err() != nil),
		})
	}
	d.Complete = len(failed) == 0 && overallErr == nil
	d.Outcome = outcomeFound
	if !d.Complete {
		// Rows were produced, but something was not answered, so what is
		// missing is unknown. That is a different claim from "nothing is
		// missing" and from "there was nothing to find".
		d.Outcome = outcomeIncomplete
	}
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
// noRowsVerdict is why a run produced no rows, in the four forms the callers
// need: prose for a person, a structured note for a consumer, an outcome, and
// an error that is nil unless the run genuinely could not look.
type noRowsVerdict struct {
	banner  string
	note    noteJSON
	outcome string
	err     error
}

// noRows classifies a run that produced nothing. Four causes, and they are not
// the same event: two are answers and two are failures to get one.
func noRows(o options, groups []string, failed []failure, empty, excluded int, profType string, typeVerified bool, runExpired bool) noRowsVerdict {
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
			// The survivors are not one thing when a --match is set: some were
			// pruned by it and some were genuinely idle. Calling all of them
			// "no samples" here would reintroduce, in this branch, the very
			// conflation the rest of the function now avoids.
			rest := fmt.Sprintf("%d had no samples", empty)
			switch {
			case excluded > 0 && empty > 0:
				rest = fmt.Sprintf("%d yielded nothing under --match and %d had no samples", excluded, empty)
			case excluded > 0:
				rest = fmt.Sprintf("%d yielded nothing under --match", excluded)
			}
			fmt.Fprintf(&b, "!! FAILED: %d of %d %s queries failed and the other %s,\n"+
				"!! so there is nothing to report. Whether this window is idle is unknown:\n"+
				"!! the failed queries were never answered.\n",
				len(failed), len(groups), o.by, rest)
		}
		b.WriteString(formatFailures(failed, mergeQuery, runExpired))
		return noRowsVerdict{
			banner:  b.String(),
			outcome: outcomeIncomplete,
			note: noteJSON{
				Code: "no_rows_queries_failed",
				Message: fmt.Sprintf("%d of %d %s queries failed and nothing came back. "+
					"Whether this window is idle is unknown: the failed queries were never answered.",
					len(failed), len(groups), o.by),
			},
			err: fmt.Errorf("%d of %d %s queries failed; no results", len(failed), len(groups), o.by),
		}
	}
	if !typeVerified {
		// The warning about this went only to stderr, which is the case the
		// project refuses: under 2>/dev/null a typo'd selector left an empty
		// stdout that reads as an idle cluster.
		return noRowsVerdict{
			banner: "!! No data, and the profile type was never verified against the\n" +
				"!! server -- that lookup failed. A selector this server does not offer\n" +
				"!! looks exactly like this. Check it with `parcareport types`.\n",
			outcome: outcomeIncomplete,
			note: noteJSON{
				Code: "profile_type_unverified",
				Message: fmt.Sprintf("No data, and %q was never checked against the server "+
					"because that lookup failed. A selector this server does not offer looks "+
					"exactly like an idle window, so this is not evidence of either.", profType),
				Command: "parcareport types",
			},
			err: fmt.Errorf("no data in this window, and %q was never verified", profType),
		}
	}
	if excluded > 0 {
		// Some or all of the empty groups were pruned by --match rather than
		// idle. "no data in this window" is then the wrong conclusion -- the
		// window is fine, the matcher is what removed them -- and it is the
		// single most likely outcome of a typo'd --match.
		//
		// Worded for the partial case too: some values pruned, the rest
		// genuinely idle reads very differently from all of them pruned.
		//
		// Reaching here with empty > 0 needs a --match to have set
		// ExcludedGroups on some values while EmptyGroups got others -- and
		// a --match sends EVERY zero group to ExcludedGroups, whatever the
		// cause, so in practice empty is 0 and the first form is the one
		// printed. The second is kept because that is a property of the
		// counter, not of this message: if the classification ever narrows,
		// the sentence is already right instead of silently saying "all N".
		what := fmt.Sprintf("all %d", excluded)
		if empty > 0 {
			what = fmt.Sprintf("%d of %d", excluded, excluded+empty)
		}
		return noRowsVerdict{
			banner: fmt.Sprintf("!! No rows: %s %s values yielded nothing under --match '%s'.\n"+
				"!! The label itself exists, so the values came from the server and the\n"+
				"!! matcher is the likelier reason they yielded nothing than an idle window.\n"+
				"!! That is also what a typo'd --match looks like. Check it with\n"+
				"!! `parcareport labels`.\n",
				what, o.by, o.match),
			// Every query was answered. The answer was nothing, which is a
			// measurement, not a malfunction -- so the run succeeded and says
			// loudly why the result looks like this.
			outcome: outcomeEmpty,
			note: noteJSON{
				Code: "match_pruned_everything",
				Message: fmt.Sprintf("%s %s values yielded nothing under --match %q. The label "+
					"exists and the values came from the server, so the matcher is a likelier "+
					"reason than an idle window -- and this is what a typo'd --match looks like.",
					what, o.by, o.match),
				Command: "parcareport labels " + shellQuote(o.by),
			},
		}
	}
	// Say so on stdout as well. A section heading followed by silence reads
	// as truncated output, especially in an overview where other sections did
	// produce tables.
	//
	// And say it as a SUCCESS. Every query was answered; the answer was that
	// nothing ran. Reporting that as a failure is the conflation this whole
	// distinction exists to end: a script that wraps this tool saw exit 1 for
	// a correct measurement of an idle window.
	return noRowsVerdict{
		banner:  "(no data in this window)\n",
		outcome: outcomeEmpty,
		note: noteJSON{
			Code: "empty_window",
			Message: "Every query was answered and none of them found samples. This window " +
				"is idle as far as this selector is concerned -- which is a measurement, " +
				"not a failure. Widen --from, or check that an agent is still writing.",
		},
	}
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

// groupResult is one group's merge: its value, or why it has none.
type groupResult struct {
	name   string
	value  float64
	header string
	rate   bool
	// empty is set when the server returned a profile with no bytes, i.e. the
	// selector matched no series. Distinct from value == 0, which is a series
	// that matched and genuinely profiled zero.
	empty bool
	err   error
}

// mergeGroups merges one profile per label value, up to concurrency at a time,
// and returns a result per group in the order given.
func mergeGroups(ctx context.Context, c *Client, o options, profType string, groups []string,
	start, end time.Time, window time.Duration) []groupResult {
	results := make([]groupResult, len(groups))

	// A run can take minutes. With no output at all, "still merging" and
	// "hung" look identical -- and with the default --timeout the first thing
	// you saw could be an error after a minute of silence.
	prog := newProgress("merging", o.by+" groups", len(groups))
	prog.start()
	defer prog.stop()

	sem := make(chan struct{}, max(1, o.concurrency))
	var wg sync.WaitGroup
	for i, g := range groups {
		wg.Add(1)
		go func(i int, g string) {
			defer wg.Done()
			defer prog.step()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = mergeOne(ctx, c, o, profType, g, start, end, window)
		}(i, g)
	}
	wg.Wait()
	return results
}

// mergeOne merges a single group, bounded by --timeout.
func mergeOne(ctx context.Context, c *Client, o options, profType, g string,
	start, end time.Time, window time.Duration) groupResult {
	qctx, qcancel := context.WithTimeout(ctx, o.timeout)
	defer qcancel()
	raw, err := c.MergePprof(qctx, selector(profType, o.by, g, o.match), start, end)
	if err != nil {
		return groupResult{name: g, err: err}
	}
	p, err := parsePprof(raw)
	if err != nil {
		return groupResult{name: g, err: err}
	}
	if p == nil {
		// The server answered with no bytes at all: the selector matched no
		// series. With no --match this means the value has no samples in the
		// window, but WITH one it most often means --match pruned the value
		// away -- the value came from the server's full list, which knows
		// nothing about --match. mergeGroups decides which, since only it can
		// see whether a --match was set. Carried as a flag rather than an
		// error because an empty answer is a normal, successful outcome.
		return groupResult{name: g, empty: true}
	}
	m, err := interpret(p, window, isDeltaType(profType))
	return groupResult{name: g, value: m.Value, header: m.Header, rate: m.Rate, err: err}
}

// transientGroups returns the indexes of groups that failed in a way worth one
// more attempt.
func transientGroups(results []groupResult) []int {
	var idx []int
	for i, r := range results {
		if r.err != nil && looksTransient(r.err) {
			idx = append(idx, i)
		}
	}
	return idx
}

// budgetLeftFor reports whether the run's budget has room for another query:
// more than two timeouts' worth, so a retry cannot be started that only has
// time to fail on the deadline.
//
// This is the per-query guard. The retry pass has a second one -- at most half
// the remaining budget, when sections follow -- and which of the two binds
// depends on the numbers: this one first when the deadline is tight against
// --timeout, the half-budget cap first when it is generous.
func budgetLeftFor(ctx context.Context, timeout time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	dl, ok := ctx.Deadline()
	if !ok {
		return true
	}
	return time.Until(dl) > 2*timeout
}

// labelValues lists a label's values, asking again once if the server dropped
// the answer. Losing this one costs the whole report rather than one group:
// there is nothing left to break anything down by.
func labelValues(ctx context.Context, c *Client, timeout time.Duration, by, match string, start, end time.Time) ([]string, error) {
	vals, err := c.LabelValues(ctx, by, match, start, end)
	if err == nil || !looksTransient(err) || !budgetLeftFor(ctx, timeout) {
		return vals, err
	}
	// Keep the first error if the second attempt fails too. A deadline that
	// expired during the retry would otherwise replace the RST_STREAM that
	// explains the failure with a "context deadline exceeded" that does not.
	again, againErr := c.LabelValues(ctx, by, match, start, end)
	if againErr != nil {
		return vals, err
	}
	return again, nil
}

// looksTransient reports whether a failure is the server dropping the
// connection rather than rejecting the request. Those are worth one retry;
// a bad selector or an empty window is not.
func looksTransient(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, s := range []string{"RST_STREAM", "INTERNAL_ERROR", "unavailable", "Unavailable",
		"connection reset", "error reading from server", "server preface"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// sumByGroups prices every label value with ONE range query instead of one
// merge each.
//
// A merge reconstructs every distinct stacktrace so it can return a profile.
// Summing a label needs none of that, and the server will do it with sum_by.
// The cost stops scaling with the number of values, which is what made the
// breakdown unusable: `--by=comm` against a real server has 1486 values, and a
// run that merged each of them had 1034 die on the deadline.
//
// The catch is units. The range API answers in raw sample values and knows
// nothing about periods, so a CPU profile comes back as a count of stack
// samples with no way to turn it into cores. The unfiltered merge does carry
// that metadata, so its Value/Raw ratio is the conversion -- which is why this
// runs after it and only when it succeeded.
//
// That ratio is built from ONE sampling period, the merged profile's. It is a
// per-target property, so a fleet whose agents run at different frequencies is
// priced wrongly here, and the rows give no sign of it: they are scaled by the
// same factor as the total, so they sum to it however wrong the factor is. The
// fan-out priced each group from its own profile and such a fleet made the
// percentages exceed 100 -- visibly wrong, which is worth more than silently
// consistent. --fan-out exists to get that cross-check back, and the report
// says which method it used.
//
// What CAN be checked is that the two queries saw the same data, and that is
// worth doing: it catches a server that bucketed or truncated the range
// response, and a sum over a different sample-type column than the merge
// summed. Both would otherwise be invisible.
func sumByGroups(ctx context.Context, c *Client, o options, profType string,
	overall Metric, start, end time.Time, valid map[string]bool,
) ([]groupResult, int, error) {
	if overall.Raw == 0 {
		// Nothing to scale by, and dividing would give every row an infinity.
		return nil, 0, errors.New("the unfiltered merge summed to zero, so raw values cannot be converted")
	}

	sums, err := sumByOnce(ctx, c, o, profType, start, end)
	if err != nil && looksTransient(err) && budgetLeftFor(ctx, o.timeout) {
		// One retry, as every other query in this run gets. A dropped stream
		// is the server going away mid-answer, and giving up here costs a
		// merge per label value -- the very thing this exists to avoid.
		sums, err = sumByOnce(ctx, c, o, profType, start, end)
	}
	if err != nil {
		return nil, 0, err
	}

	// Every series the server returned, including the ones carrying no value
	// for this label. If that does not match what the merge summed, the two
	// queries did not see the same data and no scaling of one describes the
	// other.
	var rawAll float64
	for _, g := range sums {
		if g.Aggregated {
			return nil, 0, errors.New("the server folded several profiles into one data point, " +
				"and whether that point carries their sum is not something the API promises")
		}
		rawAll += g.Total
	}
	if rawAll == 0 {
		return nil, 0, errors.New("the range query summed to zero where the merge did not")
	}
	if d := math.Abs(rawAll-overall.Raw) / overall.Raw; d > sumByTolerance {
		return nil, 0, fmt.Errorf("the range query summed to %.0f where the merge summed to %.0f, "+
			"a %.0f%% difference, so they did not see the same data", rawAll, overall.Raw, d*100)
	}

	// Normalised against the range query's OWN total, not the merge's.
	//
	// The two are separate queries. Against a window still being written they
	// routinely differ by a per cent or so -- profiles for it are still
	// arriving between one call and the next -- and dividing by the merge's
	// total would push that difference straight into every row, leaving them
	// summing to something other than 100%. Dividing by rawAll makes the rows
	// the range query's own proportions, carried onto the total the merge
	// measured: internally consistent, and immune to a scrape landing between
	// the two calls.
	//
	// The check above is therefore for gross disagreement -- a truncated
	// response, a different sample-type column -- not for drift.
	scale := overall.Value / rawAll
	seen := make(map[string]bool, len(sums))
	dropped := 0
	out := make([]groupResult, 0, len(sums))
	for _, g := range sums {
		if g.Value == "" {
			// Series carrying no value for this label. Left out so the
			// residual is derived by subtraction exactly as the fan-out
			// derives it, and counted into rawAll above so the check can see
			// them.
			continue
		}
		if !valid[g.Value] {
			// A value the label list did not have. Counted, not silently
			// folded into the residual: that row would then say this CPU
			// carries no label, when we were told its name and discarded it.
			dropped++
			continue
		}
		seen[g.Value] = true
		out = append(out, groupResult{
			name:   g.Value,
			value:  g.Total * scale,
			header: overall.Header,
			rate:   overall.Rate,
		})
	}

	// A value the label list had and the range query did not answer for has no
	// samples in this window. The fan-out learned that by merging it and
	// getting nothing back; here it is an absence, and saying nothing about it
	// would drop the "N values had no samples" and "N values yielded nothing
	// under --match" lines that exist to stop a reader blaming the wrong thing.
	for v := range valid {
		if !seen[v] {
			out = append(out, groupResult{name: v, empty: true})
		}
	}
	return out, dropped, nil
}

// sumByTolerance is how far the range query's total may sit from the merge's
// before the two are treated as describing different data rather than the same
// data seen a moment apart.
//
// Deliberately loose. Small differences are normal and are handled by
// normalising (see sumByGroups), so this is only a guard against the failures
// that would make the whole conversion meaningless: a truncated or downsampled
// response, or a sum over a different column than the merge summed. Those are
// not subtle. Measured against a live server, two calls seconds apart over a
// window still being written differed by 1.1%.
const sumByTolerance = 0.10

func sumByOnce(ctx context.Context, c *Client, o options, profType string,
	start, end time.Time,
) ([]GroupTotal, error) {
	qctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()
	return c.SumByLabel(qctx, selector(profType, "", "", o.match), o.by, start, end)
}

// headerFor is the column heading a profile type would produce, without a
// profile to interpret.
//
// Used only when there are no rows: the unit is a property of the selector, so
// a report with nothing in it still knows what its numbers would have meant,
// and a consumer switching on `unit` should not have to handle an empty
// string.
//
// It mirrors interpret() by reading the selector's parts rather than looking
// for substrings anywhere in it. A substring version got wallclock wrong --
// `parca_agent:wallclock:nanoseconds:samples:count:delta` contains
// ":samples:count:" and was therefore read as CPU, so an empty off-CPU report
// announced its unit as cores. Off-CPU time is emphatically not cores, and
// saying so is the specific mistake this tool warns about elsewhere.
func headerFor(profType string) (header string, rate bool) {
	parts := strings.Split(profType, ":")
	if len(parts) < 5 {
		return "COUNT", false
	}
	sampleType, sampleUnit := parts[1], parts[2]
	periodType, periodUnit := parts[3], parts[4]
	isCPU := sampleType == "cpu" || periodType == "cpu"

	switch {
	case sampleUnit == "bytes":
		return "BYTES", false
	case sampleUnit == "count":
		// A count with a time-based period is a sampled duration
		// (parca-agent's CPU profile); a count with no such period is a real
		// count.
		if isTimeUnit(periodUnit) {
			return "CORES", true
		}
		return "COUNT", false
	case isTimeUnit(sampleUnit):
		if isCPU {
			return "CORES", true
		}
		if !isDeltaType(profType) {
			// Cumulative since process start, so there is no window to
			// average over.
			return "SECONDS", false
		}
		return "BLOCKED", true
	}
	return "COUNT", false
}

// emptyWindowReport is the document for a window the server had nothing in.
//
// It exists because returning only an error made an idle window and a broken
// run the same event to a caller: same exit code, same complete:false, and a
// sentence to tell them apart. The run worked; what it found was nothing.
func emptyWindowReport(o options, profType string, typeVerified bool, start, end time.Time, why error) *reportData {
	window := end.Sub(start)
	h, rate := headerFor(profType)
	return &reportData{
		ProfileType:  profType,
		TypeVerified: &typeVerified,
		Start:        start.UTC(),
		End:          end.UTC(),
		WindowSecs:   math.Round(window.Seconds()*1000) / 1000,
		GroupBy:      o.by,
		Match:        o.match,
		// The unit is a property of the selector, so it is known even with
		// nothing to show -- and Rate with it, since "cores" with rate:false
		// is a contradiction.
		Unit:     unitName(h),
		Rate:     rate,
		Delta:    isDeltaType(profType),
		SortedBy: o.sortBy,
		Outcome:  outcomeEmpty,
		Complete: true,
		Notes: []noteJSON{{
			Code: "empty_window",
			Message: explainedEmpty(why) +
				" Every query was answered, so this is a measurement of an idle window, " +
				"not a failure to take one.",
		}},
		Groups:    []groupJSON{},
		Functions: []funcJSON{},
		Failed:    []failJSON{},
		// Nothing was priced, by either method. Spelled the same way the
		// error document spells it, so "no breakdown happened" has one name.
		Breakdown: breakdownNone,
		window:    window,
		noRows:    true,
		banner:    "(no data in this window)\n",
	}
}

// explainedEmpty strips the errEmptyWindow tag from the message a reader sees.
// The sentinel is how the callers classify the answer; "empty window:" is not
// a phrase the tool uses anywhere else and reads as an error class.
func explainedEmpty(why error) string {
	msg := why.Error()
	if rest, ok := strings.CutPrefix(msg, errEmptyWindow.Error()+": "); ok {
		return strings.ToUpper(rest[:1]) + rest[1:]
	}
	return msg
}
