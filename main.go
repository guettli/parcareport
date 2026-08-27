// parcareport turns a Parca server into a cross-cluster CPU bottleneck report.
//
// One Parca server usually collects from agents in several clusters. Its web UI
// is great for exploring one flamegraph; it is not great for answering "where
// did my CPU go last night, across everything". This tool answers that on the
// command line, normalized so differently-sized clusters are comparable.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const defaultAddr = "localhost:7070"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "parcareport: %v\n", err)
		os.Exit(1)
	}
}

type options struct {
	addr        string
	insecure    bool
	from        string
	to          string
	by          string
	profileType string
	match       string
	top         int
	concurrency int
	timeout     time.Duration
}

func run(args []string) error {
	var o options
	fs := flag.NewFlagSet("parcareport", flag.ContinueOnError)
	fs.StringVar(&o.addr, "url", defaultAddr, "Parca server gRPC address (host:port)")
	fs.BoolVar(&o.insecure, "insecure", true, "use a plaintext connection")
	fs.StringVar(&o.from, "from", "-1h", "window start: RFC3339, or relative like -6h / -30m")
	fs.StringVar(&o.to, "to", "now", "window end: RFC3339, or 'now'")
	fs.StringVar(&o.by, "by", "cluster", "label to break the report down by (e.g. cluster, node, comm)")
	fs.StringVar(&o.profileType, "profile-type", "", "profile type selector (default: auto-detect when the server offers exactly one)")
	fs.StringVar(&o.match, "match", "", `extra label matchers, e.g. 'cluster="tc",comm="clickhouse"'`)
	fs.IntVar(&o.top, "top", 15, "how many functions to list (0 disables the function table)")
	fs.IntVar(&o.concurrency, "concurrency", 4, "parallel merge queries")
	fs.DurationVar(&o.timeout, "timeout", 60*time.Second, "per-query timeout; a slow group fails visibly instead of stalling the run")
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), usage)
		fs.PrintDefaults()
	}

	// Subcommands, and the one argument `labels` takes, come before flags:
	// `parcareport labels comm --url=...`.
	//
	// Both have to be peeled off here, not just the subcommand. Go's flag
	// package stops parsing at the first non-flag argument, so with only the
	// subcommand removed everything after `comm` became an ignored positional
	// -- including --url, which silently sent the query to localhost instead
	// of the server that was asked for.
	var positional []string
	for len(args) > 0 && !strings.HasPrefix(args[0], "-") && len(positional) < 2 {
		positional = append(positional, args[0])
		args = args[1:]
	}
	sub, subArg := "", ""
	if len(positional) > 0 {
		sub = positional[0]
	}
	if len(positional) > 1 {
		subArg = positional[1]
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	start, end, err := parseWindow(o.from, o.to)
	if err != nil {
		return err
	}

	c, err := Dial(o.addr, o.insecure, o.timeout)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	switch sub {
	case "", "report":
		if subArg != "" {
			return fmt.Errorf("report takes no argument, got %q (break it down with --by=%s)", subArg, subArg)
		}
		return report(ctx, c, o, start, end)
	case "labels":
		// Flags may also precede the name (`labels --url=x comm`), in which
		// case flag parsing leaves it as the first positional.
		if subArg == "" {
			subArg = fs.Arg(0)
		}
		return listLabels(ctx, c, subArg, start, end)
	case "types":
		if subArg != "" {
			return fmt.Errorf("types takes no argument, got %q", subArg)
		}
		names, err := c.ProfileTypeNames(ctx)
		if err != nil {
			return err
		}
		for _, n := range names {
			fmt.Println(n)
		}
		return nil
	default:
		return fmt.Errorf("unknown command %q (want: report, labels, types)", sub)
	}
}

// listLabels summarizes label names, or dumps one label's values in full.
// Summarizing by default matters: a label like `comm` has thousands of values,
// and printing them all turns a discovery command into a wall of text.
func listLabels(ctx context.Context, c *Client, name string, start, end time.Time) error {
	if name != "" {
		vals, err := c.LabelValues(ctx, name, start, end)
		if err != nil {
			return err
		}
		if len(vals) == 0 {
			// Printing nothing and exiting 0 is the worst of the three
			// readings: a typo'd label, an empty window and a failed query all
			// looked like success. Say which one it is.
			return explainNoValues(ctx, c, name, start, end)
		}
		sort.Strings(vals)
		for _, v := range vals {
			fmt.Println(v)
		}
		return nil
	}

	names, err := c.LabelNames(ctx, start, end)
	if err != nil {
		return err
	}
	w := newTab()
	fmt.Fprintf(w, "LABEL\tVALUES\tSAMPLE\n")
	for _, n := range names {
		vals, err := c.LabelValues(ctx, n, start, end)
		if err != nil {
			return err
		}
		sort.Strings(vals)
		sample := vals
		suffix := ""
		if len(sample) > 6 {
			sample, suffix = sample[:6], " …"
		}
		fmt.Fprintf(w, "%s\t%d\t%s%s\n", n, len(vals), strings.Join(sample, " "), suffix)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	fmt.Println("\nRun `parcareport labels <name>` to list one label's values in full.")
	return nil
}

func report(ctx context.Context, c *Client, o options, start, end time.Time) error {
	profType, err := resolveProfileType(ctx, c, o.profileType)
	if err != nil {
		return err
	}

	groups, err := c.LabelValues(ctx, o.by, start, end)
	if err != nil {
		return err
	}
	if len(groups) == 0 {
		return explainNoValues(ctx, c, o.by, start, end)
	}
	sort.Strings(groups)

	window := end.Sub(start)
	fmt.Printf("%s  %s .. %s  (%s)\n\n",
		profType, start.Format(time.RFC3339), end.Format(time.RFC3339), window.Round(time.Second))

	type result struct {
		name  string
		cores float64
		err   error
	}
	results := make([]result, len(groups))

	sem := make(chan struct{}, max(1, o.concurrency))
	var wg sync.WaitGroup
	for i, g := range groups {
		wg.Add(1)
		go func(i int, g string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			qctx, qcancel := context.WithTimeout(ctx, o.timeout)
			defer qcancel()
			sel := selector(profType, o.by, g, o.match)
			raw, err := c.MergePprof(qctx, sel, start, end)
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
			results[i] = result{name: g, cores: m.Value, err: err}
		}(i, g)
	}
	wg.Wait()

	rows := make([]Row, 0, len(results))
	var total float64
	empty := 0
	var failed []string
	for _, r := range results {
		if r.err != nil {
			// Collected, not just warned about. A warning on stderr vanishes
			// under `2>/dev/null` and leaves a table that looks complete but
			// whose totals and percentages silently omit whatever failed.
			failed = append(failed, fmt.Sprintf("%s=%s: %s", o.by, r.name, shortErr(r.err)))
			continue
		}
		total += r.cores
		// A label value with no samples in the window says nothing, and there
		// can be hundreds of them -- every `comm` on the box when the profile
		// came from a scrape target, for instance. Count them, do not print
		// them.
		if r.cores == 0 {
			empty++
			continue
		}
		rows = append(rows, Row{Name: r.name, Cores: r.cores})
	}
	if len(rows) == 0 {
		// Distinguish "the window is genuinely empty" from "every query
		// failed". Reporting the second as the first is how a broken run gets
		// mistaken for an idle cluster.
		if len(failed) > 0 {
			printFailures(failed)
			return fmt.Errorf("all %d %s queries failed; no results", len(failed), o.by)
		}
		return errors.New("no data in this window")
	}
	// Everything matching the selector, including series that carry no value
	// for the group-by label at all. Those exist -- an agent deployed without
	// the label, say -- and if we only ever summed the labelled groups they
	// would vanish from the report while still burning CPU. Reporting the
	// residual as "(unlabeled)" keeps the table honest and adds up to 100%.
	overallSel := selector(profType, "", "", o.match)
	overallRaw, err := c.MergePprof(ctx, overallSel, start, end)
	if err != nil {
		return err
	}
	overall, err := parsePprof(overallRaw)
	if err != nil {
		return err
	}
	grand := total
	header := "CORES"
	if overall != nil {
		m, err := interpret(overall, window)
		if err != nil {
			return err
		}
		header = m.Header
		grand = m.Value
		if residual := grand - total; residual > grand*0.001 {
			rows = append(rows, Row{Name: "(unlabeled)", Cores: residual})
		}
	}
	if grand > 0 {
		for i := range rows {
			rows[i].Pct = rows[i].Cores / grand * 100
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Cores > rows[j].Cores })
	printGroupTable(strings.ToUpper(o.by), header, rows, grand)
	if empty > 0 {
		fmt.Printf("(%d %s values had no samples in this window, omitted)\n", empty, o.by)
	}
	if len(failed) > 0 {
		// On stdout, next to the numbers it invalidates -- never only stderr.
		fmt.Printf("\n!! INCOMPLETE: %d of %d %s queries failed. The totals and\n"+
			"!! percentages above EXCLUDE them and are therefore wrong.\n",
			len(failed), len(groups), o.by)
		printFailures(failed)
	}

	if o.top > 0 && overall != nil {
		fmt.Println()
		fns, err := topFunctions(overall, window)
		if err != nil {
			return err
		}
		// Percentages are against the same profile the functions came from,
		// so CUM for a root frame approaches 100% rather than exceeding it.
		printFunctionTable(fns, header, o.top, grand)
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d of %d %s queries failed; results above are incomplete",
			len(failed), len(groups), o.by)
	}
	return nil
}

// explainNoValues turns an empty label-values response into a specific
// conclusion instead of the most convenient one.
//
// The values query answers "no values" for several unrelated reasons, and at
// this layer they are indistinguishable: the label does not exist, the window
// holds nothing at all, or the query quietly came back short. The old message
// asserted the first without checking -- and a retry, once, produced values
// for the very label it had just declared empty. So cross-check against the
// label names in the same window and name the reason.
//
// The cross-check is one cheap query, and it only runs on a path that is
// already about to fail.
func explainNoValues(ctx context.Context, c *Client, label string, start, end time.Time) error {
	window := fmt.Sprintf("%s .. %s", start.Format(time.RFC3339), end.Format(time.RFC3339))

	names, err := c.LabelNames(ctx, start, end)
	if err != nil {
		return fmt.Errorf("label %q returned no values, and the cross-check failed too, so "+
			"this is more likely a server problem than an empty window -- retry before believing it: %w",
			label, err)
	}
	if len(names) == 0 {
		return fmt.Errorf("the server has no labels at all in %s: nothing was written in this window. "+
			"Widen --from/--to, or check that an agent is still writing", window)
	}
	for _, n := range names {
		if n == label {
			// Parca listed the label for this window, so at least one series
			// carries it -- and a label cannot exist without a value. The two
			// answers contradict each other, which points at the values query,
			// not at the data.
			return fmt.Errorf("label %q exists in %s but returned no values, which is contradictory: "+
				"the values query most likely failed rather than found nothing. Retry it", label, window)
		}
	}
	sort.Strings(names)
	return fmt.Errorf("no label %q in %s; the server has: %s", label, window, strings.Join(names, ", "))
}

func resolveProfileType(ctx context.Context, c *Client, want string) (string, error) {
	names, err := c.ProfileTypeNames(ctx)
	if err != nil {
		return "", err
	}
	if want != "" {
		for _, n := range names {
			if n == want {
				return n, nil
			}
		}
		return "", fmt.Errorf("profile type %q not offered by this server; available:\n  %s", want, strings.Join(names, "\n  "))
	}
	switch len(names) {
	case 0:
		return "", errors.New("server offers no profile types -- is any agent writing to it?")
	case 1:
		return names[0], nil
	default:
		return "", fmt.Errorf("server offers %d profile types; pick one with --profile-type:\n  %s",
			len(names), strings.Join(names, "\n  "))
	}
}

// selector builds the PromQL-ish series selector Parca expects:
//
//	parca_agent:samples:count:cpu:nanoseconds:delta{cluster="tc"}
func selector(profType, label, value, extra string) string {
	var matchers []string
	if label != "" {
		matchers = append(matchers, fmt.Sprintf("%s=%q", label, value))
	}
	if extra != "" {
		matchers = append(matchers, extra)
	}
	if len(matchers) == 0 {
		return profType
	}
	return fmt.Sprintf("%s{%s}", profType, strings.Join(matchers, ","))
}

// parseWindow accepts RFC3339 or a relative offset from now ("-6h").
func parseWindow(from, to string) (time.Time, time.Time, error) {
	now := time.Now()
	end, err := parseTime(to, now)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("--to: %w", err)
	}
	start, err := parseTime(from, now)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("--from: %w", err)
	}
	if !start.Before(end) {
		return time.Time{}, time.Time{}, fmt.Errorf("--from (%s) must be before --to (%s)", start.Format(time.RFC3339), end.Format(time.RFC3339))
	}
	return start, end, nil
}

func parseTime(s string, now time.Time) (time.Time, error) {
	if s == "" || s == "now" {
		return now, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is neither RFC3339 nor a duration like -6h", s)
	}
	if d > 0 {
		d = -d // "6h" reads as "6h ago", same as "-6h"
	}
	return now.Add(d), nil
}

// printFailures lists what broke, capped so a wholesale outage does not bury
// the numbers under hundreds of identical lines.
func printFailures(failed []string) {
	const show = 5
	for i, f := range failed {
		if i == show {
			fmt.Printf("!!   ... and %d more\n", len(failed)-show)
			break
		}
		fmt.Printf("!!   %s\n", f)
	}
}

// shortErr trims the gRPC boilerplate so the reason is readable at a glance.
func shortErr(err error) string {
	msg := err.Error()
	if i := strings.LastIndex(msg, "desc = "); i >= 0 {
		return msg[i+len("desc = "):]
	}
	if i := strings.LastIndex(msg, ": "); i >= 0 {
		return msg[i+2:]
	}
	return msg
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

const usage = `parcareport - cross-cluster CPU bottleneck report from a Parca server

Usage:
  parcareport [report] [flags]   break CPU down by a label, then list hot functions
  parcareport labels [name]      summarize labels, or list one label's values
  parcareport types [flags]      list profile types the server offers

CORES is average cores busy over the window: CPU-seconds / wall-seconds. It is
comparable across clusters of different sizes, unlike raw sample counts.

Flags:
`
