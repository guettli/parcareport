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

	"github.com/google/pprof/profile"
)

const (
	defaultAddr = "localhost:7070"
	// defaultSortBy is named so a test can pin it: the point of this default
	// is that it is flat, and flipping it back is the regression to catch.
	defaultSortBy = "flat"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "parcareport: %v\n", err)
		os.Exit(1)
	}
}

type options struct {
	addr         string
	insecure     bool
	from         string
	to           string
	by           string
	profileType  string
	match        string
	top          int
	sortBy       string
	concurrency  int
	timeout      time.Duration
	bearerToken  string
	tokenFile    string
	username     string
	password     string
	passwordFile string
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
	fs.StringVar(&o.sortBy, "sort", defaultSortBy, "order functions by 'flat' (self time) or 'cum' (cumulative)")
	fs.IntVar(&o.concurrency, "concurrency", 4, "parallel queries: group merges, and the labels fan-out")
	fs.DurationVar(&o.timeout, "timeout", 60*time.Second, "per-query timeout; a slow group fails visibly instead of stalling the run")
	fs.StringVar(&o.bearerToken, "bearer-token", "", "Authorization: Bearer token (requires --insecure=false)")
	fs.StringVar(&o.tokenFile, "bearer-token-file", "", "read the bearer token from a file, keeping it out of the process list")
	fs.StringVar(&o.username, "username", "", "basic auth username (requires --insecure=false)")
	fs.StringVar(&o.password, "password", "", "basic auth password; prefer --password-file")
	fs.StringVar(&o.passwordFile, "password-file", "", "read the basic auth password from a file, keeping it out of the process list")
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

	auth, err := o.auth()
	if err != nil {
		return err
	}

	c, err := Dial(o.addr, o.insecure, o.timeout, auth)
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
		return listLabels(ctx, c, subArg, start, end, o.concurrency)
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

// auth assembles the credentials from the flags.
//
// A token on the command line is visible in the process list to anyone on the
// box, so --bearer-token-file exists and wins when both are given.
func (o options) auth() (Auth, error) {
	a := Auth{BearerToken: o.bearerToken, Username: o.username, Password: o.password}
	if o.tokenFile != "" {
		v, err := readSecretFile("--bearer-token-file", o.tokenFile)
		if err != nil {
			return Auth{}, err
		}
		a.BearerToken = v
	}
	if o.passwordFile != "" {
		v, err := readSecretFile("--password-file", o.passwordFile)
		if err != nil {
			return Auth{}, err
		}
		a.Password = v
	}
	if a.BearerToken != "" && a.Username != "" {
		return Auth{}, errors.New("pass either a bearer token or basic auth, not both")
	}
	// A password with no username produces no header at all, so the request
	// would go out unauthenticated and come back as a bare 401 that says
	// nothing about the flag having been ignored. Fail here instead.
	if a.Password != "" && a.Username == "" {
		return Auth{}, errors.New("--password needs --username; on its own it is not a credential")
	}
	// RFC 7617 gives the colon to the first separator, so a username
	// containing one silently shifts the split and authenticates as somebody
	// else's name with the wrong password.
	if strings.Contains(a.Username, ":") {
		return Auth{}, fmt.Errorf("--username %q contains a colon, which basic auth uses as the separator", a.Username)
	}
	return a, nil
}

// readSecretFile reads a credential from a file and rejects what cannot be one.
//
// A secret on the command line is visible in the process list to anyone on the
// box, and in shell history, so every credential this tool accepts has a file
// form and the file wins.
func readSecretFile(flag, path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("%s: %w", flag, err)
	}
	// Trailing newlines are near-universal in secret files, and a credential
	// with one attached fails as an opaque 401.
	v := strings.TrimSpace(string(b))
	if v == "" {
		return "", fmt.Errorf("%s %s is empty", flag, path)
	}
	// Interior whitespace means the file holds something other than one
	// credential -- two lines, or a comment. An Authorization header carrying
	// a newline is rejected far from the flag that caused it.
	if strings.ContainsAny(v, " \t\r\n") {
		return "", fmt.Errorf("%s %s contains whitespace inside the value; "+
			"it should hold one credential and nothing else", flag, path)
	}
	return v, nil
}

// listLabels summarizes label names, or dumps one label's values in full.
// Summarizing by default matters: a label like `comm` has thousands of values,
// and printing them all turns a discovery command into a wall of text.
func listLabels(ctx context.Context, c *Client, name string, start, end time.Time, concurrency int) error {
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
	// One Values query per label name, fanned out. Sequentially this was the
	// slowest thing in the tool and the first thing anyone runs: against a
	// loaded server it never finished, printing not even the header. `report`
	// already had --concurrency for exactly this shape of work.
	type row struct {
		name string
		vals []string
		err  error
	}
	rows := make([]row, len(names))
	prog := newProgress("querying", "labels", len(names))
	prog.start()
	sem := make(chan struct{}, max(1, concurrency))
	var wg sync.WaitGroup
	for i, n := range names {
		wg.Add(1)
		go func(i int, n string) {
			defer wg.Done()
			defer prog.step()
			sem <- struct{}{}
			defer func() { <-sem }()
			// The deadline is taken inside LabelValues, after the semaphore,
			// so a goroutine parked waiting for a slot does not burn it.
			vals, err := c.LabelValues(ctx, n, start, end)
			rows[i] = row{name: n, vals: vals, err: err}
		}(i, n)
	}
	wg.Wait()
	prog.stop()

	w := newTab()
	fmt.Fprintf(w, "LABEL\tVALUES\tSAMPLE\n")
	var failedMsgs []string
	for _, r := range rows {
		// One label's failure used to abort the whole summary, losing every
		// label that did work. Name the missing row and keep going.
		if r.err != nil {
			failedMsgs = append(failedMsgs, shortErr(r.err))
			fmt.Fprintf(w, "%s\t?\t!! %s\n", r.name, shortErr(r.err))
			continue
		}
		sort.Strings(r.vals)
		sample, suffix := r.vals, ""
		if len(sample) > 6 {
			sample, suffix = sample[:6], " …"
		}
		fmt.Fprintf(w, "%s\t%d\t%s%s\n", r.name, len(r.vals), strings.Join(sample, " "), suffix)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	// Immediately after the table it annotates, and before the unrelated
	// next-step line -- report puts the explanation right under the numbers
	// too.
	if len(failedMsgs) > 0 {
		// shortErr keeps only the gRPC tail, which for a deadline is the least
		// useful half: the advice metaErr attached sits in front of it.
		if h := hintFor(failedMsgs, metadataQuery); h != "" {
			fmt.Print(h)
		}
	}
	fmt.Println("\nRun `parcareport labels <name>` to list one label's values in full.")
	if len(failedMsgs) > 0 {
		return fmt.Errorf("%d of %d label queries failed; the rows marked !! are missing",
			len(failedMsgs), len(names))
	}
	return nil
}

func report(ctx context.Context, c *Client, o options, start, end time.Time) error {
	// Before any query: a typo here should not cost minutes of merging first.
	sortBy, err := parseSortKey(o.sortBy)
	if err != nil {
		return err
	}

	profType, typeVerified, err := resolveProfileType(ctx, c, o.profileType)
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
		profType, start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339), window.Round(time.Second))

	type result struct {
		name   string
		cores  float64
		header string
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
			results[i] = result{name: g, cores: m.Value, header: m.Header, err: err}
		}(i, g)
	}
	wg.Wait()
	prog.stop()

	rows := make([]Row, 0, len(results))
	var total float64
	empty := 0
	// The groups already know their unit. Keeping one means the fallback below
	// does not have to guess "CORES" for what might be a byte or count profile.
	groupHeader := ""
	var failed []failure
	for _, r := range results {
		if r.err != nil {
			// Collected, not just warned about. A warning on stderr vanishes
			// under `2>/dev/null` and leaves a table that looks complete but
			// whose totals and percentages silently omit whatever failed.
			failed = append(failed, failure{group: fmt.Sprintf("%s=%s", o.by, r.name), msg: shortErr(r.err)})
			continue
		}
		total += r.cores
		if r.header != "" {
			groupHeader = r.header
		}
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
		// Nothing to show, for one of two very different reasons, and calling
		// a broken run an idle cluster is the mistake worth ruling out.
		//
		// But they mix: some queries can fail while every survivor comes back
		// genuinely empty. Saying "all N queries failed" off len(failed) then
		// gets it backwards in both directions -- with --by=comm, one flaky
		// query turned "199 values had no samples, 1 failed" into "all 1 comm
		// queries failed". So state both counts.
		if len(failed) > 0 {
			// On stdout with a banner. The bare `!!` lines this used to print
			// had no introduction, so the worse of the two outcomes was the
			// one explained less well, and the summary went only to stderr --
			// the case the banner exists to avoid.
			if len(failed) == len(groups) {
				fmt.Printf("!! FAILED: all %d %s queries failed, so there is nothing to report.\n"+
					"!! This is not an empty window -- the queries did not come back.\n",
					len(failed), o.by)
			} else {
				fmt.Printf("!! FAILED: %d of %d %s queries failed and the other %d had no samples,\n"+
					"!! so there is nothing to report. Whether this window is idle is unknown:\n"+
					"!! the failed queries were never answered.\n",
					len(failed), len(groups), o.by, empty)
			}
			printFailures(failed, mergeQuery)
			return fmt.Errorf("%d of %d %s queries failed; no results", len(failed), len(groups), o.by)
		}
		if !typeVerified {
			// The warning about this went only to stderr, which is the case
			// the project refuses: under 2>/dev/null a typo'd selector left an
			// empty stdout that reads as an idle cluster.
			fmt.Printf("!! No data, and the profile type was never verified against the\n" +
				"!! server -- that lookup failed. A selector this server does not offer\n" +
				"!! looks exactly like this. Check it with `parcareport types`.\n")
			return fmt.Errorf("no data in this window, and %q was never verified", profType)
		}
		return errors.New("no data in this window")
	}
	// Everything matching the selector, including series that carry no value
	// for the group-by label at all. Those exist -- an agent deployed without
	// the label, say -- and if we only ever summed the labelled groups they
	// would vanish from the report while still burning CPU. Reporting the
	// residual as "(unlabeled)" keeps the table honest and adds up to 100%.
	overallSel := selector(profType, "", "", o.match)
	// Bounded like every per-group merge. This one carries no label matcher at
	// all, so it is the widest query in the run and the likeliest to be slow;
	// on the bare process context it could stall the run for ten minutes,
	// which is what --timeout exists to prevent.
	octx, ocancel := context.WithTimeout(ctx, o.timeout)
	overallRaw, overallErr := c.MergePprof(octx, overallSel, start, end)
	ocancel()
	var overall *profile.Profile
	if overallErr == nil {
		overall, overallErr = parsePprof(overallRaw)
	}

	grand := total
	// Only reached when the unfiltered merge came back empty or failed, which
	// the groups contradict -- but a wrong unit in the heading is worse than a
	// vague one, and the groups already worked theirs out.
	header := groupHeader
	if header == "" {
		header = "CORES"
	}
	// Without that merge there is no residual and no denominator. Percentages
	// against the labelled groups alone would be the one number guaranteed to
	// be wrong when a group is missing, so drop them rather than invent them.
	// An empty response is as unknown as a failed one: parsePprof returns
	// (nil, nil) for no bytes, and the groups having values contradicts it.
	// Printing TOTAL ... 100.0 off the group sum in that state asserted a
	// fleet-wide figure nothing had measured.
	knowTotal := overallErr == nil && overall != nil
	if overall != nil {
		mt, err := interpret(overall, window)
		if err != nil {
			return err
		}
		header = mt.Header
		grand = mt.Value
		if residual := grand - total; residual > grand*0.001 {
			rows = append(rows, Row{Name: "(unlabeled)", Cores: residual})
		}
	}
	if knowTotal && grand > 0 {
		for i := range rows {
			rows[i].Pct = rows[i].Cores / grand * 100
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Cores > rows[j].Cores })
	printGroupTable(strings.ToUpper(o.by), header, rows, grand, knowTotal)
	if empty > 0 {
		fmt.Printf("(%d %s values had no samples in this window, omitted)\n", empty, o.by)
	}
	// One banner, however many things went wrong. Two headers, each describing
	// the run as if it were the only problem, read as two unrelated reports --
	// and the group-failure wording talked about "totals and percentages"
	// that the other banner had just explained away.
	if len(failed) > 0 || overallErr != nil {
		// On stdout, next to the numbers it invalidates -- never only stderr.
		fmt.Print("\n!! INCOMPLETE\n")
		all := failed
		if len(failed) > 0 {
			if knowTotal {
				fmt.Printf("!! %d of %d %s queries failed. The totals and percentages above\n"+
					"!! EXCLUDE them and are therefore wrong.\n", len(failed), len(groups), o.by)
			} else {
				fmt.Printf("!! %d of %d %s queries failed. The sum above excludes them.\n",
					len(failed), len(groups), o.by)
			}
		}
		if overallErr != nil {
			// The group breakdown is the main output of the command and is
			// already computed by this point, so discarding it because an
			// auxiliary query failed was the wrong trade -- but so would be
			// printing it as though it were complete.
			fmt.Printf("!! The unfiltered merge failed, so percentages, the (unlabeled) row\n"+
				"!! and the hot-function table are missing, and any series carrying no\n"+
				"!! %q label is absent from the sum above.\n", o.by)
			all = append(append([]failure{}, failed...),
				failure{group: "(overall)", msg: shortErr(overallErr)})
		}
		printFailures(all, mergeQuery)
	}

	if o.top > 0 && overall != nil {
		fmt.Println()
		fns, err := topFunctions(overall, window, sortBy)
		if err != nil {
			return err
		}
		// Percentages are against the same profile the functions came from,
		// and against the sorted column: self time sums to the total across
		// all functions, so FLAT percentages are small and spread out, while
		// CUM for a root frame approaches 100% rather than exceeding it.
		printFunctionTable(fns, header, o.top, grand, sortBy)
	}
	switch {
	case len(failed) > 0 && overallErr != nil:
		return fmt.Errorf("%d of %d %s queries failed and so did the unfiltered merge; "+
			"results above are incomplete", len(failed), len(groups), o.by)
	case len(failed) > 0:
		return fmt.Errorf("%d of %d %s queries failed; results above are incomplete",
			len(failed), len(groups), o.by)
	case overallErr != nil:
		return fmt.Errorf("the unfiltered merge failed, so the total, the percentages and "+
			"the function table are missing: %s", shortErr(overallErr))
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
	window := fmt.Sprintf("%s .. %s", start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339))

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

// resolveProfileType returns the selector to use and whether it was actually
// checked against the server. An unverified type is the likeliest explanation
// for an empty report, so the caller has to be able to say so.
func resolveProfileType(ctx context.Context, c *Client, want string) (string, bool, error) {
	// An explicit selector needs nothing from the server. The lookup is only
	// here to catch a typo, which otherwise comes back as an empty merge that
	// reads like an idle window -- so it is worth attempting, but it must not
	// be able to fail a run on its own. It used to: against a loaded server
	// this deadline killed fully-specified runs that would have worked.
	if want != "" {
		names, err := c.ProfileTypeNames(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parcareport: could not check --profile-type against the server "+
				"(%s); using %q as given\n", shortErr(err), want)
			return want, false, nil
		}
		for _, n := range names {
			if n == want {
				return n, true, nil
			}
		}
		return "", false, fmt.Errorf("profile type %q not offered by this server; available:\n  %s", want, strings.Join(names, "\n  "))
	}

	names, err := c.ProfileTypeNames(ctx)
	if err != nil {
		return "", false, err
	}
	switch len(names) {
	case 0:
		return "", false, errors.New("server offers no profile types -- is any agent writing to it?")
	case 1:
		return names[0], true, nil
	default:
		return "", false, fmt.Errorf("server offers %d profile types; pick one with --profile-type:\n  %s",
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
		return time.Time{}, time.Time{}, fmt.Errorf("--from (%s) must be before --to (%s)",
			start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339))
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

// failureKind distinguishes the two sorts of query the tool makes, because the
// advice for them differs. A merge can be made smaller -- a narrower window,
// fewer series -- while a metadata query already asks for almost nothing, so
// there is nothing to trim and a failure says something about the server.
// Telling someone to narrow --from or use --match after a failed label lookup
// suggests an action that does not apply: listLabels passes no matchers at all.
type failureKind int

const (
	mergeQuery failureKind = iota
	metadataQuery
)

// failure is one group's query that did not come back.
type failure struct {
	group string // e.g. `cluster=tc`
	msg   string
}

// printFailures lists what broke, grouped by cause.
//
// Capping a flat list could hide a whole cause: eight instances failing the
// same way showed five lines and "and 3 more", while a single group failing
// for a different reason might be the one truncated away. Grouping by message
// gives every distinct cause a line, and collapses a wholesale outage to one.
func printFailures(failed []failure, kind failureKind) {
	var order []string
	byMsg := map[string][]string{}
	for _, f := range failed {
		if _, seen := byMsg[f.msg]; !seen {
			order = append(order, f.msg)
		}
		byMsg[f.msg] = append(byMsg[f.msg], f.group)
	}

	const showCauses, showGroups = 5, 3
	shown := order
	for i, msg := range order {
		if i == showCauses {
			// 9b: "1 more distinct errors" read wrong.
			more := len(order) - showCauses
			noun := "errors"
			if more == 1 {
				noun = "error"
			}
			fmt.Printf("!!   ... and %d more distinct %s\n", more, noun)
			shown = order[:showCauses]
			break
		}
		groups := byMsg[msg]
		if len(groups) == 1 {
			fmt.Printf("!!   %s: %s\n", groups[0], msg)
			continue
		}
		list, suffix := groups, ""
		if len(list) > showGroups {
			list, suffix = list[:showGroups], fmt.Sprintf(", and %d more", len(groups)-showGroups)
		}
		fmt.Printf("!!   %s  (%d groups: %s%s)\n", msg, len(groups), strings.Join(list, ", "), suffix)
	}
	// Only the causes actually on screen, so a hint never refers to a line
	// that was truncated away.
	if h := hintFor(shown, kind); h != "" {
		fmt.Print(h)
	}
}

// hintFor suggests a next step for failures whose own text does not imply one.
//
// "stream terminated by RST_STREAM with error code: INTERNAL_ERROR" is the
// motivating case: there is no gRPC boilerplate for shortErr to strip and
// nothing in it says what to do, yet it can take minutes to arrive.
func hintFor(msgs []string, kind failureKind) string {
	var reset, deadline bool
	for _, m := range msgs {
		switch {
		case strings.Contains(m, "RST_STREAM"), strings.Contains(m, "INTERNAL_ERROR"):
			reset = true
		case strings.Contains(m, "deadline exceeded"), strings.Contains(m, "timed out"):
			deadline = true
		}
	}
	var b strings.Builder
	if reset {
		if kind == mergeQuery {
			b.WriteString("!! The server closed the stream mid-merge, which usually means the merge hit a\n" +
				"!! server limit or the server errored on it. Try a narrower --from window, or\n" +
				"!! fewer series with --match.\n")
		} else {
			b.WriteString("!! The server closed the stream on a query that asks for almost nothing, so\n" +
				"!! this points at the server rather than at what was asked of it. Retry.\n")
		}
	}
	if deadline {
		if kind == mergeQuery {
			b.WriteString("!! Raise --timeout, or narrow the window with --from so each merge is smaller.\n")
		} else {
			// metaErr already says this, but shortErr keeps only the gRPC tail.
			b.WriteString("!! These queries are normally instant, so a timeout means the server is slow\n" +
				"!! or unreachable rather than the window being too large. Retry, or raise\n" +
				"!! --timeout.\n")
		}
	}
	return b.String()
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

const usage = `parcareport - cross-cluster CPU bottleneck report from a Parca server

Usage:
  parcareport [report] [flags]   break CPU down by a label, then list hot functions
  parcareport labels [name]      summarize labels, or list one label's values
  parcareport types [flags]      list profile types the server offers

CORES is average cores busy over the window: CPU-seconds / wall-seconds. It is
comparable across clusters of different sizes, unlike raw sample counts.

Functions are listed by self time (FLAT) by default -- the code that was
actually on-CPU. --sort=cum orders by cumulative time instead, which shows
what work a frame was part of, but puts runtime plumbing on top.

Flags:
`
