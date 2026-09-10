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
	addr        string
	insecure    bool
	from        string
	to          string
	by          string
	profileType string
	match       string
	top         int
	sortBy      string
	output      string
	maxGroups   int
	// setFlags records which flags were given, so a subcommand can refuse one
	// it would otherwise ignore.
	setFlags    map[string]bool
	concurrency int
	timeout     time.Duration
	deadline    time.Duration
	// resolvedType lets a caller that already knows the profile type skip the
	// lookup. Not a flag: only overview sets it.
	resolvedType string
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
	fs.StringVar(&o.profileType, "profile-type", "", "profile type: full selector or a unique substring like 'cpu' (default: the CPU profile)")
	fs.StringVar(&o.match, "match", "", `extra label matchers, e.g. 'cluster="tc",comm="clickhouse"'`)
	fs.IntVar(&o.top, "top", 15, "how many functions to list (0 disables the function table)")
	fs.StringVar(&o.sortBy, "sort", defaultSortBy, "order functions by 'flat' (self time) or 'cum' (cumulative)")
	fs.StringVar(&o.output, "output", outputTable, "'table' for a person, 'json' for a script")
	fs.IntVar(&o.maxGroups, "max-group-values", 50, "overview: skip a breakdown whose label has more values than this")
	fs.IntVar(&o.concurrency, "concurrency", 4, "parallel queries: group merges, and the labels fan-out")
	fs.DurationVar(&o.timeout, "timeout", 60*time.Second, "per-query timeout; a slow group fails visibly instead of stalling the run")
	fs.DurationVar(&o.deadline, "deadline", 10*time.Minute, "budget for the whole run; 0 removes it and relies on --timeout alone")
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
	// Which flags were actually given. A default is indistinguishable from an
	// explicit value otherwise, and overview needs to tell them apart to
	// refuse the ones it would overwrite.
	o.setFlags = map[string]bool{}
	fs.Visit(func(f *flag.Flag) { o.setFlags[f.Name] = true })

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

	// A per-query --timeout at or above the whole run's budget can never fire:
	// the run's own clock reaches every outstanding query first, and they all
	// report their own deadline at once. That reads as N slow queries when it
	// was one clock, and the hint it prints ("raise --timeout") cannot work
	// because --timeout is already past the ceiling. Refuse the combination
	// rather than let it mislead.
	if o.deadline > 0 && o.timeout >= o.deadline {
		return fmt.Errorf("--timeout (%s) must be below --deadline (%s), or it can never fire: "+
			"the run's budget would expire first and every outstanding query would report its own "+
			"deadline at the same moment. Lower --timeout, or raise --deadline", o.timeout, o.deadline)
	}

	ctx := context.Background()
	cancel := func() {}
	if o.deadline > 0 {
		ctx, cancel = context.WithTimeout(ctx, o.deadline)
	}
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
	case "overview":
		if subArg != "" {
			return fmt.Errorf("overview takes no argument, got %q", subArg)
		}
		return overview(ctx, c, o, start, end)
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
		return fmt.Errorf("unknown command %q (want: report, overview, labels, types)", sub)
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
	out, err := parseOutput(o.output)
	if err != nil {
		return err
	}
	d, gerr := gatherReport(ctx, c, o, start, end)
	if out == outputJSON {
		// A successful run renders as it stands; anything else still emits a
		// document, so a consumer parses one shape whatever happened.
		if d != nil && gerr == nil {
			if jerr := renderJSON(d); jerr != nil {
				return jerr
			}
			return nil
		}
		renderJSONError(o, d, start, end, gerr)
		return gerr
	}
	if d != nil {
		renderTable(d)
	}
	return gerr
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
			// A full selector needs nothing from the server, so it can be
			// taken on trust. An abbreviation cannot: expanding it IS the
			// lookup. Passing it through unexpanded produced a run that could
			// only fail -- against a real server, `--profile-type=cpu` with a
			// failed lookup sent "cpu" as the selector and every merge came
			// back "profile-type selection must be of the form ...", after
			// minutes of waiting.
			if !looksLikeSelector(want) {
				return "", false, fmt.Errorf("cannot expand --profile-type %q: that is an abbreviation, "+
					"and the profile type list it would be matched against could not be read (%s). "+
					"Retry, or pass the full selector", want, shortErr(err))
			}
			fmt.Fprintf(os.Stderr, "parcareport: could not check --profile-type against the server "+
				"(%s); using %q as given\n", shortErr(err), want)
			return want, false, nil
		}
		got, err := matchProfileType(want, names)
		if err != nil {
			return "", false, err
		}
		return got, true, nil
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
	}
	// Several types, and no answer given. Refusing was defensible when the
	// alternative was guessing, but a real server offers many -- eleven on the
	// one this was tested against -- so auto-detect never applied and every
	// invocation had to paste the full selector. This tool is a CPU report and
	// CORES is its headline unit, so a CPU delta profile is the right default;
	// the chosen type is echoed in the report heading, so it is visible rather
	// than hidden.
	cpu := cpuDeltaTypes(names)
	switch {
	case len(cpu) == 1:
		return cpu[0], true, nil
	case len(cpu) > 1:
		// Two agents writing CPU profiles under different names. Picking one
		// would silently report a fraction of the fleet as though it were all
		// of it, so list exactly the candidates rather than every type.
		return "", false, fmt.Errorf("server offers %d CPU profiles and nothing to choose between them; "+
			"pick one with --profile-type:\n  %s", len(cpu), strings.Join(cpu, "\n  "))
	}
	return "", false, fmt.Errorf("server offers %d profile types and none of them is a CPU profile "+
		"to default to; pick one with --profile-type:\n  %s", len(names), strings.Join(names, "\n  "))
}

// looksLikeSelector reports whether a value is a complete profile type
// selector rather than an abbreviation of one.
//
// Parca wants name:sampleType:sampleUnit:periodType:periodUnit, optionally
// followed by :delta, and rejects anything else outright. That is exactly the
// line between a value that can be used without asking the server and one
// that cannot.
func looksLikeSelector(s string) bool {
	parts := strings.Split(s, ":")
	if len(parts) != 5 && len(parts) != 6 {
		return false
	}
	for _, p := range parts {
		if p == "" {
			return false
		}
	}
	return len(parts) == 5 || parts[5] == "delta"
}

// matchProfileType resolves --profile-type against what the server offers.
//
// An exact selector always wins. Failing that it is treated as a substring,
// because the full six-part form is long, easy to get subtly wrong -- the
// order of `samples:count:cpu:nanoseconds` matters -- and had to be retyped for
// every single invocation. `--profile-type=cpu` is what people mean.
//
// An ambiguous abbreviation is an error listing the candidates, never a guess:
// `memory` matches four types on a typical server, and silently picking
// inuse_space over alloc_space would be answering a different question than
// the one asked.
func matchProfileType(want string, names []string) (string, error) {
	for _, n := range names {
		if n == want {
			return n, nil
		}
	}
	var hits []string
	for _, n := range names {
		if strings.Contains(n, want) {
			hits = append(hits, n)
		}
	}
	switch len(hits) {
	case 1:
		return hits[0], nil
	case 0:
		return "", fmt.Errorf("no profile type matching %q on this server; available:\n  %s",
			want, strings.Join(names, "\n  "))
	default:
		return "", fmt.Errorf("%q matches %d profile types; be more specific:\n  %s",
			want, len(hits), strings.Join(hits, "\n  "))
	}
}

// cpuDeltaTypes finds the on-CPU delta profiles among the server's types.
//
// The period type carries the meaning: parca-agent's CPU profile is
// `parca_agent:samples:count:cpu:nanoseconds:delta`, where `cpu` is the period
// type. Matching on the substring "cpu" alone would also catch a wallclock
// profile from a target that happens to have "cpu" in its name, and off-CPU
// time is emphatically not what CORES means.
func cpuDeltaTypes(names []string) []string {
	var out []string
	for _, n := range names {
		parts := strings.Split(n, ":")
		// name:sampleType:sampleUnit:periodType:periodUnit[:delta]
		if len(parts) == 6 && parts[3] == "cpu" && parts[5] == "delta" {
			out = append(out, n)
		}
	}
	return out
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
	fmt.Print(formatFailures(failed, kind))
}

// formatFailures builds what printFailures prints, so a caller that must not
// write to stdout yet can hold on to it.
func formatFailures(failed []failure, kind failureKind) string {
	var b strings.Builder
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
			more := len(order) - showCauses
			// "1 more distinct errors" reads wrong.
			noun := "errors"
			if more == 1 {
				noun = "error"
			}
			fmt.Fprintf(&b, "!!   ... and %d more distinct %s\n", more, noun)
			shown = order[:showCauses]
			break
		}
		groups := byMsg[msg]
		if len(groups) == 1 {
			fmt.Fprintf(&b, "!!   %s: %s\n", groups[0], msg)
			continue
		}
		list, suffix := groups, ""
		if len(list) > showGroups {
			list, suffix = list[:showGroups], fmt.Sprintf(", and %d more", len(groups)-showGroups)
		}
		fmt.Fprintf(&b, "!!   %s  (%d groups: %s%s)\n", msg, len(groups), strings.Join(list, ", "), suffix)
	}
	// Only the causes actually on screen, so a hint never refers to a line
	// that was truncated away.
	if h := hintFor(shown, kind); h != "" {
		b.WriteString(h)
	}
	return b.String()
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
  parcareport overview [flags]   what this server has, and what is busy in it
  parcareport labels [name]      summarize labels, or list one label's values
  parcareport types [flags]      list profile types the server offers

CORES is average cores busy over the window: CPU-seconds / wall-seconds. It is
comparable across clusters of different sizes, unlike raw sample counts.

Functions are listed by self time (FLAT) by default -- the code that was
actually on-CPU. --sort=cum orders by cumulative time instead, which shows
what work a frame was part of, but puts runtime plumbing on top.

Flags:
`
