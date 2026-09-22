package main

import (
	"fmt"
	"strings"
)

// A report answers "which of these is biggest". The next question is always
// "and what inside it", and until now the output said nothing about how to ask
// it -- every `parcareport ...` string in the tool sat on an error or a skip
// path, so a reader whose query FAILED got three suggestions and a reader who
// found a bottleneck got none.
//
// The drill-down is the command that narrows the report to one row. That is
// worth more than it first looks, because the function table is built from the
// unfiltered merge: it is fleet-wide, and it is NOT the breakdown of the row
// above it. Narrowing with --match is the only way to get the functions for one
// cluster, one workload, one process -- which is the question a person actually
// has once they know where the time went.

// drillJSON is how a consumer gets at the same thing without parsing a command
// line. The parts are given separately so an agent can compose its own call,
// and Command is the ready-to-run form for a person.
type drillJSON struct {
	Command string `json:"command"`
	By      string `json:"by"`
	Match   string `json:"match"`
}

// drillFor builds the command that narrows this report to one group value.
//
// It returns nil for the residual "(unlabeled)" row. That row is every series
// carrying no value for the label at all, and there is no matcher that selects
// it: `cluster=""` is a claim about the value, not about the label's absence,
// and offering it would hand the reader a command that quietly reports
// something else. What that row means is explained in the notes instead.
func drillFor(o options, groupName string, unlabeled bool) *drillJSON {
	if unlabeled || groupName == "" {
		return nil
	}

	by := nextBreakdown(o.by, o.knownLabels)

	// Already pinned to this value AND grouping the same way: the command
	// would hand back the very report being read, while reading like there is
	// somewhere further to go. Pinned but regrouping is a real next step --
	// `--by namespace --match 'cluster="tc"'` is not the cluster report.
	//
	// The comparison only catches the spelling this tool produces. A matcher
	// written `cluster='tc'` or `cluster=~"tc"` slips through and the command
	// carries a duplicate, which PromQL accepts and simply ANDs -- ugly, not
	// wrong.
	if by == o.by {
		want := fmt.Sprintf("%s=%q", o.by, groupName)
		for _, m := range matchers(o.match) {
			if strings.TrimSpace(m) == want {
				return nil
			}
		}
	}

	match := addMatcher(o.match, o.by, groupName)

	args := baseCommandArgs(o, "")
	args = append(args,
		"--by "+shellQuote(by),
		"--match "+shellQuote(match),
	)
	return &drillJSON{Command: strings.Join(args, " "), By: by, Match: match}
}

// baseCommandArgs is the leading part of any command this tool suggests:
// everything that decides WHAT is queried and HOW the server is reached.
//
// Shared so a suggestion cannot drift from the run that produced it. A note
// that named a different profile type, or omitted the TLS flags, would send
// the reader somewhere else while claiming to be a closer look at this.
//
// profileTypeOverride replaces the run's own type, for a suggestion that
// deliberately changes it -- "you are burning CPU on the allocator, go look at
// alloc_space".
func baseCommandArgs(o options, profileTypeOverride string) []string {
	args := []string{"parcareport"}
	// Not carried: presentation and budgets (--top, --timeout, --deadline,
	// --concurrency, --output). Those change how the answer is shown, not what
	// it is, and the defaults are as good a starting point as the last run's.
	//
	// A relative --from is carried verbatim, so `--from -6h` in a suggestion
	// means six hours before you run it, not before the run that printed it.
	// Pass absolute times if you need the identical window; the README says so.
	if o.setFlags["url"] {
		args = append(args, "--url "+shellQuote(o.addr))
	}
	if !o.insecure {
		// Go's flag package needs the = form for a bool; "--insecure false"
		// parses as a flag plus a positional and is rejected.
		args = append(args, "--insecure=false")
	}
	// Credentials by reference only. --bearer-token and --password take the
	// secret on the command line, and echoing one into a terminal -- and into
	// the JSON a consumer may log -- would leak it. The file and username
	// forms name where to look, which is safe to print.
	if o.setFlags["bearer-token-file"] {
		args = append(args, "--bearer-token-file "+shellQuote(o.tokenFile))
	}
	if o.setFlags["username"] {
		args = append(args, "--username "+shellQuote(o.username))
	}
	if o.setFlags["password-file"] {
		args = append(args, "--password-file "+shellQuote(o.passwordFile))
	}
	pt := profileTypeOverride
	if pt == "" {
		// resolvedType, not just the flag: overview REFUSES --profile-type and
		// picks one per section, so setFlags never records it there. Gating on
		// the flag alone made overview's heap section offer a command that
		// re-ran against the default CPU profile -- a different profile,
		// presented as a closer look at this one.
		pt = drillProfileType(o)
	}
	if pt != "" {
		args = append(args, "--profile-type "+shellQuote(pt))
	}
	if o.setFlags["from"] {
		args = append(args, "--from "+shellQuote(o.from))
	}
	if o.setFlags["to"] {
		args = append(args, "--to "+shellQuote(o.to))
	}
	if o.fanOut {
		// Carried because it changes what is measured, not how it is shown: a
		// suggestion offered *because* the fan-out is expensive must re-run
		// the fan-out, or it answers a different question than the one that
		// was refused.
		args = append(args, "--fan-out")
	}
	if o.setFlags["sort"] {
		// Dropping --sort=cum flips the function table back to flat, and the
		// function table is what these commands are for.
		args = append(args, "--sort "+shellQuote(o.sortBy))
	}
	return args
}

// drillProfileType returns the profile type the command must name, from
// whichever source the caller had. Empty when the run took the default and the
// default is still right.
func drillProfileType(o options) string {
	if o.resolvedType != "" {
		return o.resolvedType
	}
	if o.setFlags["profile-type"] {
		return o.profileType
	}
	return ""
}

// nextBreakdown picks the label to group by once the report is narrowed to one
// value of the current one: inside a cluster you want namespaces, inside a
// namespace workloads, and so on down overviewBreakdowns.
//
// known is the server's label list when the caller already has it -- overview
// reads one anyway -- and empty when it does not. A plain report does not fetch
// one: a hint is not worth an extra round trip on every run, and a suggestion
// naming a label the server does not carry sends the reader to an error, which
// is the experience this whole feature exists to replace.
//
// With no list, or with nothing finer available, the label stays as it is. That
// still narrows -- the row's own numbers and, crucially, its function table --
// it just does not subdivide.
//
// Caveat worth knowing: overview's list is the union of label names across all
// profile types, so a label carried only by some other type can still be
// suggested. The result is an empty report rather than an error.
func nextBreakdown(by string, known []string) string {
	at := -1
	for i, b := range overviewBreakdowns {
		if b == by {
			at = i
			break
		}
	}
	if at < 0 || at == len(overviewBreakdowns)-1 {
		return by
	}
	if known == nil {
		// Nothing to check against, so do not guess.
		return by
	}
	for _, cand := range overviewBreakdowns[at+1:] {
		for _, k := range known {
			if k == cand {
				return cand
			}
		}
	}
	return by
}

// addMatcher appends one label=value to an existing --match, which may be
// empty. Values are quoted the same way selector() quotes them, so what the
// hint prints is what the query would use.
func addMatcher(match, label, value string) string {
	m := fmt.Sprintf("%s=%q", label, value)
	if match == "" {
		return m
	}
	return match + "," + m
}

// shellSafe is the set of characters that never need quoting in any POSIX
// shell -- the same set python's shlex.quote treats as bare.
//
// An allowlist rather than a list of dangerous characters: a denylist has to
// be complete to be correct, and the one this replaced was not. It let
// `--url [::1]:7070` (the ordinary spelling of an IPv6 target) and
// `--profile-type 'cpu*'` through unquoted, which zsh refuses to run at all --
// "no matches found" -- before parcareport is even invoked.
const shellSafe = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789@%+=:,./-_"

// shellQuote wraps a value so it survives being pasted into a shell. The
// matchers contain double quotes, so single quotes are the outer form, and a
// value containing a single quote is spliced the only way POSIX allows.
func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	if strings.IndexFunc(s, func(r rune) bool { return !strings.ContainsRune(shellSafe, r) }) < 0 {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}
