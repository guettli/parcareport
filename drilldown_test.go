package main

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

func drillOpts(by, match string, known []string, set ...string) options {
	o := options{
		by: by, match: match, knownLabels: known,
		addr: "parca.example:7070", from: "-6h", to: "now",
		insecure: true, sortBy: "cum",
		profileType: "memory:inuse_space:bytes:space:bytes",
		setFlags:    map[string]bool{},
	}
	for _, f := range set {
		o.setFlags[f] = true
	}
	return o
}

// The command a hint prints has to be one that runs. These assert the exact
// string, because a hint that is almost right is worse than none: it sends the
// reader somewhere and the mistake only shows up in the numbers.
func TestDrillForBuildsARunnableCommand(t *testing.T) {
	tests := []struct {
		name  string
		o     options
		group string
		want  string
	}{
		{
			// Flags the reader did not give are not invented: a hint that
			// pinned --url or --from would claim the run had said things it
			// did not, and the defaults are the defaults either way.
			name:  "plain report keeps --by: no label list, so no guessing",
			o:     drillOpts("cluster", "", nil),
			group: "tc",
			want:  `parcareport --by cluster --match 'cluster="tc"'`,
		},
		{
			name:  "with a label list it steps down the hierarchy",
			o:     drillOpts("cluster", "", []string{"cluster", "namespace", "workload", "comm"}),
			group: "tc",
			want:  `parcareport --by namespace --match 'cluster="tc"'`,
		},
		{
			name:  "a missing rung is skipped, not suggested",
			o:     drillOpts("cluster", "", []string{"cluster", "workload"}),
			group: "tc",
			want:  `parcareport --by workload --match 'cluster="tc"'`,
		},
		{
			name:  "an existing --match is carried, not replaced",
			o:     drillOpts("workload", `cluster="tc"`, nil),
			group: "agentloop",
			want:  `parcareport --by workload --match 'cluster="tc",workload="agentloop"'`,
		},
		{
			name:  "flags that decide what is asked are carried",
			o:     drillOpts("cluster", "", nil, "url", "from", "profile-type", "sort"),
			group: "tc",
			want: `parcareport --url parca.example:7070 ` +
				`--profile-type memory:inuse_space:bytes:space:bytes ` +
				`--from -6h --sort cum --by cluster --match 'cluster="tc"'`,
		},
		{
			// overview REFUSES --profile-type and picks one per section, so
			// setFlags never records it. Gating on the flag alone made the
			// heap section offer a command that re-ran against the default
			// CPU profile -- a different profile, presented as a closer look
			// at this one.
			name: "overview's resolved type is carried even though no flag was set",
			o: func() options {
				o := drillOpts("instance", "", []string{"instance"})
				o.resolvedType = "memory:inuse_space:bytes:space:bytes"
				return o
			}(),
			group: "node-1:7070",
			want: `parcareport --profile-type memory:inuse_space:bytes:space:bytes ` +
				`--by instance --match 'instance="node-1:7070"'`,
		},
		{
			// A TLS deployment refuses credentials over plaintext, so a
			// command that drops --insecure=false cannot connect at all.
			// Secrets given inline are NOT echoed; the file forms are.
			name: "TLS and credential files are carried, inline secrets are not",
			o: func() options {
				o := drillOpts("cluster", "", nil, "bearer-token-file", "bearer-token")
				o.insecure = false
				o.tokenFile = "/etc/parca/token"
				o.bearerToken = "s3cr3t"
				return o
			}(),
			group: "tc",
			want: `parcareport --insecure=false --bearer-token-file /etc/parca/token ` +
				`--by cluster --match 'cluster="tc"'`,
		},
		{
			// Denylist quoting let these through bare, and zsh refuses to run
			// the line at all -- "no matches found" -- before parcareport is
			// ever invoked.
			name: "shell metacharacters in carried values are quoted",
			o: func() options {
				o := drillOpts("cluster", "", nil, "url", "profile-type")
				o.addr = "[::1]:7070"
				o.profileType = "cpu*"
				return o
			}(),
			group: "tc",
			want:  `parcareport --url '[::1]:7070' --profile-type 'cpu*' --by cluster --match 'cluster="tc"'`,
		},
		{
			// The only path through the '"'"' splice.
			name:  "a value containing a single quote survives the shell",
			o:     drillOpts("comm", "", nil),
			group: "it's",
			want:  `parcareport --by comm --match 'comm="it'"'"'s"'`,
		},
		{
			name:  "a value with a quote stays valid in both shell and PromQL",
			o:     drillOpts("comm", "", nil),
			group: `od"d`,
			want:  `parcareport --by comm --match 'comm="od\"d"'`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := drillFor(tc.o, tc.group, false)
			if got == nil {
				t.Fatalf("no drill-down produced")
			}
			if got.Command != tc.want {
				t.Errorf("command\n got %s\nwant %s", got.Command, tc.want)
			}
		})
	}
}

// Two rows have no drill-down, for different reasons, and both matter.
func TestDrillForDeclinesWhenThereIsNowhereToGo(t *testing.T) {
	// The residual row is every series carrying no value for the label at all.
	// No matcher selects that: `cluster=""` is a claim about the value. A
	// command offered here would silently report something else.
	if d := drillFor(drillOpts("cluster", "", nil), "(unlabeled)", true); d != nil {
		t.Errorf("the unlabeled row must not get a matcher, got %q", d.Command)
	}

	// Already pinned to this value: appending the same matcher again would
	// hand back the report being read, while looking like a next step.
	if d := drillFor(drillOpts("node", `node="tc"`, nil), "tc", false); d != nil {
		t.Errorf("an already-pinned row must not be offered again, got %q", d.Command)
	}

	// Pinned alongside another matcher is still pinned.
	if d := drillFor(drillOpts("node", `cluster="tc",node="tc"`, nil), "tc", false); d != nil {
		t.Errorf("pinned among other matchers must still decline, got %q", d.Command)
	}

	// But a different value of the same label is a real narrowing.
	if d := drillFor(drillOpts("node", `node="p16"`, nil), "tc", false); d == nil {
		t.Error("a different value of the pinned label should still drill down")
	}

	// Pinned but regrouped is a genuine next step: --by namespace under
	// cluster="tc" is not the cluster report. Declining it would silence the
	// hint on `overview --match=...`, which refuses --by and so always has a
	// pinned row.
	d := drillFor(drillOpts("cluster", `cluster="tc"`, []string{"cluster", "namespace"}), "tc", false)
	if d == nil {
		t.Fatal("pinned but regrouped should still drill down")
	}
	if d.By != "namespace" {
		t.Errorf("regrouped drill-down should step down, got --by %s", d.By)
	}
}

// nextBreakdown must never name a label the server has not got: the reader
// would land on an error, which is exactly the experience this feature exists
// to replace.
func TestNextBreakdownOnlyNamesLabelsThatExist(t *testing.T) {
	tests := []struct {
		by    string
		known []string
		want  string
	}{
		{"cluster", nil, "cluster"},                      // unknown: keep
		{"cluster", []string{"cluster"}, "cluster"},      // nothing finer
		{"cluster", []string{"cluster", "comm"}, "comm"}, // skip the gaps
		{"namespace", []string{"namespace", "workload"}, "workload"},
		{"comm", []string{"cluster", "comm"}, "comm"}, // last rung
		{"node", []string{"node", "comm"}, "node"},    // not in the hierarchy
		{"cluster", []string{}, "cluster"},            // empty list is not nil, still nothing to pick
	}
	for _, tc := range tests {
		if got := nextBreakdown(tc.by, tc.known); got != tc.want {
			t.Errorf("nextBreakdown(%q, %v) = %q, want %q", tc.by, tc.known, got, tc.want)
		}
	}
}

// The printed block is what a person acts on, so it must say what the function
// table actually covers rather than letting the reader assume it belongs to the
// row above it.
func TestPrintDrillDownsNamesTheScope(t *testing.T) {
	groups := []groupJSON{
		{Name: "tc", DrillDown: &drillJSON{Command: "parcareport --by namespace --match 'cluster=\"tc\"'"}},
		{Name: "(unlabeled)", Unlabeled: true},
		{Name: "vps", DrillDown: &drillJSON{Command: "cmd-vps"}},
	}

	out := captureStdout(t, func() { printDrillDowns(groups, false, true) })
	if !strings.Contains(out, "fleet-wide") {
		t.Errorf("an unnarrowed report must say the functions are fleet-wide:\n%s", out)
	}
	if strings.Contains(out, "(unlabeled)") {
		t.Errorf("the unlabeled row has no command and must not be listed:\n%s", out)
	}
	// The row's own name in the left column, not merely somewhere inside its
	// command -- "tc" is a substring of the command too.
	if !strings.Contains(out, "  tc ") {
		t.Errorf("the row name should head its command:\n%s", out)
	}
	if !strings.Contains(out, "cmd-vps") {
		t.Errorf("every row with a command should be listed:\n%s", out)
	}

	narrowed := captureStdout(t, func() { printDrillDowns(groups, true, true) })
	if strings.Contains(narrowed, "fleet-wide") {
		t.Errorf("under a --match the functions are not fleet-wide:\n%s", narrowed)
	}
	// Positively, not just by absence: blanking the scope word entirely would
	// satisfy the check above while printing "are  -- they describe".
	if !strings.Contains(narrowed, "--match selected") {
		t.Errorf("a narrowed report must say what the functions do cover:\n%s", narrowed)
	}

	// With no function table there is nothing for the scope sentence to be
	// about, so it must not be printed as though there were.
	noFns := captureStdout(t, func() { printDrillDowns(groups, false, false) })
	if strings.Contains(noFns, "functions above") {
		t.Errorf("--top=0 printed no functions, so nothing is 'above':\n%s", noFns)
	}
	if !strings.Contains(noFns, "narrow this report") {
		t.Errorf("the commands should still be offered:\n%s", noFns)
	}

	// Nothing to offer means nothing printed -- not an empty heading.
	empty := captureStdout(t, func() {
		printDrillDowns([]groupJSON{{Name: "(unlabeled)", Unlabeled: true}}, false, true)
	})
	if strings.TrimSpace(empty) != "" {
		t.Errorf("no drill-downs should print nothing, got:\n%s", empty)
	}
}

// Only so many commands are printed, however many rows there are: a wall of
// near-identical lines is not more useful than the few a reader acts on.
func TestPrintDrillDownsIsCapped(t *testing.T) {
	var groups []groupJSON
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		groups = append(groups, groupJSON{Name: n, DrillDown: &drillJSON{Command: "cmd-" + n}})
	}
	out := captureStdout(t, func() { printDrillDowns(groups, false, true) })
	if strings.Contains(out, "cmd-d") || strings.Contains(out, "cmd-e") {
		t.Errorf("only the top %d rows should be shown:\n%s", drillDownsShown, out)
	}
	if !strings.Contains(out, "cmd-c") {
		t.Errorf("the top %d rows should all be shown:\n%s", drillDownsShown, out)
	}
}

// The wiring, not the pieces.
//
// Every test above calls drillFor or printDrillDowns directly, so all of them
// stayed green with the two call sites deleted -- the report would have shipped
// with no hints at all and a full suite. These assert the two places the
// feature actually reaches a reader.
func TestDrillDownsReachTheOutput(t *testing.T) {
	total := 3.0
	d := &reportData{
		ProfileType: "parca_agent:samples:count:cpu:nanoseconds:delta",
		GroupBy:     "cluster",
		Unit:        "cores",
		Total:       &total,
		knowTotal:   true,
		header:      "CORES",
		window:      time.Hour,
		Groups: []groupJSON{
			{Name: "tc", Value: 2, DrillDown: &drillJSON{
				Command: "parcareport --by namespace --match 'cluster=\"tc\"'",
				By:      "namespace", Match: `cluster="tc"`}},
			{Name: "(unlabeled)", Value: 1, Unlabeled: true},
		},
		Functions: []funcJSON{{Name: "runtime.memmove", Cum: 1, Flat: 1}},
	}

	table := captureStdout(t, func() { renderTable(d) })
	if !strings.Contains(table, `--match 'cluster="tc"'`) {
		t.Errorf("renderTable printed no drill-down command:\n%s", table)
	}
	if !strings.Contains(table, "describe every row together") {
		t.Errorf("renderTable did not say what the function table covers:\n%s", table)
	}

	out := captureStdout(t, func() {
		if err := renderJSON(d); err != nil {
			t.Fatalf("renderJSON: %v", err)
		}
	})
	var got struct {
		Groups []struct {
			Name      string     `json:"name"`
			DrillDown *drillJSON `json:"drill_down"`
		} `json:"groups"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("emitted invalid JSON: %v\n%s", err, out)
	}
	if len(got.Groups) != 2 {
		t.Fatalf("want 2 groups, got %d", len(got.Groups))
	}
	if got.Groups[0].DrillDown == nil || got.Groups[0].DrillDown.By != "namespace" {
		t.Errorf("a labelled group lost its drill_down: %+v", got.Groups[0])
	}
	// Explicitly null, not absent: "there is no command for this row" is a
	// fact, and a missing key leaves a consumer to infer it.
	if !strings.Contains(out, `"drill_down": null`) {
		t.Errorf("the unlabeled row should render drill_down as null:\n%s", out)
	}
	if got.Groups[1].DrillDown != nil {
		t.Errorf("the unlabeled row must carry no command: %+v", got.Groups[1])
	}
}

// gatherReport must attach the commands. The renderer test above builds
// reportData by hand, so it stays green if nothing ever populates the field --
// which is precisely how a feature ships as dead code.
func TestGatherReportAttachesDrillDowns(t *testing.T) {
	f := reportFixture(t)
	f.merges[testType+`{cluster="tc"}`] = cpuProfile(t, 100)
	f.merges[testType+`{cluster="vps"}`] = cpuProfile(t, 50)
	f.merges[testType] = cpuProfile(t, 150)

	o := testOptions()
	o.insecure = true
	o.top = 5

	end := time.Now()
	d, err := gatherReport(context.Background(), testClient(f, time.Minute), o, end.Add(-time.Hour), end)
	if err != nil {
		t.Fatalf("gatherReport: %v", err)
	}
	if len(d.Groups) == 0 {
		t.Fatal("no groups to attach anything to")
	}
	for _, g := range d.Groups {
		if g.Unlabeled {
			continue
		}
		if g.DrillDown == nil {
			t.Fatalf("group %q carries no drill-down command", g.Name)
		}
		want := `cluster=` + strconv.Quote(g.Name)
		if g.DrillDown.Match != want {
			t.Errorf("group %q: match = %q, want %q", g.Name, g.DrillDown.Match, want)
		}
		// The command has to be the one this very run would have produced,
		// not an approximation of it: what the hint prints and what the tool
		// queries are built by the same code for exactly this reason.
		if sel := selector(d.ProfileType, "", "", g.DrillDown.Match); !strings.Contains(sel, want) {
			t.Errorf("group %q: match does not round-trip into a selector: %s", g.Name, sel)
		}
	}
}
