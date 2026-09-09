package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const (
	outputTable = "table"
	outputJSON  = "json"
)

func parseOutput(s string) (string, error) {
	switch s {
	case "", outputTable:
		return outputTable, nil
	case outputJSON:
		return outputJSON, nil
	}
	return "", fmt.Errorf("--output must be table or json, got %q", s)
}

// renderTable prints the report for a person to read.
func renderTable(d *reportData) {
	if d.noRows {
		// Nothing to tabulate. The banner says which of the two reasons it is.
		fmt.Print(d.banner)
		return
	}
	fmt.Printf("%s  %s .. %s  (%s)\n\n",
		d.ProfileType, d.Start.Format("2006-01-02T15:04:05Z"), d.End.Format("2006-01-02T15:04:05Z"),
		windowLabel(d.WindowSecs))

	rows := make([]Row, 0, len(d.Groups))
	for _, g := range d.Groups {
		r := Row{Name: g.Name, Cores: g.Value}
		if g.Pct != nil {
			r.Pct = *g.Pct
		}
		rows = append(rows, r)
	}
	total := 0.0
	if d.Total != nil {
		total = *d.Total
	}
	printGroupTable(strings.ToUpper(d.GroupBy), d.header, rows, total, d.knowTotal)
	if d.EmptyGroups > 0 {
		fmt.Printf("(%d %s values had no samples in this window, omitted)\n", d.EmptyGroups, d.GroupBy)
	}

	// One banner, however many things went wrong.
	if len(d.failed) > 0 || d.overallErr != nil {
		fmt.Print("\n!! INCOMPLETE\n")
		all := d.failed
		if len(d.failed) > 0 {
			if d.knowTotal {
				fmt.Printf("!! %d of %d %s queries failed. The totals and percentages above\n"+
					"!! EXCLUDE them and are therefore wrong.\n", len(d.failed), d.groupCount, d.GroupBy)
			} else {
				fmt.Printf("!! %d of %d %s queries failed. The sum above excludes them.\n",
					len(d.failed), d.groupCount, d.GroupBy)
			}
		}
		if d.overallErr != nil {
			fmt.Printf("!! The unfiltered merge failed, so percentages, the (unlabeled) row\n"+
				"!! and the hot-function table are missing, and any series carrying no\n"+
				"!! %q label is absent from the sum above.\n", d.GroupBy)
			all = append(append([]failure{}, d.failed...),
				failure{group: "(overall)", msg: shortErr(d.overallErr)})
		}
		printFailures(all, mergeQuery)
	}

	if len(d.Functions) > 0 {
		fmt.Println()
		fns := make([]Row, 0, len(d.Functions))
		for _, f := range d.Functions {
			fns = append(fns, Row{Name: f.Name, Cores: f.Cum, Flat: f.Flat})
		}
		printFunctionTable(fns, d.header, len(fns), total, d.sortKey)
	}
}

// renderJSON prints the report for a program to read.
//
// Indented, because a report is read by people too when something looks wrong,
// and a single line of JSON is not. It is still one document, so `jq` and
// friends handle it either way.
func renderJSON(d *reportData) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(d); err != nil {
		return fmt.Errorf("writing JSON: %w", err)
	}
	return nil
}

// renderJSONError emits a document for a run that could not get far enough to
// produce a report.
//
// A consumer should parse one shape whatever happened. Printing nothing on
// failure would make "the window was empty" and "the command broke"
// indistinguishable to a script -- the same conflation the tool refuses
// everywhere else.
func renderJSONError(o options, d *reportData, err error) {
	if d == nil {
		d = &reportData{
			ProfileType: o.profileType,
			GroupBy:     o.by,
			Match:       o.match,
			Groups:      []groupJSON{},
			Functions:   []funcJSON{},
			Failed:      []failJSON{},
		}
	}
	d.Complete = false
	if err != nil {
		d.Error = err.Error()
	}
	_ = renderJSON(d)
}

// windowLabel formats the window the way time.Duration.Round(time.Second) did.
func windowLabel(secs float64) string {
	h := int(secs) / 3600
	m := (int(secs) % 3600) / 60
	s := int(secs) % 60
	if h > 0 {
		return fmt.Sprintf("%dh%dm%ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
