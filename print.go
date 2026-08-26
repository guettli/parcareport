package main

import (
	"fmt"
	"os"
	"text/tabwriter"
)

func newTab() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
}

func printGroupTable(groupHeader, unitHeader string, rows []Row, total float64) {
	w := newTab()
	fmt.Fprintf(w, "%s\t%s\t%%TOTAL\n", groupHeader, unitHeader)
	for _, r := range rows {
		fmt.Fprintf(w, "%s\t%s\t%.1f\n", r.Name, formatValue(r.Cores, unitHeader), r.Pct)
	}
	fmt.Fprintf(w, "TOTAL\t%s\t100.0\n", formatValue(total, unitHeader))
	w.Flush()
}

// formatValue keeps rates at three decimals but renders byte and count totals
// readably -- "1.4 GiB" rather than 1503238553.000.
func formatValue(v float64, unitHeader string) string {
	switch unitHeader {
	case "BYTES":
		return humanBytes(v)
	case "COUNT":
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.3f", v)
}

func humanBytes(v float64) string {
	const unit = 1024
	if v < unit {
		return fmt.Sprintf("%.0f B", v)
	}
	div, exp := float64(unit), 0
	for n := v / unit; n >= unit && exp < 4; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", v/div, "KMGTP"[exp])
}

func printFunctionTable(rows []Row, unitHeader string, top int, total float64) {
	if len(rows) > top {
		rows = rows[:top]
	}
	w := newTab()
	fmt.Fprintf(w, "FUNCTION\tCUM\tFLAT\t%%TOTAL\n")
	for _, r := range rows {
		pct := 0.0
		if total > 0 {
			pct = r.Cores / total * 100
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%.1f\n",
			truncate(r.Name, 60), formatValue(r.Cores, unitHeader), formatValue(r.Flat, unitHeader), pct)
	}
	w.Flush()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
