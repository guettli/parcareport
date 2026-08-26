package main

import (
	"fmt"
	"os"
	"text/tabwriter"
)

func newTab() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
}

func printGroupTable(header string, rows []Row, total float64) {
	w := newTab()
	fmt.Fprintf(w, "%s\tCORES\t%%TOTAL\n", header)
	for _, r := range rows {
		fmt.Fprintf(w, "%s\t%.3f\t%.1f\n", r.Name, r.Cores, r.Pct)
	}
	fmt.Fprintf(w, "TOTAL\t%.3f\t100.0\n", total)
	w.Flush()
}

func printFunctionTable(rows []Row, top int, total float64) {
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
		fmt.Fprintf(w, "%s\t%.3f\t%.3f\t%.1f\n", truncate(r.Name, 60), r.Cores, r.Flat, pct)
	}
	w.Flush()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
