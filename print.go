package main

import (
	"fmt"
	"os"
	"text/tabwriter"
)

func newTab() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
}

// printGroupTable renders the breakdown. knowTotal says whether the grand
// total is trustworthy: when the unfiltered merge failed there is no
// denominator, so the %TOTAL column is dropped rather than filled with
// percentages of a subtotal that silently omits whatever is missing.
func printGroupTable(groupHeader, unitHeader string, rows []Row, total float64, knowTotal bool) {
	w := newTab()
	if knowTotal {
		fmt.Fprintf(w, "%s\t%s\t%%TOTAL\n", groupHeader, unitHeader)
	} else {
		fmt.Fprintf(w, "%s\t%s\n", groupHeader, unitHeader)
	}
	for _, r := range rows {
		if knowTotal {
			fmt.Fprintf(w, "%s\t%s\t%.1f\n", r.Name, formatValue(r.Cores, unitHeader), r.Pct)
			continue
		}
		fmt.Fprintf(w, "%s\t%s\n", r.Name, formatValue(r.Cores, unitHeader))
	}
	if knowTotal {
		fmt.Fprintf(w, "TOTAL\t%s\t100.0\n", formatValue(total, unitHeader))
	} else {
		// Named so it cannot be mistaken for the fleet-wide figure.
		fmt.Fprintf(w, "SUM OF LISTED\t%s\n", formatValue(total, unitHeader))
	}
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

// printFunctionTable lists the top functions. The sorted column is marked with
// a `*` and the percentage follows it, so which question the table answers is
// visible in the table itself rather than inferred from the flags.
func printFunctionTable(rows []Row, unitHeader string, top int, total float64, by sortKey) {
	if len(rows) > top {
		rows = rows[:top]
	}
	cumHdr, flatHdr := "CUM", "FLAT*"
	if by == sortCum {
		cumHdr, flatHdr = "CUM*", "FLAT"
	}
	w := newTab()
	fmt.Fprintf(w, "FUNCTION\t%s\t%s\t%%TOTAL\n", cumHdr, flatHdr)
	for _, r := range rows {
		v := r.Flat
		if by == sortCum {
			v = r.Cores
		}
		pct := 0.0
		if total > 0 {
			pct = v / total * 100
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%.1f\n",
			truncate(r.Name, 60), formatValue(r.Cores, unitHeader), formatValue(r.Flat, unitHeader), pct)
	}
	w.Flush()
}

// truncate shortens a name to n runes. Counting bytes would both cut a
// multi-byte symbol mid-rune and misjudge the column width.
func truncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	// n-1 runes plus the one-rune ellipsis, so the result is exactly n wide.
	return string(r[:n-1]) + "…"
}
