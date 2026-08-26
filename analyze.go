package main

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/google/pprof/profile"
)

// Row is one line of a report table.
type Row struct {
	Name  string
	Cores float64 // average cores busy over the window
	Flat  float64 // self cores (function tables only)
	Pct   float64
}

// Metric is how one profile type wants to be summarized. Not every profile is
// a rate: dividing a heap size or a goroutine count by the window is nonsense,
// so the unit decides both the number and the column header.
type Metric struct {
	Header string  // column title, e.g. CORES / BLOCKED / BYTES / COUNT
	Value  float64 // already normalized per the header's meaning
	Rate   bool    // true if Value was divided by the window
}

// interpret summarizes a profile the way its units actually mean.
//
//   - CPU time (samples x period, or an explicit time unit under a "cpu" period)
//     becomes average cores busy.
//   - Off-CPU wallclock time becomes the average number of threads blocked --
//     the same arithmetic, but it is not a core and must not say so.
//   - Heap profiles are byte totals; goroutine and mutex profiles are counts.
//     Neither is divided by anything.
func interpret(p *profile.Profile, window time.Duration) (Metric, error) {
	idx, unit := valueIndex(p)
	var total int64
	for _, s := range p.Sample {
		if idx < len(s.Value) {
			total += s.Value[idx]
		}
	}

	switch unit {
	case "bytes":
		return Metric{Header: "BYTES", Value: float64(total)}, nil
	case "count":
		// A count with a time-based period is a sampled duration (parca-agent's
		// CPU profile); a count with no such period is a real count.
		if p.Period > 0 && p.PeriodType != nil && isTimeUnit(p.PeriodType.Unit) {
			secs, err := scaleToSeconds(p, total, unit)
			if err != nil {
				return Metric{}, err
			}
			return Metric{Header: "CORES", Value: cores(secs, window), Rate: true}, nil
		}
		return Metric{Header: "COUNT", Value: float64(total)}, nil
	}

	if isTimeUnit(unit) {
		secs, err := scaleToSeconds(p, total, unit)
		if err != nil {
			return Metric{}, err
		}
		header := "BLOCKED" // wallclock: average threads waiting, not cores
		if sampleTypeIsCPU(p, idx) {
			header = "CORES"
		}
		return Metric{Header: header, Value: cores(secs, window), Rate: true}, nil
	}
	return Metric{}, fmt.Errorf("unsupported sample unit %q", unit)
}

func isTimeUnit(u string) bool {
	switch u {
	case "nanoseconds", "microseconds", "milliseconds", "seconds":
		return true
	}
	return false
}

func sampleTypeIsCPU(p *profile.Profile, idx int) bool {
	if idx < len(p.SampleType) && p.SampleType[idx].Type == "cpu" {
		return true
	}
	return p.PeriodType != nil && p.PeriodType.Type == "cpu"
}

// cpuSeconds converts a merged pprof into CPU-seconds.
//
// This is the subtle part. parca-agent's CPU profile has sample type
// "samples:count" with period type "cpu:nanoseconds" -- the sample VALUE is a
// count of stack samples, not a duration. Multiplying by the period converts it
// to CPU time. Profiles whose sample unit is already a time unit are used
// as-is. Getting this wrong silently scales the whole report.
func cpuSeconds(p *profile.Profile) (float64, error) {
	idx, unit := valueIndex(p)
	var total int64
	for _, s := range p.Sample {
		if idx < len(s.Value) {
			total += s.Value[idx]
		}
	}
	return scaleToSeconds(p, total, unit)
}

// valueIndex picks which of a profile's sample values to report on, preferring
// an explicit time-unit column over a raw count.
func valueIndex(p *profile.Profile) (int, string) {
	if len(p.SampleType) == 0 {
		return 0, ""
	}
	for i, st := range p.SampleType {
		switch st.Unit {
		case "nanoseconds", "microseconds", "milliseconds", "seconds":
			return i, st.Unit
		}
	}
	return 0, p.SampleType[0].Unit
}

func scaleToSeconds(p *profile.Profile, v int64, unit string) (float64, error) {
	switch unit {
	case "nanoseconds":
		return float64(v) / 1e9, nil
	case "microseconds":
		return float64(v) / 1e6, nil
	case "milliseconds":
		return float64(v) / 1e3, nil
	case "seconds":
		return float64(v), nil
	case "count":
		// Sample counts only become a duration via the sampling period.
		if p.Period <= 0 || p.PeriodType == nil {
			return 0, fmt.Errorf("profile reports counts but has no sampling period; cannot convert to CPU time")
		}
		switch p.PeriodType.Unit {
		case "nanoseconds":
			return float64(v) * float64(p.Period) / 1e9, nil
		case "microseconds":
			return float64(v) * float64(p.Period) / 1e6, nil
		case "milliseconds":
			return float64(v) * float64(p.Period) / 1e3, nil
		case "seconds":
			return float64(v) * float64(p.Period), nil
		}
		return 0, fmt.Errorf("unsupported period unit %q", p.PeriodType.Unit)
	}
	return 0, fmt.Errorf("unsupported sample unit %q", unit)
}

// cores normalizes CPU-seconds by wall-clock, giving "average cores busy".
// This is what makes clusters of different sizes comparable: 0.5 means half a
// core was busy on average for the whole window, whatever the cluster's size.
func cores(cpuSecs float64, window time.Duration) float64 {
	if window <= 0 {
		return 0
	}
	return cpuSecs / window.Seconds()
}

func parsePprof(raw []byte) (*profile.Profile, error) {
	if len(raw) == 0 {
		return nil, nil // no data in window
	}
	p, err := profile.Parse(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("parse pprof: %w", err)
	}
	return p, nil
}

// topFunctions aggregates a profile by function, returning cumulative and flat
// cores. Cumulative counts a function once per sample even if it recurses.
func topFunctions(p *profile.Profile, window time.Duration) ([]Row, error) {
	idx, unit := valueIndex(p)
	m, err := interpret(p, window)
	if err != nil {
		return nil, err
	}
	rate := m.Rate
	cum := map[string]int64{}
	flat := map[string]int64{}

	for _, s := range p.Sample {
		if idx >= len(s.Value) {
			continue
		}
		v := s.Value[idx]
		if v == 0 {
			continue
		}
		seen := map[string]bool{}
		for i, loc := range s.Location {
			for _, line := range loc.Line {
				name := funcName(line)
				if !seen[name] {
					seen[name] = true
					cum[name] += v
				}
				// Leaf frame of the leaf location carries the self time.
				if i == 0 && len(loc.Line) > 0 && line == loc.Line[0] {
					flat[name] += v
				}
			}
		}
	}

	rows := make([]Row, 0, len(cum))
	for name, c := range cum {
		var cs, fs float64
		if rate {
			var err error
			if cs, err = scaleToSeconds(p, c, unit); err != nil {
				return nil, err
			}
			if fs, err = scaleToSeconds(p, flat[name], unit); err != nil {
				return nil, err
			}
		}
		if !rate {
			// Byte and count profiles are totals, not per-second rates.
			rows = append(rows, Row{Name: name, Cores: float64(c), Flat: float64(flat[name])})
			continue
		}
		rows = append(rows, Row{Name: name, Cores: cores(cs, window), Flat: cores(fs, window)})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Cores != rows[j].Cores {
			return rows[i].Cores > rows[j].Cores
		}
		return rows[i].Name < rows[j].Name
	})
	return rows, nil
}

func funcName(l profile.Line) string {
	if l.Function == nil || l.Function.Name == "" {
		// Unsymbolized frames are common for system binaries without debuginfo.
		// Bucket them so they do not fragment the top-N into noise.
		return "[unsymbolized]"
	}
	return l.Function.Name
}
