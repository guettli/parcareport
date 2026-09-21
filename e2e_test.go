//go:build e2e

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// End-to-end against a real Parca that is scraping cmd/zoo.
//
// The unit tests answer a fake server, so they can only prove rendering: given
// this profile, print these numbers. A fake answers with whatever the test
// already believes, so nothing in that suite fails if parcareport stops
// *finding* things. This one plants known pathologies in a real process, lets
// a real Parca scrape them, and asserts the report blames what we planted.
//
// Run it with:
//
//	PARCA_URL=127.0.0.1:7071 go test -tags e2e -run TestE2EZoo -v .
//
// Skipped unless PARCA_URL is set, so `go test ./...` stays hermetic.

// Each case maps one function in cmd/zoo to the profile that should reveal it.
//
// Membership in the function table is deliberately NOT the assertion. Some of
// these profiles hold fewer functions than --top, so "is it listed" is
// satisfied by a single stray nanosecond, and every case would still pass with
// the wrong name in it. Rank and share are what separate "we found the
// bottleneck" from "the frame exists somewhere".
//
// Share is measured against cum, not the printed %TOTAL: that percentage
// follows the sorted column, which is flat, and the planted frames for the
// goroutine/mutex/block cases have no self time at all -- they are blamed as
// the parents of runtime.gopark and friends.
//
// Unit is the stable machine name (unitName), not the display heading. Those
// assertions are load bearing: a cumulative duration must come back as a
// "seconds" total, never averaged over --from.
var zooCases = []struct {
	name        string
	profileType string
	wantFunc    string
	wantUnit    string
	wantRank    int     // 0-based position in the function table, at worst
	wantMinPct  float64 // share of the profile's total, at least
}{
	// process_cpu is ambiguous on its own: the server offers both a samples
	// count and an explicit nanosecond duration.
	{"cpu_hot_loop", "process_cpu:samples:count:cpu:nanoseconds:delta", "zooBurnCPU", "cores", 10, 5},
	{"alloc_churn", "alloc_space", "zooChurnAlloc", "bytes", 5, 50},
	{"live_heap", "inuse_space", "zooHoldHeap", "bytes", 5, 30},
	// The next three are blamed as *parents* of runtime.gopark and friends, so
	// they have no self time. The table sorts by flat, which puts every
	// trivial flat=1 runtime frame above them and makes their exact rank
	// depend on how many of those happen to exist in a given snapshot. Their
	// share is decisive instead (measured at 99%+, 100% and 40%), so rank is
	// only a loose sanity bound here.
	{"goroutine_leak", "goroutine", "zooLeakGoroutines", "count", 10, 50},
	{"mutex_contention", "mutex:delay", "zooContendMutex", "seconds", 10, 30},
	{"block_on_channel", "block:delay", "zooBlockOnChannel", "seconds", 10, 15},
}

func TestE2EZoo(t *testing.T) {
	addr := os.Getenv("PARCA_URL")
	if addr == "" {
		t.Skip("set PARCA_URL to run the end-to-end test")
	}

	c, err := Dial(addr, true, 60*time.Second, Auth{})
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}

	for _, tc := range zooCases {
		t.Run(tc.name, func(t *testing.T) {
			d, err := gatherZoo(t, c, addr, tc.profileType)
			if err != nil {
				t.Fatalf("gatherReport(%s): %v", tc.profileType, err)
			}

			// Complete only means no group query failed. An empty window is a
			// complete run that found nothing, so that is checked separately.
			if !d.Complete {
				t.Fatalf("run reported itself incomplete: failed=%+v", d.Failed)
			}
			if d.Unit != tc.wantUnit {
				t.Errorf("unit = %q, want %q", d.Unit, tc.wantUnit)
			}
			if len(d.Functions) == 0 {
				t.Fatalf("no functions in report for %s", tc.profileType)
			}

			rank := -1
			for i, f := range d.Functions {
				if strings.Contains(f.Name, tc.wantFunc) {
					rank = i
					break
				}
			}
			if rank < 0 || rank > tc.wantRank {
				t.Fatalf("%s ranked %s, want at worst %d\n%s",
					tc.wantFunc, rankStr(rank), tc.wantRank, topList(d))
			}
			if d.Total == nil || *d.Total <= 0 {
				t.Fatalf("no usable total for %s, cannot judge share", tc.profileType)
			}
			if share := d.Functions[rank].Cum / *d.Total * 100; share < tc.wantMinPct {
				t.Errorf("%s is %.1f%% of the profile, want at least %.1f%%\n%s",
					tc.wantFunc, share, tc.wantMinPct, topList(d))
			}
		})
	}
}

// gatherZoo retries while the report is empty. Four of the six profile types
// are non-delta, so their evidence is a single scrape: if the test starts
// between the server coming up and that scrape landing, the honest answer is
// "not yet" rather than a failure.
func gatherZoo(t *testing.T, c *Client, addr, profileType string) (*reportData, error) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for attempt := 1; ; attempt++ {
		end := time.Now()
		o := options{
			addr:        addr,
			insecure:    true,
			by:          "instance", // the Go scrape tier carries job/instance
			profileType: profileType,
			top:         200, // see the comment on zooCases: rank is the assertion
			concurrency: 2,
			maxGroups:   50,
			timeout:     30 * time.Second,
			// Kept well under `go test -timeout`: six cases share that budget,
			// and a stuck run should report itself rather than be killed.
			deadline: 45 * time.Second,
		}
		// The zoo has only just started, so a wider window would understate
		// every rate by the fraction of it that predates the workload.
		d, err := gatherReport(context.Background(), c, o, end.Add(-5*time.Minute), end)
		if err == nil && len(d.Functions) > 0 {
			return d, nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return nil, err
			}
			return d, nil // let the caller report the empty-result failure
		}
		t.Logf("attempt %d: no data yet for %s, retrying", attempt, profileType)
		time.Sleep(5 * time.Second)
	}
}

func rankStr(rank int) string {
	if rank < 0 {
		return "absent"
	}
	return fmt.Sprintf("%d", rank)
}

// topList renders enough of the table to debug a failure from CI logs alone.
func topList(d *reportData) string {
	var b strings.Builder
	b.WriteString("top functions:\n")
	for i, f := range d.Functions {
		if i >= 12 {
			fmt.Fprintf(&b, "  ... and %d more\n", len(d.Functions)-i)
			break
		}
		fmt.Fprintf(&b, "  %2d  cum=%-14g flat=%-14g %s\n", i, f.Cum, f.Flat, f.Name)
	}
	return b.String()
}
