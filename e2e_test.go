//go:build e2e

package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// End-to-end against a real Parca that is scraping cmd/zoo.
//
// The unit tests prove parcareport renders a canned profile correctly. They
// cannot prove it finds a real bottleneck, because a fake server answers with
// whatever the test already believes. This one plants known pathologies in a
// real process, lets a real Parca scrape them, and asserts the report blames
// the function we planted -- and, just as importantly, that the run reported
// itself complete while doing so.
//
// Run it with:
//
//	PARCA_URL=localhost:7070 go test -tags e2e -run TestE2EZoo -v
//
// Skipped unless PARCA_URL is set, so `go test ./...` stays hermetic.

// Each case maps one function in cmd/zoo to the profile that should reveal it
// and the unit that profile should report in. Unit is the *stable machine
// name* (unitName), not the display heading. These assertions are not padding:
// a duration that is a cumulative counter must come back as a "seconds" total,
// never averaged over --from.
var zooCases = []struct {
	name        string
	profileType string
	wantFunc    string
	wantUnit    string
}{
	{"cpu_hot_loop", "process_cpu:samples:count:cpu:nanoseconds:delta", "zooBurnCPU", "cores"},
	{"alloc_churn", "alloc_space", "zooChurnAlloc", "bytes"},
	{"live_heap", "inuse_space", "zooHoldHeap", "bytes"},
	{"goroutine_leak", "goroutine", "zooLeakGoroutines", "count"},
	{"mutex_contention", "mutex:delay", "zooContendMutex", "seconds"},
	{"block_on_channel", "block:delay", "zooBlockOnChannel", "seconds"},
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

	end := time.Now()
	start := end.Add(-15 * time.Minute)

	for _, tc := range zooCases {
		t.Run(tc.name, func(t *testing.T) {
			o := options{
				addr:        addr,
				insecure:    true,
				by:          "instance", // the Go scrape tier carries job/instance
				profileType: tc.profileType,
				top:         30,
				concurrency: 2,
				maxGroups:   50,
				timeout:     60 * time.Second,
				deadline:    3 * time.Minute,
			}

			d, err := gatherReport(context.Background(), c, o, start, end)
			if err != nil {
				t.Fatalf("gatherReport(%s): %v", tc.profileType, err)
			}

			// A run that failed some groups proves nothing either way, so it
			// is a failure here rather than a silent pass on partial data.
			if !d.Complete {
				t.Fatalf("run reported itself incomplete: failed=%+v", d.Failed)
			}
			if d.Unit != tc.wantUnit {
				t.Errorf("unit = %q, want %q", d.Unit, tc.wantUnit)
			}
			if len(d.Functions) == 0 {
				t.Fatalf("no functions in report for %s", tc.profileType)
			}

			names := make([]string, 0, len(d.Functions))
			found := false
			for _, f := range d.Functions {
				names = append(names, f.Name)
				if strings.Contains(f.Name, tc.wantFunc) {
					found = true
				}
			}
			if !found {
				t.Errorf("planted bottleneck %s was not blamed.\ntop functions: %s",
					tc.wantFunc, strings.Join(names, "\n               "))
			}
		})
	}
}
