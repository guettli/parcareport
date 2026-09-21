package main

import (
	"strings"
	"testing"
)

// A --match sends EVERY zero group to ExcludedGroups, whether or not the
// value would have had samples -- the values come from the server's
// unfiltered list, so the tool cannot tell "pruned" from "idle" and says the
// reading true of both. This pins that: a value with a real merge that
// yields 0 is still counted as excluded, so it is reported as yielding
// nothing under the matcher rather than as a window with no samples.
func TestMatchSendsZeroGroupsToExcludedNotEmpty(t *testing.T) {
	f := reportFixture(t)
	f.values["cluster"] = []string{"pruned", "idlezero"}
	// "pruned" has no merge entry -> the server sends no bytes.
	// "idlezero" parses, so mergeOne does not take its p == nil path, but
	// contributes 0 -- the genuinely-idle shape.
	f.merges[testType+`{cluster="idlezero",namespace="default"}`] = cpuProfile(t, 0)

	o := testOptions()
	o.match = `namespace="default"`
	out, err := runReport(t, f, o)
	// Answered, and the answer was nothing: an outcome, not a failure.
	if err != nil {
		t.Fatalf("a matcher that pruned everything is not a failure: %v", err)
	}
	// Both land in the same bucket, so the message says "all" and names the
	// matcher. It must NOT say the window had no samples.
	if !strings.Contains(out, "all 2") {
		t.Errorf("both zero groups share the excluded bucket:\n%s", out)
	}
	if strings.Contains(out, "no samples in this window") {
		t.Errorf("the matcher is the named cause, not the window:\n%s", out)
	}
}
