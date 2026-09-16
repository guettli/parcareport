package main

import (
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
)

// Prometheus's own parser is the authority on where one matcher ends and the
// next begins -- it is the parser the server runs these through. Asserting
// matchers() against it beats a hand-written table, which is only an
// approximation of it and is exactly what missed the escaped-quote case: the
// table had an escaped quote, but only ever as the LAST matcher, where a
// swallowed separator is invisible.
//
// Cases must be valid PromQL, so the count Prometheus reports is meaningful.
func TestMatchersAgreesWithThePromQLParser(t *testing.T) {
	cases := []string{
		`a="1",b="2"`,
		`pod=~"a,b"`,
		`pod="has,comma",ns="x"`,
		`a="quo\"te",b="2"`,
		`pod="has\"q",ns="x"`,
		`a='single'`,
		`a="1",b=~"x,y",c!="z"`,
		// An escaped backslash, then a real closing quote, then a separator.
		// The escape path has to end at the right quote or this one matcher
		// swallows the next.
		`a="x\\",b="2"`,
	}
	for _, in := range cases {
		wrapped := "m{" + in + "}"
		expr, err := parser.NewParser(parser.Options{}).ParseMetricSelector(wrapped)
		if err != nil {
			t.Errorf("%q: prometheus itself rejects it: %v", in, err)
			continue
		}
		want := len(expr) - 1 // minus __name__
		got := len(matchers(in))
		t.Logf("%-26q prometheus=%d ours=%d\n", in, want, got)
		if got != want {
			t.Errorf("%q: prometheus says %d matchers, we split into %d: %#v", in, want, got, matchers(in))
		}
	}
}
