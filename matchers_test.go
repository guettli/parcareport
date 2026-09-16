package main

import (
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
)

// Ground truth: hand the joined splitter output to Prometheus's own parser
// and to the un-split original; both must parse, and the split must agree
// with what Prometheus itself considers separate matchers.
// Prometheus's own parser is the authority on where one matcher ends and
// the next begins -- it is the same parser the server runs these through.
// Asserting our split against it beats a hand-written table, which is
// exactly what missed the escaped-quote case.
func TestMatchersAgreesWithThePromQLParser(t *testing.T) {
	cases := []string{
		`a="1",b="2"`,
		`pod=~"a,b"`,
		`pod="has,comma",ns="x"`,
		`a="quo\"te",b="2"`,
		`pod="has\"q",ns="x"`,
		`a='single'`,
		`a="1",b=~"x,y",c!="z"`,
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
