package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"golang.org/x/term"
)

// progress reports how far a fan-out has got, so a run that takes minutes does
// not look identical to a run that has hung.
//
// It writes to stderr, and only when stderr is a terminal. Stderr so it stays
// out of a redirected report, and only on a terminal because the carriage
// returns that keep it to one line become noise in a log or a CI transcript.
type progress struct {
	out   io.Writer
	verb  string // "merging" or "querying" -- these are not the same work
	label string
	total int
	width int // digits in total, so every line is exactly as wide as the last
	on    bool

	// One mutex over the increment and the write. With only the counter
	// atomic, a goroutine holding 6 could win the write race against one
	// holding 16, and since \r does not erase, the shorter line left the tail
	// of the longer one behind -- "... 6/20" rendered as "... 6/200". It also
	// made the count visibly go backwards.
	mu   sync.Mutex
	done int
}

func newProgress(verb, label string, total int) *progress {
	// A single unit is not a fan-out worth narrating.
	on := total > 1 && term.IsTerminal(int(os.Stderr.Fd()))
	return newProgressTo(os.Stderr, verb, label, total, on)
}

// newProgressTo takes the writer and the decision to render, so the rendering
// can be tested. Deciding inside newProgress left the drawing code with no
// coverage at all and made its only test assert a property of the environment
// -- it passed under `go test`, which pipes, and failed under a pty.
func newProgressTo(out io.Writer, verb, label string, total int, on bool) *progress {
	return &progress{
		out:   out,
		verb:  verb,
		label: label,
		total: total,
		width: len(strconv.Itoa(max(total, 1))),
		on:    on,
	}
}

// start announces the size of the job. This is known before the first query
// returns, and it is what tells you roughly how long to expect.
func (p *progress) start() {
	if !p.on {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintf(p.out, "%s %d %s...", p.verb, p.total, p.label)
}

// step records one finished unit, whether it succeeded or failed -- this counts
// progress, not results.
func (p *progress) step() {
	if !p.on {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done++
	fmt.Fprintf(p.out, "\r%s %d %s... %*d/%d", p.verb, p.total, p.label, p.width, p.done, p.total)
}

// stop clears the line so the report that follows starts clean.
func (p *progress) stop() {
	if !p.on {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintf(p.out, "\r%*s\r", p.lineLen(), "")
}

// lineLen is the exact width of the longest line step can write, so stop
// clears all of it and no more.
func (p *progress) lineLen() int {
	// "<verb> <total> <label>... <done>/<total>"
	return len(p.verb) + 1 + p.width + 1 + len(p.label) + len("... ") + p.width + 1 + p.width
}
