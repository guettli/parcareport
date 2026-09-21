// Command zoo is a workload with deliberate, known bottlenecks.
//
// It exists so the end-to-end test has something to find. Every pathology
// lives in its own top-level function with a distinctive name, so a test can
// assert that the profile blames exactly that name — "parcareport found the
// thing we planted" is a much stronger claim than "parcareport produced
// output".
//
// Each function maps to one row of docs/bottlenecks.md:
//
//	zooBurnCPU         -> CPU-bound compute        (process_cpu)
//	zooChurnAlloc      -> allocation churn         (memory:alloc_space)
//	zooHoldHeap        -> live heap / retention    (memory:inuse_space)
//	zooLeakGoroutines  -> goroutine leak           (goroutine)
//	zooContendMutex    -> lock contention          (mutex:*)
//	zooBlockOnChannel  -> blocking on a primitive  (block:*)
//
// Go's mutex and block profiles are off unless the process turns them on,
// which is itself worth demonstrating: without the two Set* calls in main the
// endpoints return empty profiles and a report cannot tell "no contention"
// from "never measured".
//
// The point is to be *recognisable*, not maximal. Each pathology does only as
// much as it takes to dominate its own profile, so that none of them starves
// the others on a two-core CI runner and the process stays in something like a
// steady state for the length of a job.
package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof handlers on DefaultServeMux
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// retained keeps zooHoldHeap's allocations reachable, so they show up as live
// heap rather than as garbage.
//
// sink stops the compiler deciding the work is dead. It is atomic because
// several pathologies write it concurrently and this file is meant to be
// exemplary: `go build -race` on it should be quiet.
var (
	retained [][]byte
	churned  atomic.Pointer[[]byte]
	sink     atomic.Uint64
)

// maxLeakedGoroutines caps the leak. Unbounded growth would make the workload
// behave differently depending on how far into the job it is sampled: GC
// stack-scan work grows with the goroutine count, so the process would get
// steadily slower rather than staying comparable across runs.
const maxLeakedGoroutines = 20_000

func main() {
	addr := flag.String("addr", "127.0.0.1:6060", "address for the pprof endpoints")
	flag.Parse()

	// Without these, /debug/pprof/mutex and /debug/pprof/block are empty and
	// the e2e test's contention assertions would fail for a reason that has
	// nothing to do with parcareport. Fraction/rate 1 records every event,
	// which is why a short scrape is enough.
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)

	go zooBurnCPU()
	go zooChurnAlloc()
	go zooHoldHeap()
	go zooLeakGoroutines()
	go zooContendMutex()
	go zooBlockOnChannel()

	log.Printf("zoo serving pprof on %s", *addr)
	srv := &http.Server{Addr: *addr, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

// zooBurnCPU is the CPU-bound hot loop. The arithmetic is pointless but real:
// it has to survive the optimizer so the samples land in this frame.
func zooBurnCPU() {
	var x uint64 = 1
	for {
		for i := 0; i < 5_000_000; i++ {
			x = x*6364136223846793005 + 1442695040888963407
			x ^= x >> 33
		}
		sink.Store(x)
		// Yield so this does not starve the other pathologies on a small runner.
		time.Sleep(20 * time.Millisecond)
	}
}

// zooChurnAlloc allocates and discards, which is what shows up in alloc_space
// (and as runtime.mallocgc in the CPU profile) without growing the live heap.
//
// The buffer is larger than Go's 64 KiB implicit-stack limit and escapes via a
// package variable, because a smaller non-escaping make() is stack-allocated
// and the heap profiler never sees it at all — the case silently measured
// nothing until both were true.
//
// The rate is deliberately modest. Allocating gigabytes per second dominates
// alloc_space no better than this does, and it makes the garbage collector,
// rather than the planted bottleneck, the most expensive thing in the CPU
// profile.
func zooChurnAlloc() {
	for {
		for i := 0; i < 20; i++ {
			b := make([]byte, 128*1024)
			b[0] = byte(i)
			sink.Add(uint64(b[0]))
			churned.Store(&b)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// zooHoldHeap retains what it allocates, which is the difference between
// "churn" and "retention": this one moves inuse_space, the other does not.
// It stops at 128 MiB — enough to dominate a small process's live heap.
func zooHoldHeap() {
	for {
		if len(retained) < 512 {
			retained = append(retained, make([]byte, 256*1024))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// zooLeakGoroutines parks goroutines on a channel nobody ever sends to. The
// goroutine profile counts them and names the frame they are stuck in, which
// is the closure below rather than this function.
func zooLeakGoroutines() {
	for {
		if runtime.NumGoroutine() < maxLeakedGoroutines {
			for i := 0; i < 20; i++ {
				go func() {
					blocked := make(chan struct{})
					<-blocked // parked forever, on purpose
				}()
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// zooContendMutex makes several goroutines fight over one mutex. The holder
// does a little work while holding it, so the others actually block and the
// mutex profile records a delay rather than an uncontended fast path.
//
// Note that the profiles blame the closure (zooContendMutex.func1), not this
// function: a goroutine's stack starts at the function it was launched with.
func zooContendMutex() {
	var mu sync.Mutex
	for i := 0; i < 8; i++ {
		go func() {
			for {
				mu.Lock()
				var x uint64
				for j := 0; j < 20_000; j++ {
					x += uint64(j)
				}
				sink.Add(x)
				mu.Unlock()
				// Without this the eight contenders burn more CPU than
				// zooBurnCPU does, which makes the CPU case's headline claim
				// false even though its own assertion still passes.
				time.Sleep(time.Millisecond)
			}
		}()
	}
}

// zooBlockOnChannel blocks receivers on an unbuffered channel that is fed
// slowly. This is the block profile's territory: waiting on a sync primitive
// rather than on a lock.
func zooBlockOnChannel() {
	ch := make(chan int)
	for i := 0; i < 4; i++ {
		go func() {
			for range ch {
			}
		}()
	}
	for {
		ch <- 1
		time.Sleep(25 * time.Millisecond)
	}
}
