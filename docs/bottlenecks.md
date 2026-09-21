# Bottlenecks: which ones `parcareport` can find, and which it cannot

This is a map of the bottlenecks that make software slow, and an honest verdict
for each: can `parcareport` (really: Parca) show it, how, and if not, why not
and where to look instead.

A report which silently covers only part of the space is worse than one that
says what it cannot see. Neither a human nor an agent should ever conclude
"parcareport found nothing, therefore nothing is wrong" — for whole classes of
problem, finding nothing is the only thing it *could* do.

## The one rule that explains every verdict below

**Parca samples stacks. It tells you _where in your code_ time is spent or
memory is held. It does not measure hardware counters, device latency, or queue
depth — so it cannot tell you _why the machine underneath was slow_.**

"Your code spends 40% of its wall time inside `os.File.Read`" is a Parca
answer. "Your disk served 4k IOPS at 12 ms p99" is not, and never will be.

One useful corollary: the agent profiles the **kernel** too, so kernel-side CPU
cost (`do_syscall_64`, `__schedule`, `ksoftirqd`, `handle_mm_fault`) *is*
visible. The rule excludes counters and device metrics, not kernel code.

## Three outcomes, not two

Before reading any verdict, know that a run has three possible endings, and
only one of them is evidence. `--output=json` names which one in `outcome`:

| `outcome` | `complete` | exit | means |
| :--- | :--- | :--- | :--- |
| `found` | `true` | 0 | rows were produced |
| `empty` | `true` | 0 | every query was answered, and the answer was nothing |
| `incomplete` | `false` | non-zero | something was not answered, so what is missing is unknown |

1. **Found it** — a bottleneck is named.
2. **Cannot see it** — this document's ❌ and ⚠️ rows, or `outcome: "empty"`.
   Absence of evidence, and it is *evidence of absence* only to the extent the
   ❌ and ⚠️ rows allow.
3. **The run was incomplete** — `outcome: "incomplete"`. `parcareport` never
   presents partial results as complete: it prints `!! INCOMPLETE` and sets
   `"complete": false`. **Check that before concluding anything.**

The distinction between 2 and 3 is the difference between "this service is
idle" and "your monitoring is broken", and it used to be unavailable: an
answered query that found nothing and three dead queries both produced
`complete: false` and a non-zero exit, separable only by reading an English
error string. An idle window is now a **successful measurement** — exit 0, `complete: true` —
and a `NOTES` entry says which flavour of nothing it was:

| note code | outcome | exit | |
| :--- | :--- | :--- | :--- |
| `empty_window` | `empty` | 0 | every query answered, nothing there |
| `match_pruned_everything` | `empty` | 0 | the matcher selected nothing — also what a typo'd `--match` looks like |
| `no_rows_queries_failed` | `incomplete` | non-zero | nothing came back; whether the window is idle is **unknown** |
| `profile_type_unverified` | `incomplete` | non-zero | no data *and* the type was never checked, so this is evidence of neither |

The last two are outcome 3, not outcome 2: they did not look.

Also note what `overview` actually sweeps: **CPU, plus live heap
(`inuse_space`)**. It does not run `wallclock`, `mutex`, `block`, `goroutine`
or `alloc_space`. So "the overview found nothing" says nothing at all about
lock contention, blocking, goroutine leaks, allocation churn or off-CPU waits —
each of those needs its own explicit `--profile-type` run.

<a id="coverage"></a>

## What this Parca actually collects

Coverage comes in two tiers, and the tier decides which bottlenecks are visible
for which workloads.

| Tier | Profile types | Covers |
|---|---|---|
| **eBPF agent** | `…:cpu:nanoseconds:delta`, `…:wallclock:nanoseconds:…` | every process on every node, kernel included |
| **Go `/debug/pprof` scrape** | `memory:{inuse,alloc}_{space,objects}`, `goroutine`, `block:{delay,contentions}`, `mutex:{delay,contentions}` | only the scraped Go services |

**Labels are not guaranteed.** The agent tier always carries `node` and `comm`.
`cluster` requires `--metadata-external-labels` on the agents;
`namespace`, `workload`, `workload_kind` and `container` exist only if the
agents were given `relabel_configs` via `--config-path`. A stock agent
deployment commonly offers only a subset — `overview` reporting just
`cluster comm node` is a normal outcome, not a fault. Breakdowns whose label is
missing are skipped, not faked. The scrape tier carries `job`/`instance`
instead, so it is grouped with `--by=instance`.

Off-CPU (`wallclock`) additionally requires `--off-cpu-threshold` on the agents.

<a id="symbolization"></a>

**Symbolization is itself a coverage limit.** Without frame pointers or
debuginfo, stacks collapse into `[unsymbolized]`. The CPU is still measured but
no longer attributable — a profile dominated by `[unsymbolized]` is a tooling
problem to fix, not a finding to act on.

Legend: **✅ yes** · **⚠️ partially / only targeted / with caveats** · **❌ not visible**

---

## 1. CPU

### ✅ CPU-bound compute (hot loops, expensive algorithms)

*Tested: [`zooBurnCPU`](../cmd/zoo/main.go) planted, `cpu_hot_loop` asserted in [e2e_test.go](../e2e_test.go).*
The core case, and the one everything else is measured against.

```sh
parcareport --by=workload                      # who burns cores
parcareport --by=comm --match 'workload="x"'   # which process
parcareport --match 'workload="x"'             # cluster breakdown + hot functions
```
`CORES` is absolute (CPU-seconds / wall-seconds), so it is comparable across
clusters and against what you pay for.

### ✅ Expensive serialization, reflection, crypto, compression
Not a separate class so much as the most common *shape* of the one above — it
shows up as a recognizable library frame (`encoding/json`, `reflect`, `tls`,
`gzip`) near the top of self time.

### ✅ Performance regressions over time
Take the same CPU breakdown over two windows and compare. CPU is a delta
profile, so this is meaningful. **Never do this with `alloc_space` or the
`mutex`/`block` counters** — see the age caveat under §2.

### ⚠️ Syscall overhead
Kernel frames appear in agent stacks, so a process dominated by syscall entry
is visible as such (`do_syscall_64` routinely tops real tables). What you
cannot get is a syscall *count* or per-syscall latency.
→ `strace`, `bpftrace`.

<a id="amdahl"></a>

### ⚠️ Insufficient parallelism (serialization, Amdahl)
An inference: a workload pinned at ≈1.0 `CORES` while work queues up is
single-threaded. Parca has no notion of "work queued", so that half of the
judgement comes from metrics or from the application.

### ⚠️ Context-switch storms
Visible as self time in `__schedule` / `finish_task_switch`. The *rate* of
context switches is not available.

### ⚠️ `GOMAXPROCS` larger than the CPU limit
The classic Kubernetes Go problem: `GOMAXPROCS` defaults to the node's core
count, so a container limited to 500m runs a scheduler and GC sized for a much
bigger machine, producing throttling and GC thrash. Parca shows the symptom
(GC and scheduler frames costing more than the work) but not the cause — the
`GOMAXPROCS` value itself is not in any profile.

<a id="throttling"></a>

### ❌ CPU throttling (cgroup quota)
A throttled container is *runnable but not running*: not on-CPU, so the CPU
profile does not sample it, and not blocked in your code, so no stack explains
it. In fairness, a container pinned at exactly its quota is the same *shape* of
inference as Amdahl above, and it is the most common Kubernetes CPU bottleneck
there is — but the throttling fact itself lives in cgroup accounting.
→ `container_cpu_cfs_throttled_seconds_total` / `cpu.stat` in your metrics stack.

### ❌ Run-queue latency (noisy neighbours, oversubscription)
Mostly the same reason. Off-CPU sampling does span deschedule→reschedule, so
involuntary preemption technically lands on the preempted frame — but that is
noise spread across every frame rather than a signal you can act on.
→ node metrics, PSI (pressure stall information), scheduler tracepoints.

### ❌ Frequency/thermal throttling, SMT contention
No code location; properties of the machine's power and topology state.
→ `perf stat`, `turbostat`.

---

## 2. Memory

### ✅ Live heap and leaks (Go services)

*Tested: [`zooHoldHeap`](../cmd/zoo/main.go) planted, `live_heap` asserted in [e2e_test.go](../e2e_test.go).*
`memory:inuse_space` is the live heap by allocation site.

```sh
parcareport --profile-type=inuse_space --by=instance
```

A leak is *growth*, and **one run does not show growth**: a non-delta profile is
merged over a single scrape interval, so you get one number, not a series. Run
it repeatedly, or walk `--to` backwards, and compare. Watch `stale_series` /
`doubled_series` in the output — a series that disappeared is counted, not
silently dropped.

<a id="gc-pressure"></a>

### ✅ Allocation churn and GC pressure (Go services)

*Tested: [`zooChurnAlloc`](../cmd/zoo/main.go) planted, `alloc_churn` asserted in [e2e_test.go](../e2e_test.go).*
Two independent signals agree here, which is what makes it trustworthy:
`memory:alloc_space` shows *which code allocates*, and the CPU profile shows
the *cost* as time in `runtime.mallocgc`, `runtime.gcBgMarkWorker` and
especially `runtime.gcAssistAlloc` — mark assist is where GC pressure turns
into request latency, and unlike stop-the-world pauses it is plainly visible in
the CPU profile.

⚠️ **Age caveat:** memory profiles are cumulative since process start, so
`alloc_space --by=instance` ranks instances partly by *how long they have been
running*, not by allocation rate. Comparing two processes of different ages
measures their ages. See the README, "Measuring before and after a change".

### ⚠️ GC pause latency
You can see what the collector costs in CPU; you cannot see the *pause
distribution* — how long any individual stop-the-world was, or which request it
hit.
→ `go_gc_duration_seconds` (default Go collector), or `go_gc_pauses_seconds`
where runtime-metrics collection is enabled; `GODEBUG=gctrace=1`.

### ⚠️ OOM kills
The kill is a kernel event with no stack; the profile simply stops. But the
*cause* is usually visible beforehand in `inuse_space` (what was retained) or
`alloc_space` (what churned), so Parca is the right tool for the post-mortem
even though it cannot raise the alarm.
→ the alarm comes from `kube_pod_container_status_last_terminated_reason`, dmesg.

### ⚠️ Page faults and swap
Not invisible, contrary to intuition. Minor faults are on-CPU kernel work and
appear in agent self time (`handle_mm_fault`, `clear_page_erms`, `__do_fault`);
major faults and swap-in park the thread and appear as off-CPU waits at the
faulting frame. What you do not get is a fault *rate*.
→ node metrics, `vmstat`.

### ❌ Cache misses (L1/L2/LLC), memory bandwidth saturation, NUMA remote access, TLB misses, false sharing
These are *stall reasons inside an instruction*, measured by the CPU's
performance monitoring unit. A sampling profiler sees that a function is on-CPU
for a long time; it cannot see whether those cycles did useful work or waited
on a cache line. Two functions with identical profiles can differ 10× in
efficiency and Parca reports them the same.
→ `perf stat` (IPC, `cache-misses`, `LLC-load-misses`), `perf c2c` for false
sharing, `numastat`.

**This is the single largest blind spot**, and the most common way a CPU
profile misleads: "this function is hot" can mean "this function is
memory-bound", and optimizing its logic then changes nothing.

---

## 3. Storage I/O

### ⚠️ Where your code waits on disk
Off-CPU (`wallclock`) stacks show a thread parked in `read`/`write`/`fsync`,
which identifies the *call site* that waits. Read [the off-CPU
trap](#the-off-cpu-trap) first, and use it targeted:

```sh
parcareport --profile-type=wallclock --by=comm --match 'workload="x"'
```

### ❌ Disk latency, IOPS, throughput, queue depth, read amplification
Numbers about the device, not your code. Parca knows a thread waited; not that
the disk took 12 ms, that the queue was 64 deep, or that the journal was the
contended resource.
→ node-exporter disk metrics, `iostat`, `biolatency`.

---

## 4. Network and external dependencies

### ✅ Dependencies you run yourself
Cross-*process* is not cross-*machine*: the agent profiles **every** process on
your nodes, so a self-hosted database, cache or proxy is profiled like anything
else. "ClickHouse is burning 1.8 cores in `ThreadPoolImpl`" is a Parca answer.
Only dependencies outside your fleet are opaque.

### ⚠️ Where your code waits on the network
Same mechanism and caveat as disk: off-CPU stacks show a goroutine parked in
`netpoll`/`Read`, or in a connection-pool acquire.

### ⚠️ Connection-pool exhaustion
Many goroutines blocked at the pool's acquire function — visible in the
`goroutine` profile for Go services, and as off-CPU time at that frame.

### ⚠️ Packet-processing cost
The *device* numbers (bandwidth, drops, retransmits) are invisible, but the
kernel CPU cost of moving packets is not: high packet rates surface as
`ksoftirqd` / softirq self time under `--by=comm`.

### ❌ RPC/DB latency attribution, retries, head-of-line blocking
Parca cannot tell you the *remote* service took 300 ms, only that you waited.
Time inside another process is invisible and there is no request ID tying a
wait to a downstream span.
→ distributed tracing; server-side latency metrics.

---

## 5. Concurrency and synchronization

### ✅ Lock contention (Go services) — with three conditions

*Tested: [`zooContendMutex`](../cmd/zoo/main.go) planted, `mutex_contention` asserted in [e2e_test.go](../e2e_test.go).*
`mutex:delay` attributes contention time and `mutex:contentions` the event
count. Before trusting either:

- **They are off by default.** Without `runtime.SetMutexProfileFraction` (and
  `runtime.SetBlockProfileRate` for `block`) in the service, the endpoints
  return empty profiles. An empty result is then indistinguishable from "no
  contention" — exactly the conflation this tool refuses everywhere else.
- **`mutex:delay` attributes to the goroutine that _unlocked_**, not to the
  goroutines that waited. `block:delay` is the other way round — it attributes
  to the blocked goroutine. Read the stacks with that asymmetry in mind.
- **`delay` is a cumulative total, not a rate.** `mutex:delay` and `block:delay`
  accumulate since process start, so they are reported as a `SECONDS` total
  rather than averaged over the window — the number does not move when you
  change `--from`. The age caveat from §2 applies in full: ranking instances by
  a lifetime counter partly ranks them by uptime. For a like-for-like
  comparison use `mutex:contentions` (a clean `COUNT`), or compare the same
  instance before and after a change.

```sh
parcareport --profile-type=mutex:contentions --by=instance   # comparable count
parcareport --profile-type=mutex:delay --by=instance         # SECONDS waited
```

### ✅ Blocking on sync primitives (Go services)

*Tested: [`zooBlockOnChannel`](../cmd/zoo/main.go) planted, `block_on_channel` asserted in [e2e_test.go](../e2e_test.go).*
`block:delay` / `block:contentions` cover channel sends/receives, `select`,
`WaitGroup` and mutex waits, attributed to the blocking call site. Same three
conditions as above.

### ✅ Goroutine leaks and runaway concurrency (Go services)

*Tested: [`zooLeakGoroutines`](../cmd/zoo/main.go) planted, `goroutine_leak` asserted in [e2e_test.go](../e2e_test.go).*
`goroutine` by instance; a count that climbs and never returns is a leak, and
the profile names the function they are parked in. As with heap, growth needs
repeated runs — one run is one number.

### ⚠️ Locking in non-Go services
There is no `mutex`/`block` tier for a JVM, Python, Node or C++ service, but
the agent still sees them: futex waits appear in off-CPU stacks, and spinning
appears as on-CPU `__pthread_mutex_lock`. Coarser than Go's profiles, not
nothing.

### ⚠️ Deadlock / livelock
Goroutines permanently parked at the same frames, so successive profiles look
identical and work stops — inferable, but Parca samples rather than dumping and
has no notion of a lock *cycle*.
→ `SIGQUIT` goroutine dump, `go tool trace`.

### ❌ Priority inversion, thundering herd, scheduler fairness
Properties of scheduling decisions over time, not of where code sits.
→ `go tool trace`, scheduler tracepoints.

---

## 6. Time-shaped bottlenecks

### ❌ Tail latency and per-request attribution
An aggregate profile cannot tell you *which* request was slow. A code path
taken 0.1% of the time that dominates p99 is invisible by construction — it is
0.1% of the samples. Profiles answer "where does the bulk of time go", which is
a p50 question.
→ tracing, per-request profiling.

### ⚠️ Transient spikes and short-lived processes
`CORES` is an average over the window, so a two-minute spike inside a six-hour
window is averaged into nothing — narrow `--from`/`--to` around the incident or
you will not see it. Separately, processes that live under a second (cron jobs,
CI steps, forked helpers) are barely sampled at all at ~19 Hz. This class is
the most likely source of a false "nothing is wrong".

---

## 7. Deployment and lifecycle

### ❌ Cold start, image pull, startup time
The interesting window is before or during process start, when the agent has
barely sampled it, and much of the cost (image pull, volume mount) is outside
the process entirely.
→ kubelet events, pod startup metrics.

### ❌ Under-replication, autoscaling lag, misconfigured limits
Capacity-planning facts. Parca can say a workload burns 0.7 cores; whether that
is *too few replicas* depends on demand and SLOs it knows nothing about.
→ metrics + HPA state.

---

## The off-CPU trap

Off-CPU (`wallclock`, reported as `BLOCKED`) is the only fleet-wide signal for
"waiting", which makes it tempting to sweep the way CPU is swept. **Do not.**
Off-CPU totals are dominated by threads that are *idle, not stuck*: a parked
thread-pool worker is "blocked" exactly like a thread stuck behind a lock, and
there are always far more of the former. A fleet-wide off-CPU breakdown ranks
services by how many idle threads they keep, which is not a bottleneck ranking.

Off-CPU earns its keep **targeted**: pick one operation you already suspect is
slow and look for waits on its critical path. Judge the stacks, never the
totals — including the per-group totals table printed above them. See the
README, "Reading `BLOCKED` (off-CPU) honestly".

This is why `overview` never sweeps off-CPU at all.

---

## Which of these are covered by a test

Six of the ✅ rows are proven end to end: [`cmd/zoo`](../cmd/zoo/main.go) plants
the bottleneck in a real process, a real Parca scrapes it, and
[`e2e_test.go`](../e2e_test.go) asserts the report blames the planted function
**by rank and share** -- not merely that the name appears somewhere in the
table, which several of these profiles are small enough to satisfy by accident.

| Bottleneck | Planted by | Asserted by | Profile |
|---|---|---|---|
| CPU-bound compute | `zooBurnCPU` | `cpu_hot_loop` | `process_cpu` |
| Allocation churn | `zooChurnAlloc` | `alloc_churn` | `memory:alloc_space` |
| Live heap / retention | `zooHoldHeap` | `live_heap` | `memory:inuse_space` |
| Goroutine leak | `zooLeakGoroutines` | `goroutine_leak` | `goroutine` |
| Lock contention | `zooContendMutex` | `mutex_contention` | `mutex:delay` |
| Blocking on a primitive | `zooBlockOnChannel` | `block_on_channel` | `block:delay` |

Everything else has no test, for two different reasons, and the difference
matters when you are deciding how much to trust a row:

- **Detectable, but nothing plants it.** Regressions between two windows,
  serialization cost, and self-hosted dependencies are ✅ and would work, but
  no test exercises them. A regression in those paths would not be caught.
- **Not detectable at all.** Every ⚠️ and ❌ row. There is nothing to assert:
  a test that "proved" parcareport finds cache misses would be proving a
  falsehood. What backs those rows is the explanation above them, not a test.

## Summary

| Class | Verdict |
|---|---|
| CPU-bound compute, hot functions | ✅ |
| Serialization / reflection / crypto cost | ✅ |
| Regressions between two windows (CPU only) | ✅ |
| Allocation churn / GC pressure | ✅ Go services, age caveat |
| Live heap, leaks | ✅ Go services, needs repeated runs |
| Lock contention, sync blocking, goroutine leaks | ✅ Go services, off by default |
| Self-hosted dependencies (DB, cache, proxy) | ✅ |
| Syscall overhead, context switches, page faults | ⚠️ kernel stacks, no rates |
| Parallelism limits, `GOMAXPROCS`, deadlock | ⚠️ inference |
| Where code waits on disk/network | ⚠️ targeted off-CPU only |
| Non-Go lock waits | ⚠️ coarse |
| Transient spikes, sub-second processes | ⚠️ window-dependent |
| Cache / bandwidth / NUMA / TLB / IPC stalls | ❌ PMU (`perf`) |
| CPU throttling, run-queue latency | ❌ cgroup/node metrics |
| Disk & network latency/throughput/queues | ❌ node metrics |
| Tail latency, per-request attribution | ❌ tracing |
| Cross-fleet dependency latency | ❌ tracing |
| OOM kill event, cold start, capacity | ❌ k8s metrics/events |

Parca covers the "where does my code spend time and memory" quadrant well. When
a run finds no CPU or memory bottleneck — **and reports itself complete** — the
honest conclusion is "not a code-locality problem in what was profiled", and
the next stop is usually metrics rather than a deeper profile.
