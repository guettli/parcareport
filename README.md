# parcareport

A CLI that turns a [Parca](https://www.parca.dev/) server into a cross-cluster CPU
bottleneck report.

Parca's web UI is excellent for exploring one flamegraph. It is less good at
answering *"where did my CPU actually go last night, across everything I run?"*
— especially when a single Parca server collects from agents in several
clusters of different sizes. `parcareport` answers that on the command line.

```console
$ parcareport --url parca.example:7070 --from=-6h

parca_agent:samples:count:cpu:nanoseconds:delta  2026-08-26T03:00:00Z .. 2026-08-26T09:00:00Z  (6h0m0s)

CLUSTER  CORES  %TOTAL
vps      1.290  55.7
tc       0.697  30.1
p16      0.330  14.2
TOTAL    2.317  100.0

FUNCTION                                                      CUM    FLAT*  %TOTAL
do_syscall_64                                                 0.817  0.330  14.2
encoding/json.(*decodeState).object                           0.412  0.298  12.9
runtime.memmove                                               0.221  0.221   9.5
```

## Self time, not cumulative

The function table is ordered by **self time** (`FLAT`) — the code that was
actually on-CPU when the sampler looked. The sorted column is marked `*`, and
`%TOTAL` follows it.

This is not a cosmetic default. Ordering by cumulative value puts
`runtime.goexit` on top of every Go profile:

```
FUNCTION                                      CUM*   FLAT   %TOTAL
runtime.goexit                                2.284  0.000  76.9
golang.org/x/sync/errgroup.(*Group).Go.func1  1.563  0.000  52.6
```

"76.9% of the CPU was spent inside a goroutine" is true of almost every Go
program. Worse, the frames that *do* burn CPU are pushed below the `--top`
cutoff and never printed at all. That run totalled 2.971 cores, and the
largest self time anywhere in its top 15 was 0.120 — so sorted by self time
the same profile reads:

```
FUNCTION                                                      CUM    FLAT*  %TOTAL
github.com/parquet-go/parquet-go/encoding/thrift.(*structDe…  0.831  0.120  4.0
github.com/parquet-go/parquet-go/encoding/thrift.readStruct   0.867  0.031  1.0
github.com/parquet-go/parquet-go/encoding/thrift.decodeFunc…  0.783  0.028  0.9
```

Self-time percentages are small and spread out, because self time sums to the
profile's total across *all* functions rather than being counted once per
frame in every stack. Small numbers spread thin is the honest shape of this
workload; a single frame at 76.9% was not.

`--sort=cum` restores the cumulative order when that is the question: it shows
what larger piece of work a frame was part of, which is what you want once you
already know which function is hot.

## CORES, and why not percentages of a flamegraph

`CORES` is **average cores busy over the window**: CPU-seconds ÷ wall-seconds.
`0.5` means half a core was busy on average for the whole window.

This is the point of the tool. Raw sample counts are not comparable between a
big cluster and a small one, and a flamegraph's percentages are relative to
whatever you happened to select. Cores are an absolute, physical unit, so
"tc burns 0.7 cores" means the same thing everywhere and can be compared to
what you are paying for.

Getting there takes one non-obvious step. `parca-agent`'s CPU profile has
sample type `samples:count` with period type `cpu:nanoseconds` — the sample
value is a **count of stack samples, not a duration**. Multiplying by the
sampling period converts it to CPU time. Skip that and every number in the
report is silently off by a constant factor.

## Beyond CPU

A CPU sampler cannot see a thread that is *blocked*, and knows nothing about the
heap. If your Parca has more than CPU in it, `parcareport` reads those too and
reports each in its own unit — dividing bytes by wall-time would be nonsense:

| profile | column | means |
|---|---|---|
| `…:cpu:nanoseconds:delta` | `CORES` | average cores busy |
| `…:wallclock:nanoseconds:…` | `BLOCKED` | average threads waiting (off-CPU) |
| `memory:inuse_space:…` | `BYTES` | live heap |
| `goroutine:…` / `mutex:contentions:…` | `COUNT` | totals |

### Reading `BLOCKED` (off-CPU) honestly

Off-CPU totals are dominated by threads that are **idle, not stuck**. A thread
pool parked waiting for work is "blocked" in exactly the same way as a thread
stuck behind a lock, and there are usually far more of the former:

```
WORKLOAD     BLOCKED   %TOTAL      FUNCTION
clickhouse   1816.692    41.4      ThreadPoolImpl::ThreadFromThreadPool::worker  100%
agentloop     232.447     5.3      runtime.mstart                                 86%
```

Both of those are idleness — ClickHouse's pool workers and Go's parked Ms — not
bottlenecks. A fleet-wide off-CPU sweep therefore tends to rank services by how
many idle threads they keep, which is not interesting.

Off-CPU earns its keep **targeted**, not swept: profile one operation you
already believe is slow, and look for waits on its critical path. Judge the
stacks, never the totals.

```sh
parcareport types                                        # what the server has
parcareport --profile-type='...wallclock...' --by=cluster  # who is blocking
parcareport --profile-type='memory:inuse_space:bytes:space:bytes' --by=instance
```

Off-CPU needs `--off-cpu-threshold` on the agents (per-mille, `0` = off; note
the dashes — `--offcpu-threshold` is rejected). Heap, goroutine and mutex
profiles come from `scrape_configs` against Go `/debug/pprof` endpoints; those
series carry `job`/`instance` labels rather than the agent's
`cluster`/`comm`, so group them with `--by=instance`.

When a profile type only covers some series, group values with no samples are
counted and omitted rather than printed as a wall of zero rows.

### Partial results are never presented as complete

A breakdown runs one query per label value, and any of them can fail — a slow
merge over a wide window, a restarting server. If that happens, the totals and
percentages would silently exclude whatever failed, and the table would still
look complete.

So failures are printed **on stdout**, next to the numbers they invalidate,
and the command exits non-zero:

```
!! INCOMPLETE: 3 of 47 workload queries failed. The totals and
!! percentages above EXCLUDE them and are therefore wrong.
!!   workload=agentloop: context deadline exceeded
```

Deliberately not stderr alone: `2>/dev/null` is common in scripts, and hiding
this is exactly how a broken run gets mistaken for a real measurement. For the
same reason, a run where *every* query fails gets the same banner rather than
reporting "no data in this window", which would read as an idle cluster.

The two mix, and the counts have to keep them apart. Some queries can fail
while every survivor comes back genuinely empty, so the banner reports both
numbers rather than calling that "all queries failed" — with `--by=comm` one
flaky query would otherwise turn "199 values had no samples, 1 failed" into
"all 1 comm queries failed".

Failures are grouped by cause, so a wholesale outage collapses to one line and
a rare cause is never the one truncated away:

```
!!   stream terminated by RST_STREAM  (7 groups: instance=a, instance=b, instance=c, and 4 more)
!!   instance=z: context deadline exceeded
!! The server closed the stream mid-merge, which usually means the merge hit a
!! server limit or the server errored on it. Try a narrower --from window, or
!! fewer series with --match.
```

That last hint matters more than it looks. A stream reset carries no gRPC
boilerplate to strip and says nothing about what to do, yet it can take
minutes to arrive.

`--timeout` (default 60s) bounds each query so one slow group fails visibly
instead of stalling the run. That includes the label and profile-type lookups,
and the unfiltered merge behind the `(unlabeled)` row — which, carrying no
matcher at all, is the widest query in the run and was the one query with no
bound of its own.

If that unfiltered merge is the thing that fails, the group breakdown is
already computed and is still printed. What goes away is the total: there is no
denominator, so the `%TOTAL` column is dropped rather than filled with
percentages of a subtotal that silently omits whatever is missing.

```
CLUSTER        CORES
tc             2.316
vps            0.655
SUM OF LISTED  2.971

!! INCOMPLETE
!! The unfiltered merge failed, so percentages, the (unlabeled) row
!! and the hot-function table are missing, and any series carrying no
!! "cluster" label is absent from the sum above.
!!   (overall): context deadline exceeded
!! Raise --timeout, or narrow the window with --from so each merge is smaller.
```

However many things go wrong, there is one `!! INCOMPLETE` block. Two banners,
each describing the run as though it were the only problem, read as two
unrelated reports — and the group-failure wording talked about percentages
that the other banner had just explained were absent.

An explicit `--profile-type` is checked against the server's list, but the
check can no longer fail the run on its own. The selector is already complete
and the merges do not need the lookup; a slow server used to kill
fully-specified runs here.

When that check could not run and the report then comes back empty, the report
says so on stdout, because those two facts together are almost certainly one
fact: a selector the server does not offer looks exactly like an idle window.

An **empty** answer gets the same scepticism as a failed one. Parca reports
"no values" for a label that does not exist, for a window that holds nothing,
and for a query that simply came back short — the same response in all three
cases. Rather than assert the most convenient reading, the tool cross-checks
against the label names in the same window and names the reason:

```
no label "clustr" in 2026-08-27T06:00:00Z .. 2026-08-27T07:00:00Z; the server has: cluster, comm, node
the server has no labels at all in <window>: nothing was written in this window
label "cluster" exists in <window> but returned no values, which is contradictory:
  the values query most likely failed rather than found nothing. Retry it
```

The third one is real: a retry once produced values for the very label the
previous run had just declared empty.

### Measuring before and after a change

Use the **CPU** profile. It is a delta, so a merge over a window is a genuine
rate. Memory profiles are **cumulative since process start** (`delta=false`),
so comparing `alloc_space` between two processes of different ages measures
their ages, not their allocation rates — which will make a change look
dramatically better or worse than it was.

Sanity-check absolute numbers against `kubectl top` at least once.

## Install

```sh
go install github.com/guettli/parcareport@latest
```

## Usage

```
parcareport [report] [flags]   break CPU down by a label, then list hot functions
parcareport labels [name]      summarize labels, or list one label's values
parcareport types [flags]      list profile types the server offers
```

| Flag | Default | Meaning |
|---|---|---|
| `--url` | `localhost:7070` | Parca server gRPC address |
| `--from` | `-1h` | window start: RFC3339, or relative (`-6h`, `-30m`) |
| `--to` | `now` | window end |
| `--by` | `cluster` | label to break the report down by |
| `--match` | | extra matchers, e.g. `comm="clickhouse"` |
| `--profile-type` | auto | required only if the server offers more than one |
| `--top` | `15` | functions to list; `0` disables the table |
| `--sort` | `flat` | order functions by `flat` (self time) or `cum` |
| `--insecure` | `true` | plaintext connection |

Break down by anything the agents label:

```sh
parcareport --by=namespace --from=-24h --top=0        # which namespace burns CPU
parcareport --by=workload  --from=-24h                # which Deployment/DaemonSet
parcareport --by=comm      --from=-24h --top=0        # which processes
parcareport --by=node --match='workload="clickhouse"' # where that workload runs
parcareport labels workload                           # what values exist
```

`namespace`, `container`, `workload` and `workload_kind` only exist if the
agents are given `relabel_configs` via `--config-path` — parca-agent discovers
Kubernetes metadata but exposes it as `__meta_*` labels, which relabelling drops
unless you map them:

```yaml
relabel_configs:
  - source_labels: [__meta_kubernetes_namespace]
    target_label: namespace
  - source_labels: [__meta_kubernetes_pod_container_name]
    target_label: container
  - source_labels: [__meta_kubernetes_pod_controller_name]
    target_label: workload
  # A Deployment's pods are owned by a ReplicaSet, so the controller name
  # carries a hash that changes every redeploy. Strip it back.
  - source_labels: [__meta_kubernetes_pod_controller_kind, __meta_kubernetes_pod_controller_name]
    regex: ReplicaSet;(.+)-[^-]+
    target_label: workload
    replacement: ${1}
```

There is no `pod` label to map: parca-agent v0.38.0 does not emit
`__meta_kubernetes_pod_name` (upstream's own `kubernetes-config.yaml` example
is stale on this point). Group by `workload` instead — lower cardinality, and
stable across pod restarts.

### The `(unlabeled)` row

If some series lack the `--by` label entirely, their CPU appears as
`(unlabeled)` rather than being dropped. This is deliberate: a single agent
deployed without the label would otherwise vanish from the breakdown while
still burning CPU, and the table would quietly fail to add up. When grouping by `namespace` or `workload` a large `(unlabeled)` row is
expected and correct — it is every process outside a Kubernetes pod (kernel
threads, the kubelet, anything on the host). When grouping by `cluster` it
usually means an agent is missing its external label:

```yaml
# parca-agent DaemonSet
args:
  - --metadata-external-labels=cluster=tc
```

## Notes

- The tool speaks **gRPC**. Parca multiplexes gRPC and its web UI on one port
  and routes by `Content-Type`, so a plain JSON POST returns the UI's HTML with
  status 200 instead of an error — a confusing thing to debug from `curl`.
- Breakdowns run one merged query per label value, in parallel
  (`--concurrency`). `parcareport labels` fans out the same way — it runs one
  query per label name, which sequentially made the first command anyone
  reaches for the slowest thing in the tool. Wide windows over
  high-cardinality labels are work for the server; start narrow.
- While a fan-out runs, progress goes to **stderr**, and only when stderr is a
  terminal:

  ```
  merging 12 cluster groups...  7/12
  querying 24 labels... 18/24
  ```

  The counter is padded to the width of the total, so every line is exactly as
  wide as the last. `\r` does not erase, so a shorter line would otherwise
  leave the tail of a longer one behind and `7/12` would render as `7/120`.

  Stderr so a redirected report is unaffected, and terminal-only because the
  carriage returns that keep it to one line are noise in a log. Without it, a
  run that takes minutes looked exactly like one that had hung.
- A label whose values query fails no longer aborts the whole summary. Its row
  is marked `!!` and the rest is printed, because a partial summary that says
  what is missing beats no summary:

  ```
  LABEL    VALUES  SAMPLE
  cluster  2       tc vps
  comm     ?       !! context deadline exceeded
  node     3       n1 n2 n3
  !! These queries are normally instant, so a timeout means the server is slow
  !! or unreachable rather than the window being too large. Retry, or raise
  !! --timeout.
  ```

  The advice differs from the one a failed *merge* gets. A merge can be made
  smaller — a narrower window, fewer series — while a label query already asks
  for almost nothing, so telling someone to narrow `--from` or add `--match`
  would suggest an action that does not apply.
- Frames without debuginfo are bucketed as `[unsymbolized]` so they don't
  fragment the top-N into hex noise.

## License

Apache-2.0
