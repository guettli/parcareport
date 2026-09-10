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

FUNCTION                             CUM    FLAT*  %TOTAL
do_syscall_64                        0.817  0.330  14.2
encoding/json.(*decodeState).object  0.412  0.298  12.9
runtime.memmove                      0.221  0.221  9.5
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
WORKLOAD    BLOCKED   %TOTAL
clickhouse  1816.692  41.4
agentloop   232.447   5.3
```

Almost all of the first row is `ThreadPoolImpl::ThreadFromThreadPool::worker`
and almost all of the second is `runtime.mstart` — ClickHouse's pool workers
and Go's parked Ms. Both are idleness, not bottlenecks. A fleet-wide off-CPU
sweep therefore tends to rank services by how many idle threads they keep,
which is not interesting.

Off-CPU earns its keep **targeted**, not swept: profile one operation you
already believe is slow, and look for waits on its critical path. Judge the
stacks, never the totals.

```sh
parcareport types                          # what the server has
parcareport --profile-type=wallclock --by=cluster   # who is blocking
parcareport --profile-type=inuse_space --by=instance
```

### Naming a profile type

`--profile-type` takes either the full six-part selector or a unique substring
of one. The full form is long, and the order inside
`samples:count:cpu:nanoseconds` matters, so it is easy to get subtly wrong and
it had to be retyped for every invocation.

An ambiguous abbreviation is an error listing the candidates, never a guess —
`memory` matches four types on a typical server, and quietly picking
`inuse_space` over `alloc_space` would answer a different question than the one
asked.

With no `--profile-type` at all, the CPU delta profile is used. Auto-detect
used to require the server to offer exactly one type, which in practice never
happened: the server this was tested against offers eleven, so every single
run had to spell the selector out. This is a CPU report and `CORES` is its
headline unit, so CPU is the right default — and the type in use is echoed in
the report heading, so the choice is visible rather than hidden.

The default is matched on the *period* type being `cpu` and the profile being a
delta, not on the string "cpu" appearing somewhere. A wallclock profile also
mentions `samples` and `nanoseconds`, and off-CPU time is emphatically not what
`CORES` means.

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
!! INCOMPLETE
!! 3 of 47 workload queries failed. The totals and percentages above
!! EXCLUDE them and are therefore wrong.
!!   context deadline exceeded  (3 groups: workload=agentloop, workload=api, workload=web)
!! Raise --timeout, or narrow the window with --from so each merge is smaller.
```

Deliberately not stderr alone: `2>/dev/null` is common in scripts, and hiding
this is exactly how a broken run gets mistaken for a real measurement. For the
same reason, a run where *every* query fails says so on stdout rather than
reporting "no data in this window", which would read as an idle cluster:

```
!! FAILED: all 47 workload queries failed, so there is nothing to report.
!! This is not an empty window -- the queries did not come back.
```

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
!! Raise --timeout, or narrow the window with --from so each merge is smaller.
```

That last hint matters more than it looks. A stream reset carries no gRPC
boilerplate to strip and says nothing about what to do, yet it can take
minutes to arrive.

There are two clocks, and they have to be told apart. `--deadline` (default
10m) is the budget for the whole run; `--timeout` (default 60s) bounds one
query. Every query's clock derives from the run's, so:

- **`--timeout` at or above `--deadline` is refused.** It could never fire —
  the run's budget expires first, every outstanding query reports its own
  deadline at the same moment, and the advice to raise `--timeout` cannot
  work. `--deadline=0` removes the budget for a deliberately long run.
- **`--timeout` must be positive.** Zero is not "unbounded" here: it is a
  deadline that has already passed, so every query would fail before being
  sent.
- **When the run's budget is what expired**, the failure says so and points at
  `--deadline`, rather than blaming `--timeout` for a query that may have had
  milliseconds rather than its full allowance. One clock fired, not N.

It bounds each query so one slow group fails visibly instead of stalling the
run — including the label and profile-type lookups,
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

An abbreviation can only be resolved against the server's list, so if that
lookup fails the run stops there rather than sending the abbreviation as
though it were a selector. Parca rejects anything that is not the full
five- or six-part form, so doing otherwise produced a run that could only
fail — and failed with a complaint about selector syntax, pointing nowhere
near the lookup that had actually gone wrong.

An explicit *full* `--profile-type` is checked against the server's list, but
the check can no longer fail the run on its own. The selector is already complete
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

## `--output=json`, for scripts and agents

The table is written for a person: columns padded to a width, function names
cut to 60 characters with an ellipsis, shortfalls as prose beginning `!!`. None
of that survives being parsed — and the truncation is lossy enough that two
different frames once rendered identically, so the output could not be mapped
back to a symbol at all.

`--output=json` emits one document with the full names, the raw numbers, and
the failures as data:

```json
{
  "profile_type": "parca_agent:samples:count:cpu:nanoseconds:delta",
  "profile_type_verified": true,
  "start": "2026-09-09T14:43:26Z",
  "end": "2026-09-09T14:58:26Z",
  "window_seconds": 900,
  "group_by": "cluster",
  "unit": "cores",
  "rate": true,
  "groups": [
    {"name": "tc", "value": 2.3160163, "pct": 77.95409962975428},
    {"name": "vps", "value": 0.6549837, "pct": 22.045900370245704}
  ],
  "empty_groups": 0,
  "total": 2.971,
  "functions": [
    {"name": "github.com/parquet-go/parquet-go/encoding/thrift.(*structDecoder).decode",
     "cum": 0.8312841, "flat": 0.1203611, "pct": 4.05119824974756}
  ],
  "functions_sorted_by": "flat",
  "failed": [],
  "complete": true
}
```

Two fields matter more than the rest. **`complete`** is the machine-checkable
form of the `!! INCOMPLETE` banner, and **`failed`** says exactly which groups
are missing and why. Without them a consumer would have to grep stdout for
`!!` to notice the totals were wrong, which is the same trap the banner exists
to avoid for human readers.

**`total` is `null`** when the unfiltered merge failed or came back empty, and
every `pct` is `null` with it. The sum of the listed groups is not the total —
it omits every series carrying no group-by label — so there is no honest number
to put there.

`unit` is a stable machine name (`cores`, `blocked_threads`, `bytes`, `count`)
rather than the column heading, which is free to be reworded. `rate` says
whether the value was divided by the window; bytes and counts are not rates.

A run that produces nothing still emits a document, with `complete: false` and
`error` set. Printing only prose in that case would leave a script unable to
tell an empty window from a broken command.

## Install

```sh
go install github.com/guettli/parcareport@latest
```

## Usage

```
parcareport [report] [flags]   break CPU down by a label, then list hot functions
parcareport overview [flags]   what this server has, and what is busy in it
parcareport labels [name]      summarize labels, or list one label's values
parcareport types [flags]      list profile types the server offers
```

## `parcareport overview`

Getting oriented otherwise meant running the tool once per question, and each
run needed a profile type and a `--by` label chosen in advance — so you had to
know the answers before you could ask. `overview` asks the server what it has
and reports on that:

```console
$ parcareport overview --from=-15m

2026-09-09T14:43:26Z .. 2026-09-09T14:58:26Z  (15m0s)
11 profile types, 3 labels: cluster comm node

parca_agent:samples:count:cpu:nanoseconds:delta  ...

CLUSTER  CORES  %TOTAL
tc       2.316  78.0
vps      0.655  22.0
TOTAL    2.971  100.0

FUNCTION                                                      CUM    FLAT*  %TOTAL
github.com/parquet-go/parquet-go/encoding/thrift.(*structDe…  0.831  0.120  4.0

parca_agent:samples:count:cpu:nanoseconds:delta  ...

COMM   CORES  %TOTAL
parca  2.284  76.9
...
```

It breaks CPU down by whichever of `cluster`, `namespace`, `workload` and
`comm` exist in the window, and reports live heap by `instance` or `job` if the
server has a heap profile. Which of those exist depends on how the agents were
configured, and the label list is one cheap query — cheaper than making you
know in advance.

Heap is grouped only by `instance` or `job`, never by `cluster` — those are the
labels heap series actually carry, for the reason given under [Naming a profile
type](#naming-a-profile-type). Pairing the heap with `cluster` merged once per cluster,
found nothing, and reported "no data" for a heap profile with plenty in it.

**It is not cheap overall.** A breakdown costs one merge per label value, and
merges are the slow part — a single 15-minute breakdown over two clusters took
about two minutes against a real server. So a label with more than
`--max-group-values` (default 50) values is skipped rather than run:

```
-- not reported: CPU by comm (203 values is more than --max-group-values=50, and each one costs a merge; run `parcareport --by=comm` directly if you want it)
```

`--concurrency` bounds the queries within one section, not across sections;
sections run one after another. Start with a narrow `--from`.

**A busy Parca refuses wide queries rather than answering them slowly.** Every
merge materialises a profile in the server's memory, so several at once is
several profiles at once, on a process that is also ingesting. When it runs
out, the queries come back as `RST_STREAM with error code: INTERNAL_ERROR` or
`error reading from server`. Those are the connection going away mid-answer,
not a complaint about the query — the same query on its own succeeds.

Two things follow:

- `overview` runs **2 queries at once**, not the `--concurrency` default of 4,
  because it issues more queries than any other command and so meets the wall
  first. An explicit `--concurrency` is always honoured; if your server copes,
  say so.
- A query that failed that way is **asked once more** — and only that query,
  one at a time. Re-running the whole breakdown would send the server the same
  load that just defeated it. A query the server *rejected*, rather than
  dropped, is not retried: that would just repeat a wrong query. The budget is
  checked again before each retry, so recovering a few groups cannot eat the
  `--deadline` and leave every later section failing. Label lookups get the
  same single retry — losing one of those costs a whole breakdown rather than
  one group.

  This part is not specific to `overview`: a plain `parcareport` run retries
  its own dropped queries the same way.

When it happens you are told, because a run that quietly takes twice as long
is worth knowing about:

```
(2 namespace queries were asked again: the server dropped the first attempt)
```

If queries still fail, lower `--concurrency` to 1 and narrow `--from`. Raising
the server's memory limit does not make this go away — it only moves the point
at which it starts, and the failure is refusal, not a crash.

The hot functions come from the unfiltered merge, so every breakdown of one
profile type would produce the same table. It is shown once per type.

**A section it could not run is named, not dropped.** Otherwise there is no way
to tell "this server has no heap profile" from "the heap query failed":

```
-- not reported: live heap (no instance or job label to group by; heap profiles come from scrape targets, which carry those)
```

A section that fails part-way still prints, carrying its own `!! INCOMPLETE`
banner, and the command exits non-zero. One breakdown failing is not a reason
to discard the others — that is the point of running several.

`--output=json` gives the whole thing as one document, with each section
carrying its own `unit`, `total`, `functions` and `failed`. The function table
is deduplicated only in the table output, where a repeat would be
byte-identical; in JSON every section keeps its own, since an empty array
would read as "nothing was hot".

`--by` and `--profile-type` are refused rather than ignored: `overview` picks
both per section, so accepting them would silently do something else.

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `--url` | `localhost:7070` | Parca server gRPC address |
| `--from` | `-1h` | window start: RFC3339, or relative (`-6h`, `-30m`) |
| `--to` | `now` | window end |
| `--by` | `cluster` | label to break the report down by |
| `--match` | | extra matchers, e.g. `comm="clickhouse"` |
| `--profile-type` | the CPU profile | full selector, or a unique substring like `cpu` |
| `--top` | `15` | functions to list; `0` disables the table |
| `--output` | `table` | `json` for a machine-readable report |
| `--max-group-values` | `50` | overview: skip a breakdown with more values than this; `0` disables the skip |
| `--sort` | `flat` | order functions by `flat` (self time) or `cum`; `self` and `cumulative` also work |
| `--concurrency` | `4` | parallel queries, within one breakdown (`overview` uses 2 unless you set it) |
| `--timeout` | `60s` | per-query deadline |
| `--deadline` | `10m` | budget for the whole run; `0` removes it |
| `--insecure` | `true` | plaintext connection; `false` uses TLS |
| `--bearer-token-file` | | read an auth token from a file (needs `--insecure=false`) |
| `--username` / `--password-file` | | basic auth (needs `--insecure=false`) |

## Breaking down by other labels

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
still burning CPU, and the table would quietly fail to add up.

The row needs to be worth more than 0.1% of the total to appear. Below that a
residual is usually rounding between the per-group merges and the unfiltered
one rather than a real unlabeled series.

When grouping by `namespace` or `workload` a large `(unlabeled)` row is
expected and correct — it is every process outside a Kubernetes pod (kernel
threads, the kubelet, anything on the host). When grouping by `cluster` it
usually means an agent is missing its external label:

```yaml
# parca-agent DaemonSet
args:
  - --metadata-external-labels=cluster=tc
```

## Reaching a Parca that is not on localhost

`--insecure` defaults to true, which is right for a port-forward — the way
most people first try the tool:

```sh
kubectl -n monitoring port-forward svc/parca 7070:7070
parcareport --from=-6h
```

A Parca that is only reachable through an ingress needs `--insecure=false`,
which uses TLS with the system root certificates:

```sh
parcareport --url parca.example.com:443 --insecure=false \
            --bearer-token-file ~/.parca-token
```

Credentials go in an `Authorization` header. They are attached as gRPC
per-RPC credentials rather than by an interceptor, so gRPC itself enforces
that they never travel over a plaintext connection: combining them with
`--insecure` is refused rather than quietly leaking the token.

Basic auth works the same way:

```sh
parcareport --url parca.example.com:443 --insecure=false \
            --username svc --password-file ~/.parca-password
```

| Flag | |
|---|---|
| `--bearer-token-file` | read a bearer token from a file |
| `--bearer-token` | bearer token as an argument |
| `--username` + `--password-file` | basic auth, password from a file |
| `--username` + `--password` | basic auth, password as an argument |

**Prefer the file forms.** A secret passed as a flag is visible in the process
list to anyone on the box, and it lands in shell history. Where both are given
the file wins.

A secret file's contents are trimmed, because such files almost always end in
a newline and a credential carrying one fails as an opaque 401. Whitespace
*inside* the value is rejected instead of sent: it means the file holds
something other than a single credential — two lines, or a comment — and an
`Authorization` header containing a newline is refused far away from the flag
that caused it.

Two more combinations are refused rather than half-honoured: a bearer token
together with basic auth, and a `--username` containing a colon (RFC 7617
gives the colon to the first separator, so `a:b` with password `pw` would
authenticate as `a` with password `b:pw`).

`--password` without `--username` is refused rather than ignored. On its own
it produces no header at all, so the request would go out unauthenticated and
come back as a bare 401 that says nothing about the flag having been dropped.

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

  Run `parcareport labels <name>` to list one label's values in full.
  ```

  The advice differs from the one a failed *merge* gets. A merge can be made
  smaller — a narrower window, fewer series — while a label query already asks
  for almost nothing, so telling someone to narrow `--from` or add `--match`
  would suggest an action that does not apply.
- Frames without debuginfo are bucketed as `[unsymbolized]` so they don't
  fragment the top-N into hex noise.

## License

Apache-2.0
