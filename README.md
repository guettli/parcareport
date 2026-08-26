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

FUNCTION                                                      CUM    FLAT   %TOTAL
github.com/example/app/internal/webui.(*Data).readIssues      0.941  0.000  40.6
encoding/json.Unmarshal                                       0.895  0.001  38.6
do_syscall_64                                                 0.817  0.030  35.3
```

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
same reason, a run where *every* query fails says so rather than reporting
"no data in this window", which would read as an idle cluster.

`--timeout` (default 60s) bounds each query so one slow group fails visibly
instead of stalling the run.

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
  (`--concurrency`). Wide windows over high-cardinality labels are work for the
  server; start narrow.
- Frames without debuginfo are bucketed as `[unsymbolized]` so they don't
  fragment the top-N into hex noise.

## License

Apache-2.0
