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
parcareport --by=comm --from=-24h --top=0          # which processes burn CPU
parcareport --by=node --match='comm="clickhouse"'  # where that process runs
parcareport labels comm                            # what values exist
```

### The `(unlabeled)` row

If some series lack the `--by` label entirely, their CPU appears as
`(unlabeled)` rather than being dropped. This is deliberate: a single agent
deployed without the label would otherwise vanish from the breakdown while
still burning CPU, and the table would quietly fail to add up. A large
`(unlabeled)` row usually means an agent is missing its external label:

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
