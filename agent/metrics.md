# Metrics

## Purpose

Documents the in-memory metric registry that backs `RuntimeContext.getMetricGroup()`,
how snapshots are flattened and aggregated into `WorkflowResult`, and the exact reset
rules. Metrics are the supported replacement for accumulators.

## Read this when

- registering or resetting metrics in a function or a custom source/sink
- changing `WorkflowResult.aggregatedMetrics` or per-function metric snapshots
- adding a metric type or a metric group feature
- debugging missing/duplicated/zero metric values

## Mental model

```
FunctionHarness (per node)                      StandaloneWorkflow.buildWorkflowResult
  └── StandaloneRuntimeContext(id)                ├─ per-node metricsSnapshot()  → FunctionResult.metrics
        └── StandaloneOperatorMetricGroup(id)     └─ aggregateAllMetrics()       → WorkflowResult.aggregatedMetrics
              ├── metrics: Map<String, Metric>          (all nodes, dotted names)
              └── children: nested groups                        │
                    └── getIOMetricGroup(): StandaloneIOMetricGroup (separate, not snapshotted)
```

Metric group paths are the **node id** (`StandaloneRuntimeContext(id)` →
`StandaloneOperatorMetricGroup(id)`). Custom sources/sinks use
`getClass().getSimpleName()` as their path.

Files:
`flink-harness/src/main/java/org/flink/harness/metrics/StandaloneMetricGroup.java`,
`StandaloneOperatorMetricGroup.java`.

## Registry behavior

`StandaloneMetricGroup` keeps a `LinkedHashMap<String, Metric>` and a
`LinkedHashMap<String, StandaloneMetricGroup>` of children.

| Method | Behavior |
|---|---|
| `counter(name)` | Returns the existing metric cast to `Counter`, or creates a `StandaloneCounter`. **If a non-Counter is registered under `name`, the cast throws `ClassCastException`.** |
| `counter(name, custom)` | Registers/replaces a caller-supplied `Counter` (not a `StandaloneCounter`). |
| `gauge(name, g)`, `histogram(name, h)`, `meter(name, m)` | Register/replace. |
| `addGroup(name)` | Child group with path `path + "." + name`. |
| `addGroup(key, value)` | Nested group `path.key.value`. |
| `getScopeComponents()` | `path.split("\\.")`. |
| `getMetricIdentifier(name)` / `(name, filter)` | `path + "." + name` (with optional `CharacterFilter` applied to the path). |
| `getAllVariables()` | Empty map. |
| `snapshot()` | Flattened `Map<String,Object>` of **values** with dotted names. |
| `metricInstances()` | Flattened `Map<String,Metric>` with dotted names, preserving metric kind. |
| `resetCounters()` | Recursively sets every `StandaloneCounter` to 0. |

Snapshot value extraction (`extract`): `Counter → getCount()`, `Gauge → getValue()`,
`Meter → getCount()`, `Histogram → getCount()`, otherwise `toString()`. Histograms
therefore surface as counts, not percentiles.

`StandaloneOperatorMetricGroup` adds `getIOMetricGroup()`, returning a separate
`StandaloneIOMetricGroup` (four `IoCounter`s: records in/out, bytes in/out). These
counters satisfy the interface but are **not** populated and are **not** part of the
parent group's `metrics`/`children`, so they never appear in snapshots or aggregation.

## Per-run snapshots

`NodeHarness.metricsSnapshot()` returns the flattened snapshot for the node; this is
embedded in each `FunctionResult.metrics`. `buildWorkflowResult` includes a node in
`functionResults` iff it produced sink outputs **or** its metric snapshot is non-empty.
Snapshot keys are group-relative dotted names (e.g. `errors.count`).

## Cross-node aggregation

`StandaloneWorkflow.aggregateAllMetrics()` iterates nodes in **registration order** and
merges by dotted metric name:

- `Gauge` → put `getValue()` (last node wins).
- `Counter` / `Meter` / `Histogram` → sum `getCount()` across nodes.

Consequences:

- Node identity is **not** part of the aggregate key; two nodes registering a counter
  named `total` sum into a single `total` entry.
- Gauges with the same name silently override one another based on node order.
- Histogram aggregation uses total counts, not distribution.

These are established by `aggregatedMetricsSumCountersAcrossFunctions` and
`aggregatedMetricsGaugeLastWins`.

## Reset semantics (important)

- `FunctionHarness.clearMetrics()` → `StandaloneOperatorMetricGroup.resetCounters()`,
  which zeroes only `StandaloneCounter` instances, recursively.
- **Custom counters registered via `counter(name, counter)` are not reset.**
- **Gauges, meters, and histograms are never reset.**
- **`StandaloneSource` / `StandaloneSink` do not override `clearMetrics`/`clearState`**
  (`NodeHarness` defaults are no-ops), so metrics and any custom state on those nodes
  are **never reset** — including by `Mode.TRANSIENT`. Only `FunctionHarness` nodes
  participate in reset.
- `clearMetricsAll()` calls `clearMetrics()` on every node; the same limitations apply.

## Invariants and contracts

- Metrics are per-node and identified by their dotted name within that node.
- `resetCounters` must remain safe to call repeatedly and recursively.
- Snapshots and instances must traverse the same groups in the same order.
- Aggregation is intentionally name-based and order-sensitive; changing either is a
  behavior change.
- The IO metric group must remain non-snapshotting unless deliberately integrated.
- Accumulators are not an alternative; see `runtime-context.md`.

## Important implementation patterns

- Register metrics in `open()` (functions) or `init()` (sources/sinks) and cache the
  handle; do not look them up per element.
- Use dotted child groups for namespacing; they flatten automatically.
- To make a metric resettable in TRANSIENT mode, use the built-in `counter(name)`
  factory, not a custom counter.
- Custom nodes that need resettable metrics must override `clearMetrics`/`clearState`.

## Deliberate differences from Flink

- No reporter backends, no metric registry lifecycle, no `MetricReporter` SPI.
- No scope variables (`getAllVariables()` is empty) and no `TaskManager`/`Job` scope
  hierarchy; scope is just the node path.
- `Histogram` is reduced to a count; no min/max/mean/stddev, no percentile implementation.
- IO metrics are placeholders and never updated (Flink fills these via the task's I/O
  counters).
- Metrics are returned as plain maps rather than scraped through a registry.

## Common pitfalls / agent traps

- **Expecting custom counters to reset.** Only `StandaloneCounter` resets.
- **Expecting gauges/meters/histograms to reset.** They never do.
- **Expecting source/sink metrics to reset in TRANSIENT.** They do not (no override).
- **Assuming metric names are namespaced per node in the aggregate.** They are not.
- **Relying on gauge ordering** without knowing node registration order.
- **Registering a gauge, then calling `counter(sameName)`** — `ClassCastException`.
- **Expecting IO counters in results.** They are not snapshotted.

## Testing and verification

- `flink-harness/.../metrics/MetricGroupTest` — counter accumulation/snapshot, nested
  group flattening (`errors.count`), recursive counter reset, gauge exposure.
- `flink-harness/.../functions/ProcessFunctionHarnessTest` — `metricsSnapshot` and
  `resetAll` (resets built-in counters).
- `flink-harness/.../StandaloneWorkflowTest` — cross-node counter sum and gauge
  last-wins; `customSourceMetricsAvailable` / `sinkMetricsAvailable` prove custom
  node metrics reach `FunctionResult.metrics`.
- `flink-standalone/.../StandaloneRunnerTest` — metrics across a realistic DAG and
  `clearMetrics` for a function node.

There is **no test** for custom-counter non-reset, source/sink non-reset in TRANSIENT,
IO group non-snapshotting, or gauge-name collisions. Add tests if you change these.

## Debugging / investigation map

| Symptom | First inspect |
|---|---|
| metric missing from result | registered in `open`/`init`? node opened? snapshot empty? |
| metric doesn't reset | custom counter / gauge / meter / histogram / source/sink node |
| `ClassCastException` in `counter(name)` | a non-Counter metric already registered under that name |
| aggregate value surprising | name collision across nodes; gauge last-wins order |
| IO counters zero | by design (placeholders) |
| per-node metric correct but aggregate wrong | `aggregateAllMetrics` sum/last-wins rules |

## Related agent references

- [workflow.md](./workflow.md) — result aggregation and mode-driven reset.
- [harnesses.md](./harnesses.md) — where functions/nodes expose metric groups.
- [runtime-context.md](./runtime-context.md) — `getMetricGroup()` and the accumulator policy.
- [testing.md](./testing.md) — test inventory and gaps.

## External references

- Flink 2.3 metrics (semantic baseline for metric types/scope):
  https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/metrics/
- Flink 2.3 docs, custom metrics via the metric group:
  https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/metrics/
