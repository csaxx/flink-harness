# flink-harness

Run Flink streaming jobs as standalone Java calls — no Flink runtime environment, no
MiniCluster, no test-utils. Just lightweight harnesses wrapping `ProcessFunction` /
`KeyedProcessFunction` / `RichMapFunction` / `RichFlatMapFunction` / `RichFilterFunction`.

## Why

Production Flink jobs with complex operator chains (fan-out, keyed state, side outputs,
metrics) only run inside a Flink cluster or MiniCluster. For offline testing, demos, and
consumers who want fast feedback without Flink's runtime machinery, flink-harness provides
a custom `RuntimeContext`, recording collectors, per-key in-memory state, and a
`WorkflowBuilder` / `StandaloneWorkflow` that runs the DAG synchronously in a single thread.

## Modules

| Module | Purpose |
|--------|---------|
| `flink-harness` | Shipped library — harnesses, workflow builder, runtime context, state store |
| `flink-test` | Demo Flink job (`DemoFunctions`) — parse, route, accumulate + report pipeline |
| `flink-standalone` | Integration runner — feeds CSV order lines through the demo workflow |

## Requirements

- Java 21+ (GraalVM 25 compatible)
- Flink 2.3.0 (all artifacts)
- Single compile dependency: `flink-streaming-java` (transitive runtime classes are never instantiated)

## Quick start

```java
import org.flink.harness.*;

StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
    .addSource("in")                                          // source node — entry point for inputs
    .registerFunction("parse", new MyParseFn())               // ProcessFunction, RichMap, etc.
    .registerKeyedFunction("accum", new MyKeyedFn())          // KeyedProcessFunction
    .addSink("out")                                           // terminal collector
    .addSourceEdge("in", "parse")                             // source → function
    .addEdge("parse", "accum")
    .addEdge("accum", "out")
    .build();

// Feed elements through the graph starting at the "in" source
WorkflowResult result = wf.process(List.of("line1", "line2"), "in");

result.functionResults()          // Map<nodeId, FunctionResult> for nodes with ≥1 output or metric
result.aggregatedMetrics()        // flat cross-node map: counters/meters/histograms summed, gauges last-wins
result.outputsOf("out")           // convenience: all elements collected by the sink
```

## Build a workflow

### Sources, functions, sinks

A workflow graph has three node kinds: **sources** (entry points), **functions** (transform logic),
and **sinks** (terminal collectors).

```java
new WorkflowBuilder(Mode.CONTINUOUS)
    // Sources — default passthrough or custom subclass
    .addSource("csv")                              // untyped passthrough
    .addSource("csv", TypeInformation.of(String.class))  // with type hint
    .addSource("events", new MyCustomSource())           // custom subclass

    // Functions — any Flink RichFunction subtype
    .registerFunction("parse", new MyParseFn())                    // ProcessFunction, RichMap, etc.
    .registerFunction("filter", new MyFilterFn(),                 // with explicit type hints
        TypeInformation.of(String.class), TypeInformation.of(Boolean.class))
    .registerKeyedFunction("accum", new MyKeyedFn())               // KeyedProcessFunction
    .registerKeyedFunction("accum", new MyKeyedFn(),
        TypeInformation.of(String.class), TypeInformation.of(MyOut.class))

    // Sinks — default collecting or custom subclass
    .addSink("out")                                    // default: records all elements
    .addSink("errors", new MyCustomSink())              // custom subclass
    .addSink("out", TypeInformation.of(MyType.class))   // with type hint
    .build();
```

### Edges: main channels, keyed channels, side outputs

```java
.addEdge("src", "dst")                              // main channel (unkeyed)
.addKeyedEdge("src", "dst", order -> order.customer) // keyed main channel
.addSideOutputEdge("src", "dst", MyFn.REJECTED_TAG)  // side output channel
.addKeyedSideOutputEdge("src", "dst", tag, keySel)    // keyed side output
.addSourceEdge("src", "dst")                          // sugar: source → node
.addSinkEdge("src", "dst")                            // sugar: node → sink
.addSinkEdge("src", "dst", tag)                       // sugar: side output → sink
```

### Modes

- **`CONTINUOUS`** — metrics and state accumulate across `process()` calls (like real Flink).
  Manually reset with `clearState(nodeId)` / `clearStateAll()` / `clearMetrics(nodeId)` /
  `clearMetricsAll()`.
- **`TRANSIENT`** — everything (state, metrics, counters) is cleared after each `process()` call,
  even on exception (via try/finally inside the lock).

### Eager initialization

By default functions open lazily on first element. Call `initializeAtBuild()` to open all
functions during `build()` for fail-fast `open()` errors and predictable startup:

```java
new WorkflowBuilder(mode).initializeAtBuild()
    .registerFunction(...)
    .build();
```

### Type safety

- `TypeInformation` hints can be provided at registration time (sources, functions, sinks).
- There is no `TypeExtractor` inference: an edge is "resolved" only when both endpoint
  types are given as hints. Unresolved edges cause a loud failure at `build()` unless
  you opt out:
  ```java
  .build(true)                                // opt out of type validation
  .build(optOutTypeValidation = true)         // same, named for clarity
  ```
- Opt-out edges fall back to per-element `ClassCastException` naming the edge and function ids.

## Run a workflow

```java
// Feed elements through the graph starting at a source node
WorkflowResult result = wf.process(List.of("a", "b", "c"), "sourceId");

// Results
result.functionResults()          // Map<nodeId, FunctionResult<Object>>
result.aggregatedMetrics()        // Map<metricName, Number>
result.outputsOf("sinkId")        // convenience: List<Object>
result.sideOutputsOf("fnId", tag) // convenience: List<Object>
```

### Workflow introspection

```java
wf.getNodeIds();       // Set<String> — all nodes (sources, functions, sinks)
wf.getSourceIds();     // Set<String>
wf.getSinkIds();       // Set<String>
wf.getNode("parse");   // original function/source/sink instance
wf.getWorkflow();      // List<WorkflowNode> — serializable DAG with type info and successors
```

### Thread safety

Workflows are thread-safe by default. A single `ReentrantLock` guards the entire `process()`
call and every `clear*`/`close()` operation. Multiple workflows run without contention;
concurrent calls on the same workflow serialize.

### Lifecycle

```java
wf.close();  // calls close() on all nodes (idempotent, guarded by lock)
```

## Supported features

| Feature | Status |
|---------|--------|
| ProcessFunction | ✔ |
| KeyedProcessFunction | ✔ |
| RichMapFunction / RichFlatMapFunction / RichFilterFunction | ✔ |
| Metrics (Counter, Gauge, Meter) via `RuntimeContext.getMetricGroup()` | ✔ |
| Side outputs (OutputTag) — routable to any node | ✔ |
| Keyed state v1 (ValueState, ListState, MapState, ReducingState, AggregatingState) | ✔ in-memory per-key, no serialization |
| StandaloneSource / StandaloneSink — subclassable, default passthrough | ✔ |
| Side-channel edges (OutputTag on `Edge`) — main/side routing via `sideTag == null` | ✔ |
| Timers / TimerService | ✔ three modes: OPPORTUNISTIC, MANUAL, BACKGROUND |
| `createSerializer` | ✔ via `SerializerConfigImpl` |
| `getGlobalJobParameters` | ✔ via `WorkflowBuilder.globalJobParameters(map)` |
| v2 state (`org.apache.flink.api.common.state.v2.*`) | ✔ in-memory per-key, eager `StateFuture`; TTL-enabled descriptors rejected |
| Accumulators, broadcast variables, distributed cache | ✗ `UnsupportedOperationException` |
| CoProcessFunction / connected streams | ✗ planned |
| BroadcastProcessFunction | ✗ planned |
| Parallelism > 1 | ✗ all operators run with parallelism-1 semantics |

## Missing features

Flink features that are **not** supported (no plan to add):

| Feature | Reason |
|---------|-------|
| Windows (assigners/triggers/evictors) | Can emulate with keyed state + timers |
| Async I/O | Needs runtime async executor machinery |
| FLIP-27 Source/Sink interfaces | `process()` is the source; `StandaloneSink` covers sinks |
| State TTL (`StateTtlConfig`) | Deferred; needs time infrastructure |
| Accumulators | Permanently out of scope — superseded by metrics |
| Broadcast variables / distributed cache | DataSet legacy |
| Feedback iterations | No termination guard |
| Parallelism > 1 | Frozen design decision — all operators run with parallelism-1 semantics |

Features **planned** for future versions:

| Feature | Notes |
|---------|-------|
| Event-time timers + watermarks | See FUTURE.md |
| CoProcessFunction / connected streams | See FUTURE.md |
| BroadcastProcessFunction / BroadcastState | See FUTURE.md |
| RichSinkFunction harness | See FUTURE.md |
| CheckpointedFunction / operator state | On demand only |

## Package overview

| Package | Audience |
|---------|----------|
| `org.flink.harness` | Consumer API — `WorkflowBuilder`, `StandaloneWorkflow`, `WorkflowNode`, `Mode`, `Edge` |
| `org.flink.harness.graph` | Implementation — `StandaloneRuntimeContext`, `RecordingCollector` |
| `org.flink.harness.graph.function` | Nodes — `NodeHarness` (interface), `FunctionHarness` + subtypes, `HarnessFactory` |
| `org.flink.harness.graph.result` | `FunctionResult`, `WorkflowResult` — output containers |
| `org.flink.harness.graph.source` | `StandaloneSource`, `JsonSource` — subclassable source base classes |
| `org.flink.harness.graph.sink` | `StandaloneSink`, `JsonSink` — subclassable sink base classes |
| `org.flink.harness.metrics` | `StandaloneMetricGroup`, `StandaloneOperatorMetricGroup` — not API |
| `org.flink.harness.state` | In-memory keyed state store (v1+v2), eager `StateFuture`/`StateIterator` — not API |
| `org.flink.harness.timer` | Timer service, timer heap, processing-timer mode — consumer API |

## Build

```bash
mvn -q verify                    # full build + tests
mvn -pl flink-harness -q verify  # library only
mvn dependency:tree              # check dependency cleanliness
```

A dedicated `DependencyTreeTest` in `flink-standalone` enforces that no banned artifacts
(`flink-test-utils`, `flink-clients`, `flink-runtime-test`) leak into the production scope.

## Architecture

```
Your Flink functions (ProcessFunction, KeyedProcessFunction, RichMap, etc.)
        │
        ▼
WorkflowBuilder — typed edges, key selectors, type validation
        │
        ▼
StandaloneWorkflow — BFS execution, locking, mode management
  ├── NodeHarness implementations
  │     ├── FunctionHarness        → wraps RichFunction, wires RuntimeContext + keyed state
  │     │     ├── ProcessFunctionHarness
  │     │     ├── KeyedProcessFunctionHarness
  │     │     └── RichFunctionHarness (Map/FlatMap/Filter)
  │     ├── StandaloneSource       → synthetic source node
  │     └── StandaloneSink         → synthetic terminal node
  ├── StandaloneRuntimeContext     → metrics group + InMemoryKeyedStateStore
  ├── RecordingCollector           → captures main + side outputs
  └── WorkflowResult               → outputs, side outputs, aggregated metrics
```