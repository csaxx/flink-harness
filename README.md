# flink-harness

Run Flink streaming jobs as standalone Java calls — no Flink runtime, no MiniCluster, no test-utils.

## What it does

Wraps your Flink functions (`ProcessFunction`, `KeyedProcessFunction`, `RichMapFunction`,
`RichFlatMapFunction`, `RichFilterFunction`) in lightweight harnesses that provide a minimal
`RuntimeContext` (metrics, side outputs, keyed state) without pulling in Flink's runtime machinery.

## Project structure

| Module | Purpose |
|--------|---------|
| `flink-harness` | Shipped library — standalone harnesses + workflow builder |
| `flink-test` | Demo Flink job (`DemoFunctions`) — parse → route → accumulate + report |
| `flink-standalone` | Integration example — runs the demo job standalone |

## Quick start

```java
import org.flink.harness.*;

StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
    .registerFunction("parse", new MyParseFn())
    .registerKeyedFunction("accum", new MyKeyedFn(), input -> input.key())
    .addEdge("parse", "accum")
    .activateOutput("accum")
    .activateSideOutput("parse", MyParseFn.REJECTED)
    .build();

WorkflowResult result = wf.process(List.of("line1", "line2"), "parse");

result.outputs()       // Map<functionId, List<output>>
result.sideOutputs()   // Map<SideOutputKey, List<output>>
result.metrics()       // Map<functionId, Map<metricName, value>>
```

### Modes

- **`CONTINUOUS`** — metrics and state accumulate across `process()` calls (like real Flink)
- **`TRANSIENT`** — everything cleared after each `process()` call (even on exception)

### Type safety

Register functions with explicit `TypeInformation` hints for compile-time-safe workflows:

```java
.registerFunction("parse", new MyParseFn(),
    TypeInformation.of(String.class), TypeInformation.of(MyType.class))
```

Unresolved generics fail loudly at `build()`; opt out with `build(true)` if needed.

### Thread safety

Workflows are thread-safe by default: the entire `process()` call (and `clear*`/`close()`) is guarded by a single workflow-level lock. Multiple workflows can run concurrently; concurrent calls on the same workflow serialize.

### Clearing state / metrics

```java
wf.clearState("accum");        // per-function
wf.clearStateAll();            // all functions
wf.clearMetrics("parse");      // per-function counters
wf.clearMetricsAll();          // all counters
```

### Workflow introspection

```java
wf.getFunctionIds();   // Set<String>
wf.getWorkflow();      // List<WorkflowNode> — serializable DAG for visualization
wf.getFunction("parse"); // original function instance
```

## Supported features

| Feature | Status |
|---------|--------|
| ProcessFunction / KeyedProcessFunction | ✔ |
| RichMap / RichFlatMap / RichFilter | ✔ |
| Metrics (Counter, Gauge, Meter) | ✔ |
| Side outputs (OutputTag) | ✔ |
| Keyed state v1 (Value/List/Map/Reducing/Aggregating) | ✔ in-memory per-key |
| Timers | ✗ deferred to v2 |
| Accumulators / broadcast / distributed cache | ✗ |
| v2 state | ✗ `UnsupportedOperationException` |

## Build

```bash
mvn -q verify                    # full build + tests
mvn -pl flink-harness -q verify  # library only
mvn dependency:tree              # check dependency cleanliness
```

## Requirements

- Java 21+ (GraalVM compatible)
- Flink 2.3.0

## Architecture

```
Your Flink job functions
        │
        ▼
WorkflowBuilder (typed edges, key selectors, activation points)
        │
        ▼
StandaloneWorkflow
  ├── FunctionHarness (per function: open/close, key bind, lock)
  │     ├── StandaloneRuntimeContext (metrics + keyed state)
  │     ├── RecordingCollector (main outputs)
  │     └── SideOutputCollector (tagged outputs)
  └── WorkflowResult (outputs, sideOutputs, metrics)
```