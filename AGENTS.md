# flink-harness

Run Flink streaming jobs as standalone Java calls — no Flink runtime environment, no
MiniCluster, no test-utils. Just lightweight harnesses wrapping ProcessFunction /
KeyedProcessFunction / RichMap/RichFlatMap/RichFilter.

## Motivations

Production Flink jobs with complex operator chains (fan-out, keyed state, side outputs,
metrics) only run inside a Flink cluster or MiniCluster. For offline testing, demos, and
consumers who don't want Flink's runtime machinery, we provide:

- **flink-harness** (the shipped library) — custom `RuntimeContext`, recording collectors,
  per-key in-memory state, per-function harnesses, `WorkflowBuilder` / `StandaloneWorkflow`
- **flink-test** — a demonstrative multi-stage Flink job covering all relevant features
- **flink-standalone** — integration example feeding inputs, returning outputs/side
  outputs/metrics

## Module layout

```
flink-harness-parent (pom; aggregator, Java 21, Flink 2.3.0)
├── flink-harness    → shipped library (no test deps)
├── flink-test       → demo Flink job (test deps only in test scope)
└── flink-standalone → standalone runner of flink-test's workflow
```

## Version pins

- Java 21 (GraalVM 25)
- Flink 2.3.0 (all artifacts)
- JUnit 5.11.4, AssertJ 3.27.3
- Jackson 2.18.3 (jackson-databind, provided scope for JsonSource/JsonSink)

## Frozen design decisions

If you change any of these, update this section AND re-evaluate all code.

### Scope (v1)

| Capability | Status |
|---|---|
| Metrics (Counter, Gauge, Meter) | ✔ implement |
| Side outputs (OutputTag) | ✔ implement (routable as side-channel edges to any node) |
| v1 state (Value/List/Map/Reducing/Aggregating) via `RuntimeContext.get*State(v1)` | ✔ in-memory per-key maps, no serialization |
| v2 state (`org.apache.flink.api.common.state.v2.*`) | ✗ `UnsupportedOperationException` |
| Timers — processing-time (keyed only) | ✔ three modes: OPPORTUNISTIC, MANUAL, BACKGROUND; per-(key, ts) dedup; event-time: UOE (future work) |
| `createSerializer` | ✔ via `TypeInformation.createSerializer(new SerializerConfigImpl())` |
| `getGlobalJobParameters` | ✔ via `WorkflowBuilder.globalJobParameters(map)` |
| Accumulators | ✗ permanently out of scope — use metrics instead |
| Broadcast variables, distributed cache | ✗ `UnsupportedOperationException` |
| `getUserCodeClassLoader` | ✗ `UnsupportedOperationException` |
| StandaloneSource / StandaloneSink | ✔ concrete, subclassable, default passthrough |
| JsonSource / JsonSink | ✔ convenience, Jackson-databind (provided scope) |
| Side-channel edges (OutputTag on `Edge`) | ✔ main/side output routing via `sideTag == null` |

### Harness approach (2026-09-03)

- **`NodeHarness`** interface consumed by `StandaloneWorkflow`. Implemented by:
  - `FunctionHarness` (abstract) — wraps Flink `RichFunction`, wires `RuntimeContext`, keyed state.
  - `StandaloneSource` / `StandaloneSink` — synthetic nodes with no-op lifecycle and own metric group.
- Subtypes of `FunctionHarness`: `ProcessFunctionHarness`, `KeyedProcessFunctionHarness`, `RichFunctionHarness`
- `Context` and `OnTimerContext` are **non-static inner classes** — instantiated through the wrapped function instance (same tecnique as Flink operator internals).
- `open(OpenContext)` called once (OpenContext is empty interface — pass singleton).
- `close()` called when the workflow is torn down.
- **Type-safe public surface, raw types inside** — raw/unchecked `@SuppressWarnings`
  confined to six helpers:
  1. Collector adapter (main + side output routing)
  2. KeySelector invocation (`apply(I)` cast)
  3. OutputTag lookup by tag-id (side-channel edge)
  4. Current-key binding
  5. Source passthrough cast (`StandaloneSource.process` default)
  6. OnTimerContext instantiation (anonymous inner class through `function.new OnTimerContext()` — same technique as Context)

### Type safety

- `TypeInformation` hints at registration + `TypeExtractor.getBaseTypes()` inference.
- Unresolved generics → **fail loudly at `build()`** unless opt out (`build(optOutTypeValidation=true)`).
- Opt-out edges fall back to per-element `ClassCastException` naming the edge and function ids.
- `getWorkflow()` returns DAG tuples annotated with resolved `TypeInformation` and `WorkflowNode.Kind` (SOURCE/FUNCTION/INK).

### WorkflowBuilder surface (API, 2026-09-03, updated 2026-09-07 with timers and fillers)

```java
new WorkflowBuilder(mode)
  .initializeAtBuild()                                   // optional: open() all functions at build()
  .clock(Clock)                                          // optional: pluggable clock (default SystemClock)
  .globalJobParameters(Map.of("k", "v"))                 // optional: job-level params (immutable, default empty)
  .setProcessingTimerMode(OPPORTUNISTIC)                 // OPPORTUNISTIC | MANUAL | BACKGROUND
  .setProcessingTimerMode(BACKGROUND, listener)          // with callback listener for results/errors
  .addSource("sourceId")                                 // default passthrough source
  .addSource("id", inType, outType)                      // with type hints
  .addSource("id", customSource)                          // custom subclass
  .registerFunction("id", functionInstance)               // ProcessFunction, RichMap, etc.
  .registerKeyedFunction("id", keyedFunctionInstance)
  .addSink("sinkId")                                     // default collecting sink
  .addSink("id", customink)                             // custom subclass
  .addSourceEdge("srcId", "dstId")                      // source → node
  .addEdge("srcId", "dstId")                            // main channel edge
  .addKeyedEdge("srcId", "dstId", keySelector)          // keyed main channel
  .addSideOutputEdge("srcId","dstId", tag)              // side channel edge (general)
  .addSinkEdge("srcId", "dstId"[, tag])                  // sink terminal edge (sugar)
  .build()
```

`process(inputs, sourceId)` returns `WorkflowResult(functionResults, aggregatedMetrics)` — functionResults is a `Map<nodeId, FunctionResult<Object>>` for nodes that produced outputs (sinks) or metrics; `aggregatedMetrics` is a flat cross-node map where counters/meters/histograms are sumed and gauges last-wins.

Modes: `CONTINUOUS` (metrics & state accumulate like real Flink; manual `clearState(id)` / `clearStateAll()` / `clearMetrics()`) and `TRANSIENT` (everything cleared after each `process()` call, including on exception via try/finally).

### Processing-time timers (2026-09-07)

| Aspect | Detail |
|---|---|
| Availability | Keyed functions only (`KeyedProcessFunction`); non-keyed `ProcessFunction` gets query-only TimerService |
| Dedup | One timer per `(key, timestamp)`; registering the same `(key, ts)` twice is a no-op |
| Delete | `deleteProcessingTimeTimer(ts)` deletes the current key's timer at that timestamp; silent no-op if absent |
| From `onTimer` | Timers can be registered/deregistered inside `onTimer`; chaining works |
| Event time | `registerEventTimeTimer`/`deleteEventTimeTimer` throw UOE (future work); `currentWatermark()` returns `Long.MIN_VALUE` |
| TRANSIENT | `registerProcessingTimeTimer` throws UOE; `currentProcessingTime()` and `currentWatermark()` still work |

#### ProcessingTimerMode (exclusive, chosen at build)

| Mode | Firing mechanism |
|---|---|
| `OPPORTUNISTIC` (default) | Due timers fire after each element invocation in `process()` and at the end of `process()`. `fireProcessingTimers()` callable as explicit nudge. |
| `MANUAL` | Timers never fire automatically. `getTimerService().fireProcessingTimers(): WorkflowResult` is the only firing path. |
| `BACKGROUND` | A single daemon thread per workflow polls for due timers (~100ms interval). Acquires the workflow lock before firing (no interleaving with `process()`). Results/errors delivered via `BackgroundTimerListener`. `fireProcessingTimers()` also callable. |

#### API additions

```java
StandaloneWorkflow.getTimerService()
    .fireProcessingTimers()      // WorkflowResult — fires all due timers across all keyed nodes
    .pendingTimerCount()         // long — count of all pending timers across all keyed harnesses
```

### Thread safety

- Thread-safe by default. One `ReentrantLock` on each `StandaloneWorkflow` guards the entire `process()` call and every `clear*`/`close()` operation (including the TRANSIENT reset, executed within the lock in the same `try/finally` that eventually unlocks).
- Multiple workflows do not contend; separate instances have separate locks.
- Direct harness access (`harness.processViaEdge`) is deliberately unlocked — the workflow is the only supported multithreaded entry point.
- **BACKGROUND timer thread**: a single daemon thread per workflow that acquires the workflow lock before firing timers, guaranteeing `onTimer` never interleaves with `processElement`. The listener is invoked after releasing the lock to avoid deadlocks if the listener calls back into the workflow.

### Dependency sliminess

`flink-streaming-java` is the only compile dependency (pulls `flink-runtime`,
`flink-core`, `flink-shaded-guava`, `commons-math3` + sfl4j transitevely). This library does **not** instantiate any runtim classes — the transiteve runtime classpath is inert. A dedicated `DependencyTreeTest` in `flink-standalone` enforces that no `flink-test-utils`, `flink-clients`, or `flink-runtime-test` artifacts slip into the production scope. The test shells out to `mvn dependency:tree` and asserts absence of the banned artifacts.

Note: `JsonSource` and `JsonSink` declare `jackson-databind` as `provided` scope. Consumers of these convenience classes must supply Jackson on their classpath or add `jackson-databind` to their own POM.

### Parallelism

All operators run with parallelism-1 semantics (single "subtask"). No key redistribution or repartitioning between edges.

### Packages (2026-09-03)

| Package | Audience |
|---|---|
| `org.flink.harness` | Consumer API — `WorkflowBuilder`, `StandaloneWorkflow`, `WorkflowResult`, `WorkflowNode`, `Mode`, `Edge` |
| `org.flink.harness.functions` | Nodes — `NodeHarness` (interface), `FunctionHarness` + subtypes, `HarnessFactory` (public, not API) |
| `org.flink.harness.source` | `StandaloneSource` — public API, subclassable |
| `org.flink.harness.sink` | `StandaloneSink` — public API, subclassable |
| `org.flink.harness.internal` | Implementation — `SandaloneRuntimeContext`, state store, collector, metric group. Do not import; public only because Java package visibility does not cross packages. |
| `org.flink.harness.timer` | Timer service — `ProcessingTimerMode`, `BackgroundTimerListener`, `WorkflowTimerService`, `StandaloneTimerService`, `TimerHeap`, `BackgroundTimerThread`. Consumer API for timer management. |

## Agent directives

- Planned feature candidates (timers, co/broadcast functions, operator state) live in `FUTIRE.md` — check it before designing anyhing beyond v1.
- KEEP THIS DOC CONCIS — it is agent-facing, not user-facing.
- **Update this file on every change** that touches design, module stucture,
  version pins, or supported features.
- Record decisions (reason + date), not prose.
- After every code change, run `mvn -q verify` from root (use `mvnw` wrapper; requires Java 21 on PATH or `JAVA_HOME`).
- If you add a new test, make sure it passes and update the CI check section.
- To verify Flink API signatures or class availability, check the source on GitHub
  (`https://raw.githubusercontent.com/apache/flink/release-2.3/...`) rather than
  inspecting local jars.