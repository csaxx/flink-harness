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

## Frozen design decisions

If you change any of these, update this section AND re-evaluate all code.

### Scope (v1)

| Capability | Status |
|---|---|
| Metrics (Counter, Gauge, Meter) | ✔ implement |
| Side outputs (OutputTag) | ✔ implement |
| v1 state (Value/List/Map/Reducing/Aggregating) via `RuntimeContext.get*State(v1)` | ✔ in-memory per-key maps, no serialization |
| v2 state (`org.apache.flink.api.common.state.v2.*`) | ✗ `UnsupportedOperationException` |
| Timers / `TimerService` | ✗ deferred to v2 |
| Accumulators, broadcast variables, distributed cache | ✗ `UnsupportedOperationException` |
| `createSerializer`, `getGlobalJobParameters`, `getUserCodeClassLoader` | ✗ `UnsupportedOperationException` |

### Harness approach

- Each function wrapped in its own harness (thin wrapper).
- `Context` and `OnTimerContext` are **non-static inner classes** — instantiated through
  the wrapped function instance (same technique as Flink operator internals).
- `open(OpenContext)` called once (OpenContext is empty interface — pass singleton).
- `close()` called when the workflow is torn down.
- **Type-safe public surface, raw types inside** — raw/unchecked `@SuppressWarnings`
  confined to four helpers:
  1. Collector adapter (main + side output routing)
  2. KeySelector invocation (`apply(I)` cast)
  3. OutputTag lookup by tag-id
  4. Current-key binding

### Type safety

- `TypeInformation` hints at registration + `TypeExtractor.getBaseTypes()` inference.
- Unresolved generics → **fail loudly at `build()`** unless the edge explicitly opted out
  (`build(optOutTypeValidation=true)`).
- Opt-out edges fall back to per-element `ClassCastException` naming the edge and
  function ids.
- `getWorkflow()` returns DAG tuples annotated with the resolved `TypeInformation`
  (implements `Serializable`), enabling visualization.

### WorkflowBuilder surface (API)

```java
new WorkflowBuilder(mode, threadSafe)
  .registerFunction("id", functionInstance)           // ProcessFunction, RichMap, etc.
  .registerKeyedFunction("id", keyedFunctionInstance)
  .addEdge("srcId", "dstId")                          // untyped edge
  .addKeyedEdge("srcId", "dstId", keySelector)        // keyed routing
  .activateOutput("fnId")                              // sink-equivalent: collect main output
  .activateSideOutput("fnId", outputTag)               // sink-equivalent: collect side output
  .build()
```

Modes: `CONTINUOUS` (metrics & state accumulate like real Flink; manual
`clearState(id)` / `clearStateAll()` / `clearMetrics()`) and `TRANSIENT`
(everything cleared after each `process()` call, including on exception via
try/finally).

### Thread safety

- Per-function `ReentrantLock` when `threadSafe=true`.
- Lock held during the entire `processElement` + collector flush.

### Dependency slimness

`flink-streaming-java` is the only compile dependency (pulls `flink-runtime`,
`flink-core`, `flink-shaded-guava`, `commons-math3` + slf4j transitively). This
library does **not** instantiate any runtime classes — the transitive runtime
classpath is inert. A dedicated `DependencyTreeTest` in `flink-standalone`
enforces that no `flink-test-utils`, `flink-clients`, or `flink-runtime-test`
artifacts slip into the production scope. The test shells out to
`mvn dependency:tree` and asserts absence of the banned artifacts.

### Parallelism

All operators run with parallelism-1 semantics (single "subtask"). No key
redistribution or repartitioning between edges.

### Package

`org.flink.harness` for the shipped library (`flink-harness` module).

## Agent directives

- KEEP THIS DOC CONCISE — it is agent-facing, not user-facing.
- **Update this file on every change** that touches design, module structure,
  version pins, or supported features.
- Record decisions (reason + date), not prose.
- After every code change, run `mvn -q verify` from root.
- If you add a new test, make sure it passes and update the CI check section.