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
| Metrics (Counter, Gauge, Meter) | ✔ rich functions only, via `RuntimeContext.getMetricGroup()`; plain functions and synthetic source/sink have no metric group |
| Side outputs (OutputTag) | ✔ implement (routable as side-channel edges to any node) |
| v1 state (Value/List/Map/Reducing/Aggregating) via `RuntimeContext.get*State(v1)` | ✔ in-memory per-key maps, no serialization |
| v2 state (`org.apache.flink.api.common.state.v2.*`) | ✔ in-memory per-key, eager `StateFuture`/`StateIterator`; TTL-enabled descriptors rejected (UOE) |
| Timers — processing-time (keyed only) | ✔ three modes: OPPORTUNISTIC, MANUAL, BACKGROUND; per-(key, ts) dedup; event-time: UOE (future work) |
| `createSerializer` | ✔ via `TypeInformation.createSerializer(new SerializerConfigImpl())` |
| `getGlobalJobParameters` | ✔ via `WorkflowBuilder.globalJobParameters(map)` |
| Accumulators | ✗ permanently out of scope — use metrics instead |
| Broadcast variables, distributed cache | ✗ `UnsupportedOperationException` |
| `getUserCodeClassLoader` | ✔ returns the harness classloader; `registerUserCodeClassLoaderReleaseHookIfAbsent` is a no-op |
| StandaloneSource / StandaloneSink | ✔ concrete, subclassable, default passthrough |
| JsonSource / JsonSink | ✔ convenience, Jackson-databind (provided scope) |
| Side-channel edges (OutputTag on `DataStreamEdge`) | ✔ main/side output routing via `sideTag == null` |
| Non-rich single-stream functions (`MapFunction` / `FlatMapFunction` / `FilterFunction`) | ✔ implement (no lifecycle, no `RuntimeContext`, no keyed state — like Flink) |

### Harness approach

- **`StreamNode`** interface (`org.flink.harness.graph`) consumed by `StandaloneWorkflow`. Implemented by:
  - `AbstractFunctionHarness<F extends Function>` (abstract) — wraps a constructor-injected Flink
    function (`getFunction()`) and owns the node identity. No lifecycle, no metrics.
  - `AbstractSingleStreamFunctionHarness<F extends Function>` (abstract) — shared output buffer and
    result assembly for the non-rich single-stream interfaces; one concrete per type:
    `MapFunctionHarness`, `FlatMapFunctionHarness`, `FilterFunctionHarness`.
  - `AbstractRichFunctionHarness<F extends AbstractRichFunction>` (abstract) — adds the
    `RuntimeContext` wiring, open/close lifecycle, key binding, keyed-state scoping and the
    per-node metric group (Flink-faithful: any rich function on a keyed edge may use keyed
    state; only non-keyed streams throw in Flink). Concrete: `ProcessFunctionHarness`,
    `KeyedProcessFunctionHarness` (timers), `RichMapFunctionHarness`,
    `RichFlatMapFunctionHarness`, `RichFilterFunctionHarness`.
  - `StandaloneSource` / `StandaloneSink` — synthetic nodes with no-op lifecycle; no metrics.
- `Context` and `OnTimerContext` are **non-static inner classes** — instantiated through the wrapped function instance (same technique as Flink operator internals).
- `open()` is idempotent via an `opened` flag: called eagerly at `build()` under
  `initializeAtBuild()`, lazily on the first element otherwise. `KeyedProcessFunctionHarness.open()`
  binds a dummy placeholder key when no key is bound yet (eager path) so state-handle
  registration cannot fail; the first keyed element replaces it.
- `close()` called when the workflow is torn down.
- Main and side outputs are captured per invocation inside each harness (`Context.output` records side outputs); `RecordingCollector` is a trivial list-backed `Collector`.
- **Type-safe public surface, raw types inside** — unchecked `@SuppressWarnings` is confined
  to narrow boundary helpers: key-selector invocation (`AbstractRichFunctionHarness.bindKey`),
  function casts in the harness constructors, source/sink passthrough casts,
  `WorkflowResult.sideOutputsOf`, and the
  `org.flink.harness.state` accessors / `CompletedStateFuture.resolve`. Details in
  `agent/harnesses.md`.

### Type safety

- `TypeInformation` hints supplied at registration only — there is **no** `TypeExtractor` inference.
- Unresolved edge types → **fail loudly at `build()`** unless opt out (`build(optOutTypeValidation=true)`).
- Opt-out edges fall back to per-element `ClassCastException` naming the edge and function ids.
- `getWorkflow()` returns DAG tuples annotated with resolved `TypeInformation` and `WorkflowNode.Kind` (SOURCE/FUNCTION/SINK).

### WorkflowBuilder surface (API)

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
  .registerFunction("id", functionInstance)               // ProcessFunction, RichMap, MapFunction, etc.
  .registerFunction("id", keyedProcessFunctionInstance)   // overload; KeyedProcessFunction
  .addSink("sinkId")                                     // default collecting sink
  .addSink("id", customSink)                             // custom subclass
  .addSourceEdge("srcId", "dstId")                      // source → node
  .addEdge("srcId", "dstId")                            // main channel edge
  .addKeyedEdge("srcId", "dstId", keySelector)          // keyed main channel
  .addSideOutputEdge("srcId","dstId", tag)              // side channel edge (general)
  .addKeyedSideOutputEdge("src","dst", tag, keySelector) // keyed side channel
  .addSinkEdge("srcId", "dstId"[, tag])                  // sink terminal edge (sugar)
  .build()
```

`process(inputs, sourceId)` returns `WorkflowResult(functionResults, aggregatedMetrics)` — functionResults is a `Map<nodeId, FunctionResult<Object>>` for nodes that produced outputs (sinks) or rich-function metrics; `aggregatedMetrics` is a flat cross-node map where counters/meters/histograms are summed and gauges last-wins.

Modes: `CONTINUOUS` (metrics & state accumulate like real Flink; manual `resetState(id)` / `resetStateAll()` / `resetMetrics(id)` / `resetMetricsAll()`) and `TRANSIENT` (everything cleared after each `process()` call, including on exception via try/finally).

### Processing-time timers

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

- Thread-safe by default. One `ReentrantLock` on each `StandaloneWorkflow` guards the entire `process()` call and every `reset*`/`close()` operation (including the TRANSIENT reset, executed within the lock in the same `try/finally` that eventually unlocks).
- Multiple workflows do not contend; separate instances have separate locks.
- Direct harness access (`harness.processElement`) is deliberately unlocked — the workflow is the only supported multithreaded entry point.
- **BACKGROUND timer thread**: a single daemon thread per workflow that acquires the workflow lock before firing timers, guaranteeing `onTimer` never interleaves with `processElement`. The listener is invoked after releasing the lock to avoid deadlocks if the listener calls back into the workflow.

### Dependency sliminess

`flink-streaming-java` is the only compile dependency (pulls `flink-runtime`,
`flink-core`, `flink-shaded-guava`, `commons-math3` + slf4j transitively). This library does **not** instantiate any runtime classes — the transitive runtime classpath is inert. A dedicated `DependencyTreeTest` in `flink-standalone` enforces that no `flink-test-utils`, `flink-clients`, or `flink-runtime-test` artifacts slip into the production scope. The test shells out to `mvn dependency:tree` and asserts absence of the banned artifacts.

Note: `JsonSource` and `JsonSink` declare `jackson-databind` as `provided` scope. Consumers of these convenience classes must supply Jackson on their classpath or add `jackson-databind` to their own POM.

### Parallelism

All operators run with parallelism-1 semantics (single "subtask"). No key redistribution or repartitioning between edges.

### Packages

| Package | Audience |
|---|---|
| `org.flink.harness` | Consumer API — `WorkflowBuilder`, `StandaloneWorkflow`, `WorkflowNode`, `Mode` |
| `org.flink.harness.graph` | Implementation — `StreamNode` (interface), `DataStreamEdge`, `StandaloneRuntimeContext`, `RecordingCollector` |
| `org.flink.harness.graph.function` | Nodes — `AbstractFunctionHarness` → `AbstractSingleStreamFunctionHarness` / `AbstractRichFunctionHarness` → concrete harnesses, `HarnessFactory` (public, not API) |
| `org.flink.harness.graph.result` | Results — `FunctionResult`, `WorkflowResult` |
| `org.flink.harness.graph.source` | `StandaloneSource`, `JsonSource` — public API, subclassable |
| `org.flink.harness.graph.sink` | `StandaloneSink`, `JsonSink` — public API, subclassable |
| `org.flink.harness.metrics` | Implementation — `StandaloneMetricGroup`, `StandaloneOperatorMetricGroup` |
| `org.flink.harness.state` | Implementation — `InMemoryKeyedStateStore` (v1+v2 keyed state), `CompletedStateFuture`, `CollectionStateIterator`, `InMemoryStateV2`. Public only because Java package visibility does not cross packages; not API. |
| `org.flink.harness.timer` | Timer service — `ProcessingTimerMode`, `BackgroundTimerListener`, `WorkflowTimerService`, `StandaloneTimerService`, `TimerHeap`, `BackgroundTimerThread`. Consumer API for timer management. |

## Agent directives

- Planned feature candidates (event-time timers, co/broadcast functions, operator
  state, sinks) live in `FUTURE.md` — check it before designing anything beyond v1.
- KEEP THIS DOC CONCISE — it is agent-facing, not user-facing. Deep detail belongs in
  `agent/` (below), not here.
- **Update this file on every change** that touches design, module structure,
  version pins, or supported features.
- Record decisions (reason), not prose.
- **Fix issues you encounter**: always correct issues, inconsistencies and stale
  references in the code or docs that you find while working on a task — but first
  indicate the problem to the user and get confirmation before changing anything
  beyond the immediate task.
- **No dates in documentation** — no "added on" stamps, no dated section headers, no
  dated decision records. Chronology belongs to git history, not to prose.
- **Comment discipline**: non-trivial classes/methods carry a concise
  rationale comment — their role in the control flow, what they do, and any trap a
  maintainer could hit. Use javadoc on consumer-API classes and `//` one-liners
  internally; no `@param`/`@return` boilerplate. A method with several distinct
  functional blocks gets a one-line `//` summary per block. Trivial methods (getters,
  fluent setters, record accessors) stay uncommented. When you add or materially change
  such a method, add/update its comment in the same change.
- **Naming discipline**: variable names (locals, fields, parameters) are
  never a single character — they indicate type and, if applicable, function.
  `AbstractFunctionHarness h = …` becomes `AbstractFunctionHarness functionHarness`;
  `StreamNode src = …` becomes `StreamNode srcNode`. Lambda parameters and
  catch parameters may stay idiomatic.
- **Design guidance**:
  - Mirror the wrapped library's class shape: the harness hierarchy follows Flink's
    own (`Function` ↔ `AbstractFunctionHarness`, `AbstractRichFunction` ↔
    `AbstractRichFunctionHarness`, `AbstractUdfStreamOperator` ↔
    `AbstractSingleStreamFunctionHarness` with one concrete harness per non-rich single-stream
    function type). Locate functionality where the wrapped type actually provides it:
    lifecycle, `RuntimeContext`, keyed state and metrics exist only on the rich branch.
  - Constructor injection over post-construction wiring: everything a node needs
    (function, clock, global job parameters) arrives via the constructor — no `set*`
    calls at build time.
  - One idempotent lifecycle method guarded by a flag (`open()`) instead of parallel
    method pairs (`openOnce`/`openOnceEager`).
  - Name methods after the wrapped domain's nomenclature (`processElement`, not
    `processViaEdge`); name symmetric operations symmetrically
    (`resetState`/`resetMetrics`/`resetAll`); name accessors after the thing
    (`metricGroup()`), not the mechanism (`unwrapMetricGroup`).
- After every code change, run `mvn -q verify` from root (use `mvnw` wrapper; requires Java 21 on PATH or `JAVA_HOME`).
- If you add a new test, make sure it passes and update the CI check section.
- To verify Flink API signatures or class availability, check the source on GitHub
  (`https://raw.githubusercontent.com/apache/flink/release-2.3.0/...`) rather than
  inspecting local jars. Note that in Flink 2.x `KeyedProcessFunction`/`TimerService`
  live in `flink-runtime` and `RuntimeContext` in `flink-core`, not in
  `flink-streaming-java`.

## Git rules

- **Use `git mv` for every move or rename** — never delete-and-add, so history is
  preserved.
- **Never prepare or create commits yourself** unless the user explicitly asks. Do
  not stage (`git add`) or commit.
- **Propose a commit message after each task**: a concise, itemized (bulleted) message
  capturing the essence of the work — not a per-file changelog of detailed changes.

## `/agent` reference system

`agent/` holds deeper, implementation-oriented references. They are **not** user
documentation and not a Flink tutorial; each answers "what must I know before changing
this subsystem?".

Rules for using them:

- Read only the reference(s) relevant to your task — do not load them all
  indiscriminately.
- Before a non-trivial change to a subsystem, load its reference(s).
- If a task crosses subsystem boundaries, load every relevant reference.
- If a reference appears inconsistent with the code, investigate the code and fix the
  reference as part of the task (the code is the source of truth).
- When you change architecture, lifecycle, invariants, APIs, behavior, threading, or an
  important implementation pattern, update the affected reference(s) and their
  cross-links **in the same task**.
- Do not rewrite a reference just because a source file was touched; update it only when
  information another agent relies on has changed. Make that determination deliberately.

| Reference | Read when | Why |
|---|---|---|
| [`agent/architecture.md`](agent/architecture.md) | adding dependencies/modules, changing the execution model, upgrading Flink, or questioning whether a Flink feature applies here | Defines what replaces the Flink cluster, the 2.3.0 compatibility baseline, dependency rules, and the deliberate omissions |
| [`agent/workflow.md`](agent/workflow.md) | touching `WorkflowBuilder`/`StandaloneWorkflow`, edges, BFS routing, result aggregation, locking, or modes | The graph/execution contract, build-validation order, and result shape all live here |
| [`agent/harnesses.md`](agent/harnesses.md) | adding a function type, changing open/close/key-binding, the inner `Context` pattern, sources/sinks, or cast handling | Emulates the Flink operator/function lifecycle; documents the raw-cast confinement policy |
| [`agent/runtime-context.md`](agent/runtime-context.md) | a function calls a `RuntimeContext` method; changing job/task info, params, serializers, or out-of-scope policy | The exact implemented/stubbed/unsupported runtime surface |
| [`agent/state.md`](agent/state.md) | changing keyed state, per-key scoping, v2 futures, TTL, or clearing | State has repository-specific lifecycle and v1/v2 semantic differences |
| [`agent/timers.md`](agent/timers.md) | changing timer registration/firing, `ProcessingTimerMode`, or the background thread | Timers are keyed-only, in-memory, mode-dependent, and can poison a workflow |
| [`agent/metrics.md`](agent/metrics.md) | registering/resetting metrics or changing result aggregation | Metric groups and cross-node aggregation have specific reset/collision rules |
| [`agent/testing.md`](agent/testing.md) | choosing a regression signal, adding tests, or interpreting a failure | Maps tests to the behavior they prove, plus platform/build caveats and coverage gaps |
