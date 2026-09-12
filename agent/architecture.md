# Architecture

## Purpose

Defines what this repository actually is, what parts of the Flink architecture it
replaces, and the constraints that every change must respect. Read this before
adding a subsystem or changing anything about execution, dependencies, or the
Flink version.

## Read this when

- deciding whether a Flink-based implementation idea is applicable here at all
- adding a module, dependency, or package
- changing the execution model (graph, queue, lifecycle, threading)
- upgrading the Flink dependency or touching `pom.xml`
- reasoning about what "standalone" means vs. a MiniCluster or a real cluster

## Mental model

```
flink-harness (library)                 analogous Flink concept intentionally ABSENT
────────────────────────                ────────────────────────────────────────────
WorkflowBuilder                         StreamGraph / StreamExecutionEnvironment
  → StandaloneWorkflow (BFS queue)      ExecutionGraph + JobManager + TaskManager
      → NodeHarness nodes               StreamOperator + StreamTask
          → StandaloneRuntimeContext    RuntimeContext / StreamTask runtime
              → state / metrics / timers
```

Everything runs in the caller's thread inside one JVM. There is no JobManager,
no TaskManager, no network stack, no checkpoint coordinator, no dispatcher,
parallelism > 1, watermarks, or event time. "Deployment" is a method call:
`StandaloneWorkflow.process(...)`.

Repository fact: `flink-streaming-java` is the only compile dependency
(`flink-harness/pom.xml`); the tests that exercise `flink-test` add
`flink-test-utils` in `test` scope only. The transitive `flink-runtime` classes
are on the compile classpath but are never instantiated by this library
(`README.md`, `DependencyTreeTest`).

## Module layout

| Module | Role |
|---|---|
| `flink-harness` | Shipped library. `org.flink.harness` consumer API + `org.flink.harness.graph.*`, `.metrics`, `.state`, `.timer`. |
| `flink-test` | Demo job (`DemoFunctions`): parse → route → keyed accumulate/report with side output. `flink-harness` is a `test`-scoped dep here; `flink-test-utils` is allowed in this module. |
| `flink-standalone` | Integration runner (`StandaloneRunner.buildWorkflow`) that wires `flink-test`'s functions into a harness workflow and runs CSV lines. Enforces dependency cleanliness. |

Package map (working tree; do not trust older docs):

```
org.flink.harness            WorkflowBuilder, StandaloneWorkflow, Edge, Mode, WorkflowNode
org.flink.harness.graph      StandaloneRuntimeContext, RecordingCollector
org.flink.harness.graph.function   NodeHarness, FunctionHarness, HarnessFactory, *Harness
org.flink.harness.graph.result     FunctionResult, WorkflowResult
org.flink.harness.graph.source     StandaloneSource, JsonSource
org.flink.harness.graph.sink       StandaloneSink, JsonSink
org.flink.harness.metrics          StandaloneMetricGroup, StandaloneOperatorMetricGroup
org.flink.harness.state            InMemoryKeyedStateStore, InMemoryStateV2,
                                   CompletedStateFuture, CollectionStateIterator
org.flink.harness.timer            TimerHeap, StandaloneTimerService, WorkflowTimerService,
                                   BackgroundTimerThread, BackgroundTimerListener,
                                   ProcessingTimerMode
```

`NodeHarness` and the `.state`/`.timer`/`.metrics`/`.graph` classes are public only
because Java package visibility does not cross packages. Treat only
`org.flink.harness` root types plus the documented subclassable `StandaloneSource`
/ `StandaloneSink` / `Json*` as consumer API.

## Deliberate differences from Flink

This repository is a *semantic emulation*, not a Flink runtime. The table records
what is intentionally present, absent, or simplified. (Flink contract / repo design
choice unless marked otherwise.)

| Concern | Real Flink | Here | Why |
|---|---|---|---|
| Cluster topology | JobManager + TaskManagers + network | None; one JVM, one thread per workflow call | Offline/demo/embedded use; no distributed execution |
| Graph construction | `StreamGraph` → `JobGraph` → `ExecutionGraph` | `WorkflowBuilder` builds a flat node map + `List<Edge>` | No scheduler to target |
| Operator chaining | fused `StreamTask` operator chains | Every node is an independent `NodeHarness` invoked per element | Simplicity; elements are plain Java objects, no serialization at edges |
| Parallelism | configurable | Always 1 (`TaskInfo.getNumberOfParallelSubtasks() == 1`) | Avoiding key redistribution/repartitioning |
| State backend | pluggable backends + checkpointing (`HeapKeyedStateBackend`, `HashMapStateBackend`) | Plain in-memory per-key `HashMap`, no serialization, no checkpoints/savepoints | No persistence layer; state is per-process |
| Event time / watermarks | supported | Event-time timers throw UOE; `currentWatermark()` returns `Long.MIN_VALUE` | See `timers.md`; planned in `FUTURE.md` §1 |
| Timers | internal timer service, per-key, snapshot on checkpoint | processing-time only, in-memory `TimerHeap`, three firing modes | See `timers.md` |
| Checkpointing / savepoints | core | Not implemented; no `CheckpointedFunction`, no operator state | `FUTURE.md` §5 lists as optional |
| Operator state | `OperatorStateStore` | Not implemented | Same |
| Broadcast variables / distributed cache | legacy, supported | UOE / no-ops | `FUTURE.md` "out of scope" |
| Accumulators | supported | Permanently out of scope (UOE) | Superseded by metrics; see `runtime-context.md` |
| Classloading | per-job user code classloader | `getUserCodeClassLoader()` returns the harness classloader; release hook is a no-op | Single classloader |
| Failure handling | task restart / failover | Exception aborts the `process()` call; no retries | No failover machinery |
| Metrics reporters | pluggable reporter backends | In-memory registry returned in `WorkflowResult` | See `metrics.md` |

Preserved Flink semantics that matter: function/operator lifecycle ordering
(`setRuntimeContext` → `open` → per-element → `close`), non-static inner
`Context`/`OnTimerContext` classes as declared upstream, per-key state scoping,
keyed-only timers, and the `RuntimeContext` method contract shape (both v1 and v2
state accessors; see `runtime-context.md`).

## Flink compatibility baseline

- **Version**: Flink **2.3.0**, all artifacts (`pom.xml` `flink.version`). Module pins are
  managed centrally in `flink-harness-parent`; do not override locally.
- **Java**: 21 (`maven.compiler.source/target`). JUnit 5.11.4, AssertJ 3.27.3,
  Jackson 2.18.3 (`provided` scope for `JsonSource`/`JsonSink`).
- **Artifacts actually used**: `flink-streaming-java` (compile, pulls `flink-core`,
  `flink-runtime`, `flink-shaded-guava`, `commons-math3`, slf4j transitively),
  `jackson-databind` (provided), `slf4j-api`.
- **Flink internals targeted**: the library implements/extends public Flink API
  types (`RichFunction`, `ProcessFunction`, `KeyedProcessFunction`, `RuntimeContext`,
  `MetricGroup`, `TimerService`) and directly constructs `Context`/`OnTimerContext`
  inner classes and descriptors. It depends transitively on `flink-runtime` because
  in Flink 2.x `KeyedProcessFunction` and `TimerService` moved there (verified at
  `release-2.3.0`: `flink-runtime/src/main/java/org/apache/flink/streaming/api/...`),
  but it never boots runtime infrastructure.
- **Version-difference trap**: because 2.x reorganized modules, do **not** reason from
  pre-2.0 Flink source layout. Verify signatures against tag `release-2.3.0` on
  GitHub (`https://raw.githubusercontent.com/apache/flink/release-2.3.0/<module>/...`).

## Invariants and contracts

- No library code may instantiate a Flink runtime class (no `MiniCluster`, no
  `StreamTask`, no `TaskManager`). Only API types and simple descriptors are touched.
- The dependency set of `flink-harness` is a contract enforced by
  `flink-standalone/src/test/java/org/flink/standalone/DependencyTreeTest.java`
  (must not contain `flink-test-utils`, `flink-clients`, `flink-runtime-test`).
- All execution is single-subtask: state, metrics and key computation assume one
  logical instance per node.
- Exactly one workflow lock governs mutation; see `workflow.md`.
- `JsonSource`/`JsonSink` require Jackson on the consumer classpath (`provided`).
  Adding a non-test dependency to the library requires a deliberate design decision.

## Important implementation patterns

- **Builder isolates construction from execution**: `WorkflowBuilder` collects and
  validates; `StandaloneWorkflow` owns the built node map and executes. Do not move
  execution into the builder.
- **Harness factory dispatch**: function-type → harness selection is centralized in
  `HarnessFactory.create(id, function, clock, mode)`. New supported function types
  are added there, not in `WorkflowBuilder`.
- **Typed public surface, raw types internally**: see `harnesses.md` for where
  unchecked casts are allowed to live.
- **Fail loudly at `build()`**: unresolved edge types throw unless the caller opts
  out with `build(true)`. Preserve this default.
- **Lifecycle through `NodeHarness`**: `StandaloneWorkflow` only ever talks to
  `NodeHarness`; sources/sinks/functions are interchangeable from its perspective.

## Common pitfalls / agent traps

- **Copying full-Flink implementations.** Upstream examples assume a `StreamTask`,
  `Environment`, checkpointing, and managed memory. None exist here. Port the
  *semantics*, not the plumbing.
- **Introducing a runtime dependency.** e.g. using `flink-test-utils` utilities in
  library code, or Jackson at compile scope. `DependencyTreeTest` and design review
  are the guards.
- **Assuming serialization across edges.** Elements are passed by reference.
  Object reuse is disabled (`isObjectReuseEnabled() == false`) but there is no
  serializer/deep copy either.
- **Assuming distribution.** No key redistribution; a key is only a state namespace.
- **Assuming persistence.** Killing the JVM loses all state, metrics, and timers.
- **Bumping Flink without checking module moves.** `KeyedProcessFunction` /
  `TimerService` are in `flink-runtime`, `RuntimeContext` in `flink-core` as of 2.3.0.
- **Treating `FUTURE.md` features as implemented.** Check `FUTURE.md` and the
  AGENTS.md scope table before assuming a capability exists.

## Testing and verification

- `flink-standalone/.../DependencyTreeTest` proves production classpath cleanliness.
  **It shells out to `cmd /c mvnw.cmd` and therefore only passes on Windows**; on
  Linux it fails with a start error. Do not "fix" it by changing the runtime
  environment blindly — see `testing.md`.
- `flink-standalone/.../StandaloneRunnerTest` and `flink-test/.../DemoFunctionsTest`
  exercise the full builder→workflow→sink path, so they are the broadest regression
  signal for architectural changes.
- Run `mvn -q verify` from the repo root (Java 21 required). Library-only:
  `mvn -pl flink-harness -q verify`.

## Debugging / investigation map

| Symptom | Likely cause | First inspect |
|---|---|---|
| A production class leaked into the classpath | new dependency | `flink-harness/pom.xml`, `DependencyTreeTest` |
| Behavior differs from a Flink job you are porting | runtime service assumed, e.g. checkpoints/timers/event time | this table, `FUTURE.md`, `state.md`, `timers.md` |
| Flink API symbol not found after version bump | module reorganization | `pom.xml`, then tag `release-2.3.0` sources |
| Everything works but nothing persists across processes | by design | `state.md`, `metrics.md` |

## Related agent references

- [workflow.md](./workflow.md) — graph construction and execution live here; required
  for any change to nodes, edges, or the run loop.
- [harnesses.md](./harnesses.md) — how Flink functions are wrapped and bound to the
  runtime; required before supporting a new function type.
- [runtime-context.md](./runtime-context.md) — the emulated Flink-runtime surface and
  its deliberate holes.
- [state.md](./state.md), [timers.md](./timers.md), [metrics.md](./metrics.md) — the
  three runtime services, each with its own lifecycle and omissions.
- [testing.md](./testing.md) — which tests establish which architecture-level
  guarantees, and the platform caveats of the build.

## External references

- Flink 2.3 docs, DataStream API lifecycle:
  https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/process_function/
  (semantic contract for `ProcessFunction` / `KeyedProcessFunction` lifecycle and timers).
- Flink 2.3 docs, state:
  https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state/
  (baseline for keyed state semantics and scoping).
- Flink 2.3 docs, metrics:
  https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/metrics/
  (metric types and scope hierarchy this project approximates).
- Upstream sources (tag `release-2.3.0`) used to verify contracts:
  `flink-runtime/.../streaming/api/functions/KeyedProcessFunction.java`,
  `flink-runtime/.../streaming/api/TimerService.java`,
  `flink-core/.../api/common/functions/RuntimeContext.java`.
