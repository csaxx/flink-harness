# Testing & verification

## Purpose

Maps the test suite as executable documentation: which test class establishes which
behavior, how to run it, and the environment traps that make some tests
platform-dependent. Use this to pick a regression signal for a change and to know
which invariants are actually verified.

## Read this when

- making a change to any subsystem and choosing what to run
- asserting a behavior and needing to know whether a test already covers it
- seeing a failing test and locating its intent
- adding a test (follow the existing naming/structure)

## Mental model

```
flink-harness   unit + integration tests of the library (no Flink runtime)
flink-test      demo job + tests that drive harnesses directly and via a workflow
flink-standalone integration runner tests + DependencyTreeTest (classpath guard)
```

Tests are the primary evidence for repository facts in the other `/agent` docs.

## Build & run

- Full build: `mvn -q verify` from the repo root (`mvnw` wrapper; **Java 21** required
  on `PATH` or `JAVA_HOME`).
- Library only: `mvn -pl flink-harness -q verify`.
- Test only, no package: `mvn -pl flink-harness -q test`.
- The parent POM sets `maven.compiler.source/target = 21`; even a successful compile on
  an older JDK is not a supported configuration.

### Environment caveats

- **`DependencyTreeTest` only works on Windows.** It builds a `ProcessBuilder("cmd", "/c",
  mvnw.cmd, "dependency:tree", ...)` (`flink-standalone/.../DependencyTreeTest.java`).
  On Linux/macOS there is no `cmd`, so the test fails to start the process. Do not
  assume this test indicates a dependency problem on non-Windows CI without reading the
  failure.
- **A WSL checkout with no Java on `PATH` cannot run Maven at all** (`java: command not
  found`). Build on the host (e.g. Windows) side, or install a JDK 21 in the shell.
- `KeyedTimerIntegrationTest` BACKGROUND tests sleep and depend on a ~100 ms poll
  interval; they can be flaky under heavy load.
- `flink-test` intentionally has `flink-test-utils` in `test` scope (with Testcontainers
  excluded). Only the `flink-harness` and `flink-standalone` *production* classpaths must
  stay clean.

## Test inventory

### `flink-harness` (library)

| Test class | Proves |
|---|---|
| `StandaloneWorkflowTest` | Multi-stage fan-out, keyed branch, side outputs to sink and to an intermediate function, custom source/sink transforms and filtering, multi-source/multi-sink, mode accumulation/reset, graph introspection (kinds/successors/types), metric aggregation (sum + gauge last-wins), and lock correctness under many concurrent `process()` calls (CONTINUOUS and TRANSIENT). |
| `WorkflowBuilderValidationTest` | Duplicate id, unknown edge, type mismatch, unkeyed inbound edge to a keyed function, unresolved generics → fail at `build()` / pass with `build(true)`, `initializeAtBuild()` opens eagerly and surfaces `open()` failures at build time. |
| `KeyedProcessFunctionHarnessTest` | Per-key state isolation and `ctx.getCurrentKey()`; a keyed function invoked with no keyed edge fails loudly. |
| `ProcessFunctionHarnessTest` | Main + side output capture, metric snapshot after invocation, `resetAll()` zeroing built-in counters. |
| `RuntimeContextFillersTest` | `globalJobParameters` default/immutability/copy-on-set/null rejection/builder wiring/TRANSIENT survival; `createSerializer` round-trips (`String`, `Integer`, `Tuple2`); all accumulator accessors throw "permanently out of scope". |
| `MetricGroupTest` | Counter accumulation + snapshot, nested group flattening (`errors.count`), recursive reset, gauge value exposure. |
| `StateStoreTest` (v1) | "no bound key" failure, per-key isolation, descriptor defaults, list/map/reducing/aggregating operations, `clearCurrentKey` vs `clearAll`. |
| `StateStoreV2Test` (v2) | v2 null/copy semantics, async eager chains, `StateIterator.onNext` behavior, all `then*` variants, exception propagation, foreign `StateFuture` rejection, TTL rejection for all five kinds, v1/v2 id isolation, clear coverage. |
| `KeyedV2StateIntegrationTest` | v2 state through the full workflow: per-key counts, CONTINUOUS accumulation, TRANSIENT clearing. |
| `TimerHeapTest` | `(key, timestamp)` ordering/dedup, delete, delete+re-register, cancelled-entry skipping, clear, peek, earliest timestamp. |
| `KeyedTimerIntegrationTest` | OPPORTUNISTIC fire-after-element and not-before-timestamp, side/main outputs from `onTimer`, key+state binding in `onTimer`, timer chaining, MANUAL explicit firing, BACKGROUND delivery + error path, TRANSIENT UOE + build rejection, `pendingTimerCount`, `clearState` clears timers. |
| `JsonSourceTest` / `JsonSinkTest` | Jackson parse/serialize, custom mapper, compact vs pretty output, error wrapping. |

### `flink-test` (demo job)

| Test class | Proves |
|---|---|
| `DemoFunctionsTest` | Each demo function through its harness (`RichFunctionHarness` MAP, `ProcessFunctionHarness`, `KeyedProcessFunctionHarness`) plus `fullWorkflowWithFanOut` end-to-end with sinks and a side output. Demonstrates constructing harnesses directly. |
| `TestSmokeTest` | The module compiles and its test harness runs. |

`DemoFunctions` (parse → route → keyed accumulate / report, with `REJECTED_TAG` side
output) is the canonical multi-stage fixture; `TestSmokeTest` is a no-op guard.

### `flink-standalone` (integration)

| Test class | Proves |
|---|---|
| `StandaloneRunnerTest` | Full runner DAG: accum/report/side outputs, per-node and aggregate metrics, CONTINUOUS accumulation, TRANSIENT reset, `clearState`, `clearStateAll`, `clearMetrics`, `getWorkflow()` kinds/successors, `getNodeIds()`, reading lines from a file. |
| `DependencyTreeTest` | Production dependency tree contains no `flink-test-utils`, `flink-clients`, `flink-runtime-test` (**Windows-only**, see caveats). |
| `StandaloneSmokeTest` | Module loads. |

`flink-standalone/src/test/resources/orders.csv` is a sample CSV fixture in the runner
module. It is currently **not referenced** by any test (the tests inline `SAMPLE_LINES`
or write a temp file), so treat it as unused until a test points at it.

## Surprising behaviors covered by tests

- TRANSIENT results still contain the run's outputs/metrics because the result is built
  before the reset (`StandaloneWorkflowTest.transientModeClearsStateBetweenRuns`).
- Concurrent CONTINUOUS runs accumulate exactly `threads × runs` (`concurrentContinuousWorkflowAccumulatesCorrectly`).
- A side output can be routed to an intermediate function and processed like a main
  stream (`sideOutputEdgeToIntermediateFunction`).
- `onTimer` may register further timers (`timerCanRegisterAnotherTimerFromOnTimer`).
- v2 `update(null)` / `put(null)` remove state; `update(empty list)` removes list state
  (`StateStoreV2Test`).

## Important invariants with **no** test coverage

Document these if you change them; add a test if the behavior is intentional:

- `getUserCodeClassLoader()` returning the harness classloader, and the release-hook no-op
  (`RuntimeContextFillersTest` does not cover these).
- Workflow-level `FunctionResult.sideOutputs` always empty / `WorkflowResult.sideOutputsOf`
  always empty (only sink outputs are asserted).
- v1 State TTL silently ignored (only v2 TTL rejection is tested).
- Custom-counter / gauge / meter / histogram non-reset; source/sink metric non-reset in
  TRANSIENT; IO-metric-group non-snapshotting.
- Gauge name-collision and counter/gauge name-collision behavior in `StandaloneMetricGroup`.
- Background timer thread's lock/no-interleaving guarantee under sustained concurrency.
- `getJobInfo()` returning a `null` job id.

## Adding tests

- Match the existing style: JUnit 5 + AssertJ, package mirrors the class under test,
  descriptive method names (`behaviorUnderCondition`).
- Prefer workflow-level tests for routing/lifecycle changes and store-level tests for
  state changes.
- Keep timing-sensitive tests minimal; use `WorkflowBuilder.clock(Clock)` (a manual clock)
  for deterministic timer tests where possible.
- For dependency/classpath changes, update `DependencyTreeTest` expectations only after a
  deliberate design decision.

## Debugging / investigation map

| Symptom | First inspect |
|---|---|
| `DependencyTreeTest` fails on Linux | platform limitation, not a classpath leak |
| Build cannot find `java` | install/point to JDK 21 (`JAVA_HOME`) |
| BACKGROUND timer test flaky | sleeps + ~100 ms poll; re-run under lighter load |
| behavior changed but suite green | likely one of the uncovered invariants above |
| need ground truth for a routing/state/timer question | find the matching test class in the tables |

## Related agent references

- [architecture.md](./architecture.md) — what the tests are protecting.
- [workflow.md](./workflow.md), [harnesses.md](./harnesses.md), [state.md](./state.md),
  [timers.md](./timers.md), [metrics.md](./metrics.md),
  [runtime-context.md](./runtime-context.md) — each lists its own focused tests; this
  document is the index and records the gaps.
