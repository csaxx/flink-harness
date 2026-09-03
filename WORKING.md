# Working document — flink-harness

## Findings (source verification against Flink 2.3.0)

Verification ran against the Apache Flink 2.3.0 tag (`release-2.3.0` on GitHub) to
validate the harness approach:

| Check | Verdict |
|---|---|
| `RuntimeContext` surface | 20 methods: metrics, v1 state (Value/List/Map/Reducing/Aggregating), v2 `@Experimental` state, accumulators, broadcast, distributed cache, job/task info → we implement metrics + v1 state; rest throw UOE |
| `OpenContext` | Empty interface — singleton passable |
| `AbstractRichFunction` | `setRuntimeContext(RuntimeContext)` is public — harness injects its own |
| `ProcessFunction` / `KeyedProcessFunction` | `Context.output(tag, value)` routes side outputs; `Context.getCurrentKey()` in keyed — feasible |
| `Context` / `OnTimerContext` | Non-static inner classes — must instantiate through the enclosing function instance (same as Flink operators) |
| `OperatorMetricGroup` | Extends `MetricGroup` with only `getIOMetricGroup()` — ~8 abstract methods, implementable in ~150 lines |
| `MetricGroup` | `counter`, `gauge`, `histogram`, `meter`, `addGroup`, `getScopeComponents`, `getAllVariables`, `getMetricIdentifier` — all trivial stubs for the standalone use-case |
| `flink-streaming-java` POM | Compile deps: `flink-core`, `flink-runtime`, `flink-file-sink-common`, `flink-shaded-guava`, `commons-math3`, slf4j. **flink-runtime is a transitive compile dep** — acceptable (inert), adds ~50MB jar weight. Optional exclusion recipe to be verified. |
| Timers | Only invoked when user code calls `timerService()` — throwing UOE is safe for timer-free functions; v2 feature |
| OpenContext import | Confirmed — functions compiled for 2.x _must_ use `open(OpenContext)` signature, not old `open(Configuration)`. Demo job must respect this. |

## Design decisions log

| Date | Decision | Rationale |
|---|---|---|
| 2026-09-02 | TypeInformation-based type safety, fail loud at build() for unresolved generics | Stronger than Class tokens (handles Tuple2, POJOs); serializable for DAG visualization; TypeExtractor inference works for most functions |
| 2026-09-02 | Defer timers to v2 | Adds ~2x harness complexity (priority queue, firing window); no demo job need |
| 2026-09-02 | Non-static inner Context classes | Must be instantiated through function instance — exact Flink operator pattern |
| 2026-09-02 | Raw types confined to 4 helper boundaries | Collector adapter, KeySelector invoke, OutputTag lookup, current-key binding |
| 2026-09-02 | CONTINUOUS mode replicates real Flink semantics (state accumulation) | User contract: dedup, sum, etc. should persist; explicit clear APIs for managing |
| 2026-09-02 | Parallelism-1 subtask semantics | No key repartitioning between edges; single-subtask execution |
| 2026-09-02 | State accessors resolve backing map lazily per operation | Fixed cross-key bleed found by tests: captured map bound wrong key |
| 2026-09-02 | Keyed functions as workflow entrypoint require entry KeySelector via registerKeyedFunction overload | Otherwise open() fails loudly at first invocation (discovered by tests) |
| 2026-09-02 | State descriptor serializers prepared via ExecutionConfig | getDefaultValue() needs initialized serializer (found by tests) |
| 2026-09-03 | Thread safety: single workflow-level lock on StandaloneWorkflow; threadSafe param removed, Thread-safe by default | User concern: in TRANSIENT mode per-function locks let concurrent traversals interleave with the finally-reset. Workflow lock guarantees atomic process(); direct harness access deliberately unlocked |

## Stepwise roadmap

### Phase 0 — Module scaffolding (completed)
- [x] Root `pom.xml` — aggregator, Java 21, Flink 2.3.0, JUnit 5, AssertJ
- [x] `flink-harness/pom.xml` — jar module with `flink-streaming-java` compile dep
- [x] `flink-test/pom.xml` — jar module with test deps
- [x] `flink-standalone/pom.xml` — jar module depending on harness + test
- [x] `mvn -q verify` passes (3 modules compile, 3 smoke tests pass)
- [x] Updated `.gitignore` (maven target/, *.iml, .idea/)
- [x] Copied Maven wrapper from sibling `truffle` project (GraalVM 25, Java 21)
- [x] Removed old root-level `src/` and `flink-harness.iml`

### Phase 1 — Harness core (completed)
- [x] `StandaloneRuntimeContext` — metrics + v1 state supported, rest UOE
- [x] `StandaloneMetricGroup`/`StandaloneOperatorMetricGroup` — flat registry, snapshot, counter reset
- [x] `InMemoryKeyedStateStore` — per-key maps with dynamically-resolved accessor backing (lazy, Flink-like handle semantics)
- [x] State accessor descriptor serializer prepared via `ExecutionConfig` so defaults work
- [x] `UnsupportedOperationException` for out-of-scope API surface
- [x] `RecordingCollector` for main outputs

### Phase 2 — Function harnesses (completed)
- [x] `FunctionHarness` base — open/close lifecycle, key-bind before openOnce, lock handling
- [x] `ProcessFunctionHarness` — non-static inner `Context` instantiated via function instance; side outputs via context
- [x] `KeyedProcessFunctionHarness` — adds `getCurrentKey()` binding via inbound edge selector
- [x] `RichFunctionHarness` — MAP/FLATMAP/FILTER dispatch
- [x] `HarnessFactory` — instance-based dispatch, fails loud on unsupported types
- [x] Raw unchecked casts confined to: KeySelector invocation, function invocation, OutputTag routing, current-key binding

### Phase 3 — WorkflowBuilder / StandaloneWorkflow (completed)
- [x] `WorkflowBuilder` — registration (+ explicit TypeInformation hints), edges, key edges, activations, strict or opt-out type validation at build()
- [x] Ambient type hints resolved at `build()`, fail loud unless `build(true)`
- [x] `StandaloneWorkflow.process(inputs, entryId)` — queue-based BFS traversal
- [x] Entry of a keyed function directly: register `KeySelector` via `registerKeyedFunction(id, fn, selector)` overloads; otherwise clear error
- [x] `getWorkflowNodes` → serializable DAG via `WorkflowNode`
- [x] `clearState/clearMetrics` per-function and all; `TRANSIENT` try/finally reset
- [x] Workflow-level `ReentrantLock` on `StandaloneWorkflow` (thread-safe by default; direct harness access unlocked)

### Phase 4 — Demo job (flink-test) (completed)
- [x] `DemoFunctions.java` — `ParsedOrder` record, `ParseFn` (RichMap), `RouteFn` (ProcessFunction, side output "rejected"), `AccumulateFn` (KeyedProcessFunction, per-customer ValueState for total+count), `ReportFn` (ProcessFunction)
- [x] Unified `Locale.US` formatting for `String.format("%.2f")` to avoid locale-dependent decimal separators
- [x] Multi-stage topology: parse → route → [fan-out: accumulate (keyed) + report (unkeyed)]
- [x] 7 tests: single-function (parse, route good/bad, accumulate, report) + full workflow integration
- [x] `flink-harness` added as test-scope dependency in flink-test pom
- [x] `Edge` record made public (was package-private, needed by demo tests)

### Phase 5 — Standalone integration (flink-standalone) (completed)
- [x] `StandaloneRunner.java` — public entry point with `buildWorkflow(mode)` and `run(csvLines, mode)` convenience methods
- [x] Sample input file `orders.csv` under `src/test/resources`
- [x] 13 tests: outputs (accum/report), side outputs, metrics, CONTINUOUS accumulate across process calls, TRANSIENT clear, clearState/clearStateAll/clearMetrics, getWorkflow DAG, getFunctionIds, runFromFile
- [x] `Edge` record made public (was package-private, needed by demo tests)

### Phase 6 — Dependency slimness verification (completed)
- [x] `DependencyTreeTest` — shells out to `mvn dependency:tree`, asserts no `flink-test-utils`, `flink-clients`, `flink-runtime-test` artifacts
- [x] All 14 tests pass (13 standalone + 1 dependency tree)
- [x] AGENTS.md dependency-slimness section updated with verified status
- [x] `slf4j-simple` added as test-scope dependency to both flink-harness and flink-standalone poms

### Phase 7 — Polish
- [ ] README.md usage example with relevant information for consumers
- [ ] JavaDoc on public API surface
### Phase 7 — Polish (completed)
- [x] `README.md` — usage example, feature matrix, build commands, architecture diagram
- [x] JavaDoc audit — all public API classes have adequate documentation
- [x] `@SuppressWarnings("unchecked")` audit — confirmed confined to 4 boundary helpers + state store internals
- [x] flink-test's `flink-harness` dep is `test` scope — no leak into flink-standalone
- [x] DependencyTreeTest verifies no banned artifacts (flink-test-utils, flink-clients, flink-runtime-test)

## Dependency key

```
- flink-harness:   compile → flink-streaming-java (transitive: flink-core, flink-runtime, ...)
                    test → JUnit 5, AssertJ, slf4j-simple (test log)
- flink-test:      compile → flink-streaming-java
                    test → flink-test-utils, JUnit 5, AssertJ
- flink-standalone: compile → flink-harness, flink-test
                    test → JUnit 5, AssertJ
```

## Build commands

| Command | Action |
|---|---|
| `mvn -q verify` | Full build + test (root) |
| `mvn -pl flink-harness -q verify` | Build + test harness only |
| `mvn -pl flink-test -q verify` | Build + test demo job (requires harness installed) |
| `mvn -pl flink-standalone -q verify` | Build + test standalone (requires both) |
| `mvn dependency:tree` | Print dependency tree for a module |

## Current step in progress

None — all phases complete.