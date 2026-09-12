# Workflow: construction & execution

## Purpose

Owns the whole path from a fluent builder to a executed DAG: node registration,
edges (main / keyed / side-output), build-time validation, the BFS element queue,
result aggregation, locking, and the CONTINUOUS/TRANSIENT lifecycle. If a change
alters how elements move or how results are produced, it belongs here.

## Read this when

- adding/changing `WorkflowBuilder` methods or edge kinds
- changing build-time validation or type checking
- changing the execution order of elements or timers
- changing `WorkflowResult` / `FunctionResult` shape or aggregation
- changing locking, modes, or reset behavior
- debugging "why did my element not reach the sink / result"

## Mental model

```
builder phase (single-threaded, no side effects):        run phase (under workflow lock):
  registerFunction/KeyedFunction ─┐                        process(inputs, sourceId)
  addSource/addSink ──────────────┤                          └─ BFS Deque<Invocation>
  addEdge/addKeyedEdge/… ─────────┤                              node.processViaEdge(elem, edge)
  build() ── validate ── open? ───┘                              → route outputs onto outbound edges
                                                                 → sinks' outputs collected
                                                                 → timers fired (mode dependent)
```

## WorkflowBuilder surface (actual)

`flink-harness/src/main/java/org/flink/harness/WorkflowBuilder.java`

```java
new WorkflowBuilder(Mode.CONTINUOUS | Mode.TRANSIENT)
  .initializeAtBuild()                                  // eager open() during build()
  .clock(Clock)                                         // default SystemClock.getInstance()
  .globalJobParameters(Map<String,String>)              // copied; default Map.of()
  .setProcessingTimerMode(mode)                         // OPPORTUNISTIC default
  .setProcessingTimerMode(BACKGROUND, listener)         // listener required for BACKGROUND
  .addSource(id) / (id, ioType) / (id, inType, outType) / (id, source) / (id, source, in, out)
  .registerFunction(id, fn) / (id, fn, inType, outType)
  .registerKeyedFunction(id, fn) / (id, fn, inType, outType)
  .addSink(id) / (id, inType) / (id, sink) / (id, sink, inType)
  .addEdge(src, dst)
  .addKeyedEdge(src, dst, keySelector)
  .addSideOutputEdge(src, dst, tag)
  .addKeyedSideOutputEdge(src, dst, tag, keySelector)
  .addSourceEdge(src, dst[, keySelector])               // sugar for addEdge/addKeyedEdge
  .addSinkEdge(src, dst[, tag])                         // sugar for addEdge/addSideOutputEdge
  .build() / .build(optOutTypeValidation)
```

Facts to preserve:

- Node ids are unique across sources, functions, and sinks (`requireUnique`); a
  duplicate throws `IllegalArgumentException` immediately at registration.
- Registration order is preserved (`LinkedHashMap`), which determines node order in
  `getWorkflow()` and the order of gauge aggregation (see `metrics.md`).
- `globalJobParameters` makes an immutable copy at registration (`Map.copyOf`).
- `clock` and timer mode are build inputs; changing the clock after build is impossible.

## Node model

`org.flink.harness.WorkflowNode` is an introspection-only record
`(functionId, kind, inputType, outputType, successors)` with
`Kind = SOURCE | FUNCTION | SINK`. `WorkflowNode.UNKNOWN_TYPE = "<unknown>"` is used
when no `TypeInformation` hint was supplied. The runtime dispatching is done via
`NodeHarness`, not `WorkflowNode` (see `harnesses.md`).

## Edge model

`org.flink.harness.Edge` = `(src, dst, keySelector, sideTag)`:

- `keyed() == keySelector != null` — the destination binds the current key from the
  transported element before invoking the function.
- `sideChannel() == sideTag != null` — the edge transports the source's side output
  for that `OutputTag` instead of its main outputs.
- Both can be set (`addKeyedSideOutputEdge`): side output is keyed at the destination.

`addSourceEdge`/`addSinkEdge` are pure sugar; they do not validate node kinds.

## Build-time validation order

`WorkflowBuilder.build(boolean optOutTypeValidation)` — order is observable, keep it:

1. `Mode.TRANSIENT` with any timer mode other than `OPPORTUNISTIC` → `IllegalStateException`.
2. `BACKGROUND` without a `BackgroundTimerListener` → `IllegalStateException`.
3. Create harnesses for all registered functions via `HarnessFactory.create(id, fn, clock, mode)`
   and wire `globalJobParameters`. Unsupported function types throw here
   (`IllegalArgumentException`; see `harnesses.md`).
4. Add sources and sinks to the node map directly (they are already `NodeHarness`).
5. `validateTopology`: a SOURCE may not receive inbound edges; a SINK may not have
   outbound edges.
6. Per-edge type + keyed validation:
   - side-channel edge with a typed tag: tag type must equal the destination input type
     when both are known; an untyped tag fails unless opted out.
   - main edge: if both source output type and destination input type are known they
     must be equal; if either is unknown it fails unless opted out.
   - a destination whose harness `requiresKeyedEdge()` (i.e. `KeyedProcessFunction`)
     must receive a `keyed()` edge.
7. If `initializeAtBuild()`, `openOnceEager()` each node.
8. Build the `WorkflowNode` graph and collect keyed harnesses for timer management.

Type hints come only from `TypeInformation` passed at registration. There is **no
`TypeExtractor` inference** (older docs claimed this; it is false). With no hint,
an edge is "unresolved" and `build()` fails unless `optOutTypeValidation == true`.
On opt-out, type mistakes surface later as `ClassCastException` during invocation,
not at build.

## Execution

`flink-harness/src/main/java/org/flink/harness/StandaloneWorkflow.java`

- `process(List<?> inputs, String sourceId)` acquires the single `ReentrantLock`,
  calls `checkFailed()`, then `doProcess`.
- `doProcess` rejects unknown `sourceId` (`IllegalArgumentException` listing
  registered sources), enqueues one `Invocation(sourceId, element, null)` per input,
  and drains. `Invocation` = `(functionId, element, inboundEdge)`.
- `drainBfsQueue` (MANUAL/BACKGROUND and non-opportunistic phases): poll one
  invocation → `node.processViaEdge(element, inboundEdge)` → if the node is a sink,
  append its outputs to the per-sink accumulator → `routeResult` enqueues downstream
  invocations. It is a FIFO BFS: fan-out preserves parent order, multiple roots are
  interleaved breadth-first.
- `routeResult(srcId, result, queue)`: for every outbound edge of `srcId`:
  - side channel: `result.sideOutputs().get(tag)`; if `null`, skip (the source did not
    emit that tag). Otherwise enqueue each element to `edge.dst()` with this edge.
  - main channel: enqueue `result.outputs()`.
  - A side output with **no** matching edge is silently dropped (it never reaches a
    result).
- `OPPORTUNISTIC` uses `drainBfsQueueOpportunistic`, which fires due timers after each
  processed element (and again after the queue drains) by appending timer outputs to
  the same queue. See `timers.md`.

## Results

`buildWorkflowResult` is called before any TRANSIENT reset:

- Only **sink** outputs are collected (`sinkIds.contains(...)`). Intermediate
  function outputs are discarded after routing. `result.outputsOf("middle")` is empty
  unless `middle` is itself a sink.
- A node appears in `functionResults` iff it produced sink outputs **or** its metrics
  snapshot is non-empty. Plain pass-through functions with no metrics do not appear.
- `FunctionResult.sideOutputs` in the workflow-level result is always `Map.of()`:
  side outputs are routed to downstream sinks but not retained on the producing node.
  **Trap:** `WorkflowResult.sideOutputsOf(fnId, tag)` therefore always returns empty
  for workflow results — assert on the side-output *sink's* outputs instead. (This is
  current behavior; it is not covered by a dedicated test.)
- `aggregatedMetrics` is a flat cross-node map; aggregation rules live in
  `metrics.md`.

## Modes and reset

- `Mode.CONTINUOUS`: state, metrics, and timers persist across `process()` calls.
- `Mode.TRANSIENT`: after every `process()` call, `resetTransient()` calls
  `resetAll()` (clear state + clear metrics) on every node. The reset runs in a
  `finally` inside the lock, so it happens even when the run throws. Results were
  already built, so a TRANSIENT result still reports that run's outputs/metrics.
- TRANSIENT forbids timer registration: the chosen timer mode must be the default
  `OPPORTUNISTIC`, and `HarnessFactory` passes `allowTimerRegistration = false`, so
  `registerProcessingTimeTimer` throws UOE at call time.

## Threading / lifecycle

- One `ReentrantLock` per `StandaloneWorkflow` guards `process`, `fireProcessingTimers`,
  `clearState*`, `clearMetrics*`, and `close`. Separate workflows do not contend.
- `close()` is idempotent (guarded by a `closed` flag): stops the background timer
  thread if present, then calls `close()` on every node. Nodes opened lazily are
  closed only if they were opened (`FunctionHarness.close` checks `opened`).
- Direct `NodeHarness.processViaEdge` calls bypass the lock by design; the workflow is
  the only supported concurrent entry point.
- `checkFailed()` runs at the start of every locked operation and rethrows a background
  timer thread failure, permanently poisoning the workflow (see `timers.md`).

## Invariants and contracts

- Build must remain side-effect-free unless `initializeAtBuild()` is requested.
- A `SOURCES` node can never be a destination; a `SINK` can never be a source.
- `KeyedProcessFunction` destinations require keyed inbound edges — enforced at build.
- `process()` must validate `sourceId` and must reset TRANSIENT state inside the lock
  in a `finally`.
- Sink outputs are the canonical observable result; do not start depending on
  intermediate outputs without changing the aggregation contract deliberately.
- Element objects cross edges by reference, not by serialization.

## Important implementation patterns

- Validation is ordered and eager at `build()`; prefer adding new checks there rather
  than at run time.
- `StandaloneWorkflow` depends only on `NodeHarness`, never on concrete harness types
  except for timer collection (`KeyedProcessFunctionHarness`).
- Prefer adding edge kinds as `Edge` fields plus builder methods and updating
  `routeResult`; the record is the single source of routing truth.
- Keep TRANSIENT reset centralized in `resetTransient()`.

## Deliberate differences from Flink

- No `StreamGraph`/`ExecutionGraph`, no chaining, no `RichFunction` fusion: each node
  is invoked independently per element.
- Processing order is global FIFO BFS across the whole graph, not per-operator
  mailbox/single-input ordering with watermarks.
- There is no backpressure, checkpoint barrier, or unaligned processing.
- Type validation is structural equality of `TypeInformation` hints, not Flink's full
  type-inference/`TypeExtractor` pipeline.
- `close()` has no task-cancellation semantics and cannot fail a job; it is best-effort
  teardown.

## Common pitfalls / agent traps

- **Expecting intermediate outputs in the result.** Only sinks are collected.
- **Relying on `WorkflowResult.sideOutputsOf`.** It is always empty; use a sink.
- **Assuming `addSourceEdge` validates kinds.** It is just `addEdge`; real checks are
  in `validateTopology`.
- **Forgetting the keyed-edge requirement** when registering a `KeyedProcessFunction`:
  build fails, and the message names the offending edge.
- **Changing type validation defaults.** `build()` must stay strict; opt-out is explicit.
- **Mutating the built graph.** Nodes/edges are captured at build; there is no
  reconfiguration API.
- **Assuming element ordering is per-key.** It is global FIFO; key only controls state.
- **Adding a mode without updating the TRANSIENT timer guard** in `build()` and
  `HarnessFactory`.

## Testing and verification

`flink-harness/src/test/java/org/flink/harness/`:

- `StandaloneWorkflowTest` — `multiStageFlowWithFanOutAndKeyedBranch`,
  `sideOutputEdgeToSink…`, `sideOutputEdgeToIntermediateFunction`,
  `customSourceTransformsInput`, `customSinkTransformsOutput`, `sinkFiltersElements`,
  `multiSourceMultiSinkWorkflow` establish routing/fan-out/side-output behavior;
  `continuousAccumulates` / `transientModeClearsStateBetweenRuns` establish mode
  semantics; `aggregatedMetricsSumCountersAcrossFunctions` /
  `aggregatedMetricsGaugeLastWins` establish aggregation; the two `concurrent*` tests
  establish lock behavior.
- `WorkflowBuilderValidationTest` — duplicate ids, unknown edge, type mismatch,
  unkeyed inbound edge to a keyed function, unresolved generics fail at build + opt-out
  passes, eager init open failure surfaces at build.
- `flink-test/src/test/java/org/flink/test/DemoFunctionsTest` and
  `flink-standalone/src/test/java/org/flink/standalone/StandaloneRunnerTest` — the
  end-to-end DAG, including `getWorkflow()` successors/kinds and clearState/metrics.

Run: `mvn -pl flink-harness -q test` (library), `mvn -q verify` (all modules).

## Debugging / investigation map

| Symptom | First inspect |
|---|---|
| element missing at sink | `routeResult` edge match (tag/keyed) / build edge list |
| `unknown source id` | `WorkflowBuilder.addSource` id vs `process` argument |
| build failure "unresolved edge type" | missing `TypeInformation` hints at registration |
| `KeyedProcessFunction … unkeyed edge` | edge was added with `addEdge`, not `addKeyedEdge` |
| intermediate output absent from result | by design — only sinks collected |
| side outputs not in result | by design — route them to a sink and read its outputs |
| counts not reset in CONTINUOUS | call `clearState`/`clearMetrics`, or use TRANSIENT |

## Related agent references

- [harnesses.md](./harnesses.md) — what `processViaEdge`/`openOnceEager` actually do.
- [state.md](./state.md) — how key binding and `clearState` interact with this loop.
- [timers.md](./timers.md) — how the OPPORTUNISTIC loop and workflow-level firing work.
- [metrics.md](./metrics.md) — the aggregation contract of `WorkflowResult`.
- [runtime-context.md](./runtime-context.md) — the context handed to each function.
- [testing.md](./testing.md) — full test inventory and platform caveats.

## External references

- Flink 2.3 DataStream process functions (lifecycle/context contract emulated here):
  https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/process_function/
- Flink 2.3 `Side Outputs`:
  https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/side_output/
