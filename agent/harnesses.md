# Harnesses & function lifecycle

## Purpose

Explains how Flink `RichFunction` instances are wrapped as graph nodes: the
`NodeHarness` contract, the `FunctionHarness` → `AbstractRichFunctionHarness`
lifecycle and key binding, the non-static `Context` construction technique, the
supported function types, and the synthetic source/sink nodes. This is where
Flink's operator lifecycle is emulated.

## Read this when

- changing how functions are opened/closed or how a runtime context is attached
- supporting a new Flink function type
- touching key binding or per-invocation output capture
- changing the source/sink base classes or JSON convenience nodes
- chasing a `ClassCastException`, an "open() failed" or a "processElement failed"
  error, or a missing output/side output

## Mental model

```
StandaloneWorkflow
   └── NodeHarness                       (only interface the workflow knows)
         ├── FunctionHarness<F>             (abstract; lifecycle + metrics)
         │     └── AbstractRichFunctionHarness<F>   (abstract; key binding + keyed state)
         │           ├── ProcessFunctionHarness
         │           ├── KeyedProcessFunctionHarness   (+ timers)
         │           ├── RichMapFunctionHarness
         │           ├── RichFlatMapFunctionHarness
         │           └── RichFilterFunctionHarness
         ├── StandaloneSource            (synthetic, subclassable)
         └── StandaloneSink              (synthetic, subclassable)
```

The hierarchy mirrors Flink's own shape (`RichFunction` → `AbstractRichFunction` →
concrete functions): lifecycle is universal, key/state management is a property of
rich functions on keyed edges.

## NodeHarness contract

`flink-harness/src/main/java/org/flink/harness/graph/function/NodeHarness.java`

- `FunctionResult<?> processElement(Object element, Edge inboundEdge)` — invoke once
  for one inbound element; `inboundEdge` carries the key selector / side tag.
- `open()` — default no-op; called eagerly at build time under `initializeAtBuild()`.
- `close()`, `resetState()`, `resetMetrics()`, `resetAll()` (resetState+resetMetrics).
- `requiresKeyedEdge()` — default `false`; `true` only for keyed functions.
- `metricsSnapshot()`, `metricGroup()` — introspection.

Public only because package visibility does not cross packages. Implement it through
the provided abstract classes.

## FunctionHarness lifecycle

`flink-harness/src/main/java/org/flink/harness/graph/function/FunctionHarness.java`

`FunctionHarness<F extends RichFunction>` holds per node: `id`, the constructor-injected
`function` (exposed via `getFunction()`), one `StandaloneRuntimeContext` (created with
the constructor-injected global job parameters), and an `opened` flag. Key/state
management lives one level down in `AbstractRichFunctionHarness`.

`processElement(element, edge)` on `AbstractRichFunctionHarness` runs in this exact
order — preserve it:

1. `bindKey(element, edge)` — if `edge != null && edge.keyed()`, cast the `KeySelector`
   to `KeySelector<Object, Object>`, call `getKey(element)`, store as `currentKey` and
   push it into the state store. A selector failure is wrapped as
   `RuntimeException("keySelector failed on edge src→dst")`.
2. `open()` — if not yet opened: `RichFunction.setRuntimeContext(runtimeContext)`
   then `RichFunction.open(OPEN_CONTEXT)` (a singleton empty `OpenContext`).
   Failure → `RuntimeException("open() failed for <id>")`.
3. `processElement(element)` — subtype-specific (protected abstract).

Consequences of that ordering:

- **Lazy open sees the first element's key already bound**, so `open()` may read
  `getRuntimeContext().getState(...)` and even the current key.
- `open()` is idempotent via the `opened` flag; the second element does not re-open.
  The same method serves the eager path (`WorkflowBuilder.initializeAtBuild()` calls
  `open()` on every node at build time) and the lazy path.
- Key selectors run before the function, so a bad selector fails the whole run with
  the edge named.

`KeyedProcessFunctionHarness.open()` (eager path): when not yet opened and no key is
bound (`currentKey() == null` — true only before the first element), it binds **a
dummy placeholder key** (`new Object()`) before delegating, so state-handle
registration in `open()` cannot hit the "no bound key" failure. The dummy key is
replaced on the first keyed element.

`close()` calls `RichFunction.close()` only when `opened`; failure → `close() failed for <id>`.
`resetState()` clears the store and nulls `currentKey`; `resetMetrics()` resets the
operator metric group counters; `resetAll()` does both (used by TRANSIENT mode).

Global job parameters arrive via the constructor (through `HarnessFactory.create`);
there is no post-construction wiring.

## The inner `Context` technique

`KeyedProcessFunctionHarness` / `ProcessFunctionHarness` create their contexts as

```java
this.context = getFunction().new Context() { … };
this.onTimerContext = getFunction().new OnTimerContext() { … };
```

This is deliberate and Flink-faithful: upstream `ProcessFunction.Context` and
`KeyedProcessFunction.Context`/`OnTimerContext` are **non-static inner classes**, so
only a function instance can instantiate them (`flink-runtime/.../streaming/api/functions/KeyedProcessFunction.java`
at tag `release-2.3.0`). Flink's own operators use the same pattern. Do not
"clean this up" into a static nested implementation — it will not compile against the
upstream class shape.

## Harness subtypes

### `ProcessFunctionHarness`

- Wraps non-keyed `ProcessFunction<Object,Object>`; `requiresKeyedEdge() == false`.
- `Context.timestamp()` returns `null`.
- `ctx.timerService()` returns a **query-only** `TimerService`:
  - `currentProcessingTime()` returns `System.currentTimeMillis()` — note it uses the
    wall clock directly, **not** the workflow `Clock`.
  - `currentWatermark()` returns `Long.MIN_VALUE`.
  - register/delete throw `UnsupportedOperationException` using
    `TimerService.UNSUPPORTED_REGISTER_TIMER_MSG` / `UNSUPPORTED_DELETE_TIMER_MSG`.
    (Flink contract: timers exist only on keyed streams.)
- Captures main outputs and side outputs per invocation; outputs are copied with
  `List.copyOf`.

### `KeyedProcessFunctionHarness`

- Wraps `KeyedProcessFunction`; `requiresKeyedEdge() == true`.
- `Context.getCurrentKey()` delegates to `AbstractRichFunctionHarness.currentKey()`.
- `OnTimerContext.timestamp()` returns the firing timestamp and `timeDomain()` is
  always `TimeDomain.PROCESSING_TIME`.
- Owns a `TimerHeap` and a `StandaloneTimerService` wired with the shared workflow
  `Clock`, `currentKey` supplier, and `allowTimerRegistration` (false in TRANSIENT).
- `fireTimer(TimerHeap.TimerEntry)` binds `entry.key()`, sets the timestamp, clears
  buffers, calls `onTimer(...)`, and returns a `FunctionResult`. Called only by the
  workflow (see `timers.md`).
- `resetState()` additionally clears the timer heap.

### `RichMapFunctionHarness` / `RichFlatMapFunctionHarness` / `RichFilterFunctionHarness`

One harness per rich single-IO function type (no shared `Kind` switch):

- `RichMapFunctionHarness`: `map(element)`; a `null` return produces no output.
- `RichFlatMapFunctionHarness`: `flatMap(element, collector)`.
- `RichFilterFunctionHarness`: element is passed through unchanged when
  `filter(element)` is true, otherwise dropped.
- No side outputs (these interfaces have no `output`).
- `requiresKeyedEdge() == false`, **but** keyed state still works if the incoming edge
  is keyed, because `bindKey` is inherited from `AbstractRichFunctionHarness`: any
  rich function on a keyed edge can use `getRuntimeContext().getState(...)`
  (Flink-faithful — `StreamingRuntimeContext` only rejects state on non-keyed
  streams). `requiresKeyedEdge` is only a build-time requirement, not a capability
  gate. Proved by `RichFunctionHarnessesTest.mapUsesKeyedStateOnKeyedEdge`.

### `HarnessFactory`

`flink-harness/src/main/java/org/flink/harness/graph/function/HarnessFactory.java`
`create(id, function, clock, mode, globalJobParameters)` dispatches by `instanceof`
in this order: `ProcessFunction`, `KeyedProcessFunction`, `RichMapFunction`,
`RichFlatMapFunction`, `RichFilterFunction`; `allowTimerRegistration` is
`mode == Mode.CONTINUOUS`. Everything a harness needs (function, clock, params) is
passed into its constructor here. Anything else that is a `RichFunction`, and
anything not a `RichFunction`, throws `IllegalArgumentException` naming the class.
New supported types are added here.

## Synthetic nodes

### `StandaloneSource`

`flink-harness/src/main/java/org/flink/harness/graph/source/StandaloneSource.java`

- Subclass and override `protected void process(IN element, Collector<Object> out)`;
  default emits the element unchanged.
- `init()` / `dispose()` are lifecycle hooks called once on first input / at close.
- `getMetricGroup()` exposes a per-instance `StandaloneOperatorMetricGroup` for
  subclass metrics — the only way custom nodes report metrics.
- `processElement` casts the element to `IN`, calls `process`, and returns the recorded
  outputs. Failure → `RuntimeException("StandaloneSource process failed")`.
- `open()` just runs `init()` once (flag-guarded).
- `requiresKeyedEdge()` is inherited `false`; sources cannot be keyed destinations
  (topology forbids inbound edges).

### `StandaloneSink`

Same structure, overriding `protected void accept(IN element, Collector<Object> collected)`.
Default records the element. Do not call `collect` to drop an element. `JsonSink`
extends it.

### `JsonSource` / `JsonSink`

- `JsonSource<T>(Class<T>[, ObjectMapper])` parses each `String` element to `T`;
  parse failure → `RuntimeException` wrapping the cause.
- `JsonSink<IN>(boolean prettyPrint[, ObjectMapper])` serializes to JSON strings
  recorded as sink outputs; failure → `RuntimeException`.
- Jackson is `provided` scope; consumers must supply it.

### `RecordingCollector`

`flink-harness/src/main/java/org/flink/harness/graph/RecordingCollector.java` — the
trivial `Collector<T>` appending to a backing list. Harnesses reuse one instance per
node and `clear()` the backing list before each invocation.

## Invariants and contracts

- Lifecycle order is `setRuntimeContext` → `open` → per-element invocations → `close`.
- `open` runs at most once per node; lazy first-element path has the real key bound,
  eager path has a dummy key (keyed functions only).
- Per-invocation output buffers are cleared before every `processElement`/`onTimer`;
  the returned lists are immutable copies.
- A source/sink subclass must not assume its `init()`/`dispose()` runs more than once.
- Only `KeyedProcessFunctionHarness` requires a keyed edge; all
  `AbstractRichFunctionHarness` subtypes can *use* keyed state on a keyed edge.
- The contexts must be created via the function instance's inner classes.

## Important implementation patterns

- Add capabilities to `FunctionHarness`/`AbstractRichFunctionHarness` and select them
  in `HarnessFactory`; do not special-case concrete harness types in `WorkflowBuilder`.
- Wrap checked exceptions with the node id and operation ("open() failed for",
  "processElement failed in", "onTimer failed in") so failures are attributable.
- Keep generic casts inside the subtype constructors; the public `processElement`
  stays typed.
- Subclassable nodes expose lifecycle hooks and a metric group, not internal buffers.

## Raw-cast confinement policy

Unchecked casts are allowed only at narrow boundary helpers, each marked
`@SuppressWarnings("unchecked")`:

1. `AbstractRichFunctionHarness.bindKey` — `KeySelector` invocation.
2. `KeyedProcessFunctionHarness` / `ProcessFunctionHarness` / `RichMapFunctionHarness`
   / `RichFlatMapFunctionHarness` / `RichFilterFunctionHarness` constructors —
   wildcard-function to typed-function casts.
3. `StandaloneSource.processElement` / `StandaloneSink.processElement` — element cast
   to `IN`.
4. `WorkflowResult.sideOutputsOf` — side-output list cast.
5. `org.flink.harness.state` accessors (`InMemoryKeyedStateStore`, `InMemoryStateV2`)
   and `CompletedStateFuture.resolve` — state value casts.

Keep new unchecked casts out of `WorkflowBuilder`, `StandaloneWorkflow`, and the
public API layer.

## Deliberate differences from Flink

- No `StreamOperator`/`StreamTask`; there is no `initializeState`, no checkpoint
  hooks, no processing-time service, no mailbox.
- `Context`/`OnTimerContext` are user-function inner classes, not `ContextImpl`
  operator classes; `timestamp()` is `null` in `processElement` (no event time).
- `open(OpenContext)` is called with an empty singleton; `open()` in Flink may receive
  richer context in future versions.
- No object reuse, no serializer setup on the function; `setRuntimeContext` is the
  only wiring.
- `close()` is not followed by any cleanup/job-finalization; metric snapshots are
  taken before close.

## Common pitfalls / agent traps

- **Reordering `bindKey` and `open`.** That breaks `open()`-registered state and
  the eager dummy-key guarantee.
- **Making `Context` static.** Upstream declares them as inner classes.
- **Assuming `ProcessFunctionHarness` honors the workflow `Clock`** for
  `currentProcessingTime()`. It calls the system clock.
- **Assuming the rich map/flatmap/filter harnesses are never keyed.** They can be,
  and then they can use keyed state.
- **Assuming side outputs are available on all function types.** Only
  `ProcessFunction`/`KeyedProcessFunction` expose `ctx.output`.
- **Adding a function type without a `HarnessFactory` branch** — build fails.
- **Returning the mutable backing list** from a harness; always `List.copyOf`.
- **Forgetting Jackson is `provided`** when using `JsonSource`/`JsonSink` in the library.

## Testing and verification

- `flink-harness/.../functions/KeyedProcessFunctionHarnessTest` —
  `statePerKeyAndCurrentKeyExposure` proves per-key state + key exposure;
  `unkeyedEdgeFailsLoudlyAtRuntime` proves a keyed function with no keyed edge fails.
- `flink-harness/.../functions/ProcessFunctionHarnessTest` — main + side output
  capture, metric snapshot, `resetAll`.
- `flink-harness/.../functions/RichFunctionHarnessesTest` — map null-drop, flatMap
  collector, filter pass/drop, and keyed state in a `RichMapFunction` on a keyed edge.
- `flink-test/.../DemoFunctionsTest` — exercises `RichMapFunctionHarness`,
  `ProcessFunctionHarness`, `KeyedProcessFunctionHarness` and a full workflow,
  including `parse`/`route`/`accum`/`report`.
- `flink-harness/.../source/JsonSourceTest` and `.../sink/JsonSinkTest` — JSON
  conversion and error wrapping.

## Debugging / investigation map

| Symptom | First inspect |
|---|---|
| `open() failed for <id>` | the function's `open`, `FunctionHarness.open` |
| `processElement failed in <id>` | function body; cause is chained |
| `keySelector failed on edge a→b` | `AbstractRichFunctionHarness.bindKey`, the `KeySelector` |
| state "no bound key" | edge not keyed, or state accessed outside invocation |
| side output lost | no matching side-output edge, or function lacks `ctx.output` |
| `UnsupportedOperationException` on timers in a non-keyed function | by design — use `KeyedProcessFunction` |
| `ClassCastException` at an edge | unresolved types built with `build(true)` |

## Related agent references

- [workflow.md](./workflow.md) — how harnesses are wired into the run loop and how
  their results are routed/aggregated.
- [runtime-context.md](./runtime-context.md) — the `RuntimeContext` every harness
  supplies to its function.
- [state.md](./state.md) — the store `AbstractRichFunctionHarness` binds keys into.
- [timers.md](./timers.md) — `KeyedProcessFunctionHarness`'s `TimerHeap`/service.
- [metrics.md](./metrics.md) — the metric group exposed by functions and nodes.
- [testing.md](./testing.md) — fixture inventory.

## External references

- Flink 2.3 `KeyedProcessFunction` (source, tag `release-2.3.0`):
  https://raw.githubusercontent.com/apache/flink/release-2.3.0/flink-runtime/src/main/java/org/apache/flink/streaming/api/functions/KeyedProcessFunction.java
  — establishes that `Context`/`OnTimerContext` are non-static inner classes.
- Flink 2.3 `RuntimeContext`:
  https://raw.githubusercontent.com/apache/flink/release-2.3.0/flink-core/src/main/java/org/apache/flink/api/common/functions/RuntimeContext.java
- Flink 2.3 `StreamingRuntimeContext` (keyed state only rejected on non-keyed streams):
  https://raw.githubusercontent.com/apache/flink/release-2.3.0/flink-runtime/src/main/java/org/apache/flink/streaming/api/operators/StreamingRuntimeContext.java
- Flink 2.3 docs, process functions:
  https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/process_function/
