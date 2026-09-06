# FUTURE.md — candidate features beyond v1

Scope: common, often-used Flink features that can be integrated into standalone
execution **without bloating the implementation**. Each entry: motivation, API
sketch, implementation notes, estimated cost, test plan.

## Global design directive (2026-09-03)

| Mode | Semantics for all future features |
|---|---|
| CONTINUOUS | Replicate a real, running Flink job. Assume an external tool loads the workflow and feeds data continuously. Processing time = wall clock. Watermarks advance as data flows. Timers/state/metrics persist across `process()` calls. |
| TRANSIENT | One-shot batch semantics. **No timer support** — `timerService()` registration methods throw `UnsupportedOperationException`. Time queries (`currentProcessingTime`, `currentWatermark`, `timestamp`) still return sane values. Everything reset after each `process()` (unchanged). |

All features below must respect the existing constraints: thread-safe via the
single workflow lock, raw casts confined to boundary helpers, fail loudly at
`build()`, no runtime-class instantiation, parallelism-1.

---

## 1. Timers + time (keyed) — headline feature

**Motivation.** The most common gap: real-world `KeyedProcessFunction`s use
timers for session timeouts, buffering windows, dedup TTLs, rate limiting.
Flink itself restricts timers to keyed streams
(`TimerService.UNSUPPORTED_REGISTER_TIMER_MSG`), so keyed-only support is
faithful to Flink, not a compromise.

**API surface.**
- `process()` inputs gain optional timestamps:
  `processTimestamped(List<TimestampedValue<T>> inputs, String sourceId)` with
  `TimestampedValue<T>(T value, long timestamp)`; plain `process()` = no
  timestamps (`ctx.timestamp()` → null, unchanged behavior).
- Watermark advancement on `StandaloneWorkflow` (CONTINUOUS only):
  - `advanceWatermark(long wm)` — fires all due event-time timers across all
    keyed functions, in timestamp order, routing their outputs into the graph.
  - Default watermark = max-seen element timestamp (auto-advance after each
    element), matching `IngestionTimeWatermarkStrategy`-ish simplicity; explicit
    `advanceWatermark` overrides for late-data testing.
- Processing time = wall clock (`System.currentTimeMillis()`). Due
  processing-time timers are fired:
  - after each element invocation (cheap check: `while (heap.peek() <= now)`),
  - at the end of every `process()` call,
  - so an external feeder that calls `process()` repeatedly sees timers fire
    between batches exactly like a live job that is idle between records.
  - No background firing thread: firing only happens inside the workflow lock
    during `process()`/`advance*()` calls. Documented as a deliberate
    standalone semantic (a job with no input is quiescent; firing is
    observable at the next interaction).

**Implementation.**
- `internal/StandaloneTimerService` — implements the 6 `TimerService` methods.
  Timer heap per keyed harness: `PriorityQueue<(timestamp, key, TimeDomain)>`,
  dedup on (key, timestamp, domain) — Flink registers one timer per key+ts;
  `delete*Timer` removes. `currentProcessingTime()` = wall clock,
  `currentWatermark()` = workflow watermark.
- `KeyedProcessFunctionHarness`: instantiate `OnTimerContext` via the same
  non-static-inner-class-through-function-instance pattern already used for
  `Context`; add `onTimer(timestamp, TimeDomain)` entry point that binds the
  timer's key (reuse `stateStore.setCurrentKey`) before invoking
  `function.onTimer(...)`, collects main + side outputs into a
  `FunctionResult` exactly like `processElement`.
- `StandaloneWorkflow.doProcess`: the BFS loop gains a timer-firing step —
  after draining (or interleaved before polling the next element when a timer
  is due earlier than the queued elements' timestamps in event time), pop due
  timers, invoke `onTimer`, enqueue their outputs. Queue entries carry
  timestamps so event-time ordering stays deterministic.
- Timer registrations live in harness state → cleared by `resetAll()` /
  `clearState()` in CONTINUOUS management APIs; in TRANSIENT the
  `TimerService` register/delete methods throw UOE (query methods still work).

**Cost.** ~350 lines incl. tests. **Touches:** new `StandaloneTimerService`,
`TimestampedValue`, `KeyedProcessFunctionHarness`, `StandaloneWorkflow`,
`WorkflowBuilder` (mode validation), AGENTS.md scope table.

**Tests.** processing-time timer fires after element; event-time timer fires
on `advanceWatermark`; dedup (same key+ts registered twice fires once);
delete; onTimer output routes downstream; timer side outputs; key isolation
(two keys, same timestamp); TRANSIENT → UOE on register; CONTINUOUS → timer
survives across `process()` calls and fires in a later call. Note: entry point
parameter changed from `functionId` to `sourceId` in v1 (2026-09-03); the
timer feature uses `sourceId` consistently.

---

## 2. CoProcessFunction / connected streams

**Motivation.** Two-input joins and stream enrichment are ubiquitous.
`CoProcessFunction` (non-keyed) and `KeyedCoProcessFunction` cover
`connect()` topologies.

**API surface.**
- `registerCoFunction(id, fn[, inType1, inType2, outType])`.
- Edges carry an input index: `addEdge(src, dst, 1|2)` /
  `addKeyedEdge(src, dst, 1|2, keySelector)` (existing single-arg overloads
  keep meaning "the only input").
- Build validation: a co-function must have exactly one inbound edge per
  input index; type checks per input.

**Implementation.** `CoProcessFunctionHarness` — `Context`/`OnTimerContext`
via the inner-class pattern; `Invocation` record gains `inputNum`; dispatch to
`processElement1/2`. If timers (§1) landed first, `KeyedCoProcessFunction`
gets timers for free via the same `StandaloneTimerService`. Side outputs and
metrics unchanged.

**Cost.** ~250 lines. **Tests.** two-input routing, per-input types, keyed
co-function with per-input key selectors, build failure on missing/duplicate
input edge, timer interplay.

---

## 3. BroadcastProcessFunction + BroadcastState

**Motivation.** The dynamic-rules / dynamic-config pattern is one of the most
common production topologies (rules stream broadcast to all parallel
instances; data stream reads rules). `BroadcastState` semantics = non-keyed
`MapState` — the existing `InMemoryMapState` is directly reusable.

**API surface.**
- `registerBroadcastFunction(id, BroadcastProcessFunction[, types])`.
- `addBroadcastEdge(src, dst)` — routes to `processBroadcastElement`;
  regular `addEdge` routes to `processElement`.
- Keyed variant (`KeyedBroadcastProcessFunction`) only if keyed timers exist.

**Implementation.**
- Harness holds `Map<String, Map<?,?>>` per registered
  `MapStateDescriptor` name (operator-state-like, unkeyed). `Context` exposes
  read/write `BroadcastState`, `ReadOnlyContext` exposes
  `ReadOnlyBroadcastState` — both thin wrappers over the same backing map
  (read-only variant wraps mutators with UOE).
- **Ordering semantics (must document):** BFS processing means a broadcast
  element only affects data elements processed after it. Feeding tools should
  send broadcast input first. Optional cheap improvement: broadcast invocations
  go to the front of the queue (priority over data invocations within one
  `process()` batch) — mirroring Flink's "broadcast side is processed with
  priority on input availability" closely enough for offline runs.
- CONTINUOUS: broadcast state persists across `process()` calls (like a
  running job). TRANSIENT: cleared with the rest.

**Cost.** ~300 lines. **Tests.** rules-update-then-data-flows,
read-only enforcement on data side, queue-priority ordering, persistence
across calls in CONTINUOUS, reset in TRANSIENT.

---

## 4. RichSinkFunction harness

**Motivation.** Real jobs end in sinks with `open/close`, metrics, and
occasionally two-phase-commit-style logic. A sink harness lets existing
production sinks run unmodified. Since v1 (2026-09-03) `StandaloneSink`
provides the graph terminal; a `RichSinkFunction` adapter could plug a Flink
sink function into the sink node.

**API surface.** `addSink(id, new RichSinkAdapter<>(fn))` where the adapter
extends `StandaloneSink` and wraps `RichSinkFunction`. Consumed elements are
still recorded in `FunctionResult.outputs` (so tests assert on them);
`SinkFunction.Context` = `currentProcessingTime`, `currentWatermark`,
`timestamp` — trivial once §1 exists (null/defaults otherwise).

**Cost.** ~100 lines. **Tests.** open/close lifecycle, metrics in sink,
elements recorded, watermark visible in context.

---

## 5. Cheap RuntimeContext fillers

| Method | Implementation | Cost |
|---|---|---|
| `getGlobalJobParameters()` | `Map<String,String>` supplied via `WorkflowBuilder.globalJobParameters(map)`, immutable | ~15 lines |
| `createSerializer(TypeInformation)` | `typeInfo.createSerializer(new ExecutionConfig())` — works standalone, no runtime classes | ~3 lines |
| accumulators | **Keep UOE** — superseded by metrics; supporting them adds a parallel, redundant metrics universe | — |

---

## 6. CheckpointedFunction / operator state (optional, lowest priority)

**Motivation.** Many library functions implement `CheckpointedFunction` and
currently can't run at all. Parallelism-1 makes operator list state and union
list state identical, so this is cheap.

**Implementation.** `FunctionInitializationContext` backed by an in-memory
`OperatorStateStore` (list/union-list/broadcast state) + `KeyedStateStore`
delegating to `InMemoryKeyedStateStore`. `initializeState` called during
`openOnce`. `snapshotState` invoked via explicit
`StandaloneWorkflow.snapshotState()` hook (no persistence — returns per-function
success; lets tests exercise snapshot logic). No savepoint serialization —
documented.

**Cost.** ~300 lines. Defer unless a concrete consumer needs it.

---

## Explicitly out of scope (bloat without payoff)

| Feature | Reason |
|---|---|
| Windows (assigners/triggers/evictors) | A runtime of its own; users can emulate most window logic with keyed state + timers (§1) |
| Async I/O | Needs the runtime's async executor/waiter machinery |
| FLIP-27 Source/Sink interfaces | SplitEnumerator/Reader machinery; feeding via `process()` *is* the source; §4 covers sinks |
| State TTL (`StateTtlConfig`) | Subtle semantics; revisit after §1 (needs time infrastructure anyway) |
| v2 state API (`state.v2.*`) | Experimental in 2.3; keep UOE |
| Accumulators | Superseded by metrics |
| Broadcast variables / distributed cache | DataSet legacy |
| Feedback iterations | Queue tolerates cycles already, but no termination guard — not a feature to formalize |
| Parallelism > 1 | Violates frozen design decision (parallelism-1 semantics) |

## Suggested sequencing

1. §1 timers + time (unlocks the most real-world functions)
2. §2 CoProcessFunction, §3 broadcast (topology coverage)
3. §4 sink + §5 fillers (completeness polish)
4. §6 CheckpointedFunction (only on demand)

Each step: update AGENTS.md scope table + WORKING.md decision log, add tests,
`mvn -q verify`.