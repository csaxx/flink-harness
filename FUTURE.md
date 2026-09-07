# FUTURE.md — candidate features beyond v1

Scope: common, often-used Flink features that can be integrated into standalone
execution **without bloating the implementation**. Each entry: motivation, API
sketch, implementation notes, estimated cost, test plan.

## Global design directive (2026-09-03, updated 2026-09-07)

| Mode | Semantics for all future features |
|---|---|
| CONTINUOUS | Replicate a real, running Flink job. Processing time = wall clock (or pluggable Clock). Processing-time timers supported via three modes (OPPORTUNISTIC / MANUAL / BACKGROUND). Watermarks advance as data flows (future work). State/metrics persist across `process()` calls. |
| TRANSIENT | One-shot batch semantics. No timer support — `register*` / `delete*` throw `UnsupportedOperationException`. Time queries (`currentProcessingTime`, `currentWatermark`, `timestamp`) still return sane values. Everything reset after each `process()` (unchanged). |

All features below must respect the existing constraints: thread-safe via the
single workflow lock, raw casts confined to boundary helpers, fail loudly at
`build()`, no runtime-class instantiation, parallelism-1.

---

## 1. Event-time timers + watermarks + processTimestamped

**Motivation.** Completion of the timer story: event-time timers, watermark advancement,
and timestamped input API. Processing-time timers are already implemented (see AGENTS.md).

**Remaining work items:**

1. **Event-time timers + watermarks**: `advanceWatermark(long wm)`, auto-advance
   from element timestamps, `TimestampedValue` input, `registerEventTimeTimer` /
   `deleteEventTimeTimer` + `advanceWatermark` triggering firings.
2. **`processTimestamped(List<TimestampedValue<T>>, sourceId)`**: timestamped
   input API so `ctx.timestamp()` returns element timestamps.
3. **`ctx.timestamp()` in processElement**: currently returns null; with
   timestamped input support it would return the element's timestamp.
4. **Deterministic timer tests with ManualClock**: OPPORTUNISTIC / MANUAL modes
   already use builder.clock(Clock). BACKGROUND mode with ManualClock is limited
   by real wall-clock polling (~100ms resolution). Low priority — BACKGROUND
   with SystemClock works for integration tests.

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
`timestamp` — trivial once processing-time timers exist (null/defaults otherwise).

**Cost.** ~100 lines. **Tests.** open/close lifecycle, metrics in sink,
elements recorded, watermark visible in context.

---

## 5. CheckpointedFunction / operator state (optional, lowest priority)

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
| Windows (assigners/triggers/evictors) | A runtime of its own; users can emulate most window logic with keyed state + timers (see AGENTS.md) |
| Async I/O | Needs the runtime's async executor/waiter machinery |
| FLIP-27 Source/Sink interfaces | SplitEnumerator/Reader machinery; feeding via `process()` *is* the source; §4 covers sinks |
| State TTL (`StateTtlConfig`) | Subtle semantics; revisit after §1 (needs time infrastructure anyway) |
| Accumulators | Permanently out of scope — superseded by metrics |
| Broadcast variables / distributed cache | DataSet legacy |
| Feedback iterations | Queue tolerates cycles already, but no termination guard — not a feature to formalize |
| Parallelism > 1 | Violates frozen design decision (parallelism-1 semantics) |

## Suggested sequencing

1. §1 event-time / watermarks / processTimestamped (remaining future work)
2. §2 CoProcessFunction, §3 broadcast (topology coverage)
3. §4 sink (completeness polish)
4. §5 CheckpointedFunction (only on demand)

Each step: update AGENTS.md scope table, add tests,
`mvn -q verify`.

## Implemented (formerly out of scope — 2026-09-07)

### v2 state API (`org.apache.flink.api.common.state.v2.*`)

**Decision (2026-09-07).** Removed from out-of-scope and implemented in full:
all 5 state kinds (Value/List/Map/Reducing/Aggregating), sync + async methods,
eager `StateFuture`/`StateIterator` (continuations run immediately on the caller
thread), isolated v2 namespace in the same in-memory per-key store, TTL-enabled
descriptors rejected with UOE. ~500 LoC total. Rationale: interfaces live in
`flink-core-api` (already on the compile classpath transitively via `flink-core`),
descriptors reuse v1 `ReduceFunction`/`AggregateFunction`, and the eager-future
model is a faithful deterministic emulation for a synchronous harness.