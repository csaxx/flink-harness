# Timers (processing time)

## Purpose

Documents the processing-time timer implementation: the per-keyed-node `TimerHeap`,
the keyed-only `StandaloneTimerService`, the three firing modes, the background timer
thread, and how timers interact with modes and clearing. Event time and watermarks do
not exist.

## Read this when

- changing timer registration/deletion, dedup, or firing order
- adding or changing a `ProcessingTimerMode`
- touching `BackgroundTimerThread`/`BackgroundTimerListener` or workflow locking
- debugging "my timer never fires" / "my timer fires at the wrong time"
- considering event-time support (`FUTURE.md` §1)

## Mental model

```
One per KeyedProcessFunctionHarness          One per StandaloneWorkflow
─────────────────────────────────            ───────────────────────────
TimerHeap (PriorityQueue + active map)  ──▶  WorkflowTimerService.fireProcessingTimers()
StandaloneTimerService (Clock, heap)          BackgroundTimerThread (BACKGROUND mode)
   used by ctx.timerService()                    └─ polls, locks workflow, fires
```

Timers are keyed-only, faithful to Flink (`KeyedProcessFunction` only). A non-keyed
`ProcessFunction` receives a query-only `TimerService` (`harnesses.md`).

Files:
`flink-harness/src/main/java/org/flink/harness/timer/TimerHeap.java`,
`StandaloneTimerService.java`, `WorkflowTimerService.java`, `BackgroundTimerThread.java`,
`BackgroundTimerListener.java`, `ProcessingTimerMode.java`.

## TimerHeap mechanics

- `TimerEntry { timestamp, seq, key, cancelled }`; `TimerKey { key, timestamp }`.
- A `PriorityQueue<TimerEntry>` orders by `(timestamp, seq)` — insertion order breaks
  ties deterministically.
- An `active` HashMap (`TimerKey → TimerEntry`) enforces **one timer per `(key, timestamp)`**:
  `tryRegister` returns `false` and does nothing if the pair already exists.
- `tryDelete(key, ts)` removes from `active`, marks the entry `cancelled` (lazy delete),
  and returns whether it existed; deleting a non-existent timer is a silent `false`.
- `clean()` pops cancelled entries from the queue head lazily; `pollDue(now)` cleans,
  then removes and returns the earliest entry with `timestamp <= now`, or `null`.
- `peekDue(now)` does not remove; `size()`/`isEmpty()` reflect the `active` map;
  `earliestTimestamp()` returns `Long.MAX_VALUE` when empty; `clear()` drops everything.
- Timers are per-node: two keyed nodes have independent heaps.

## StandaloneTimerService

Implements `org.apache.flink.streaming.api.TimerService`:

| Method | Behavior |
|---|---|
| `currentProcessingTime()` | `clock.absoluteTimeMillis()` — honors `WorkflowBuilder.clock(Clock)`. |
| `currentWatermark()` | `Long.MIN_VALUE` always. |
| `registerProcessingTimeTimer(ts)` | If `!keyed \|\| !allowRegister` → `UnsupportedOperationException(UNSUPPORTED_REGISTER_TIMER_MSG)`; else `heap.tryRegister(currentKey.get(), ts)`. Duplicate `(key, ts)` is a no-op. |
| `deleteProcessingTimeTimer(ts)` | Same guard; else `heap.tryDelete(currentKey.get(), ts)`; absent → silent no-op. |
| `registerEventTimeTimer(ts)` | Always UOE ("event-time timers are not supported yet — future work"). |
| `deleteEventTimeTimer(ts)` | Always UOE. |

`allowRegister` is `mode == CONTINUOUS` (from `HarnessFactory`). So in TRANSIENT mode
*registration and deletion* throw, while `currentProcessingTime()`/`currentWatermark()`
still work.

## Firing modes

`ProcessingTimerMode` is chosen at build and is exclusive. TRANSIENT requires the
default `OPPORTUNISTIC`; any other mode fails at `build()`.

| Mode | Mechanism |
|---|---|
| `OPPORTUNISTIC` (default) | The run loop fires due timers after **each element invocation** and again after the queue drains. `fireProcessingTimers()` also works as an explicit nudge. |
| `MANUAL` | Nothing fires automatically. The only path is `workflow.getTimerService().fireProcessingTimers()`. |
| `BACKGROUND` | A single daemon thread per workflow polls every ~100 ms, acquires the workflow lock, fires due timers, and delivers results/errors via the `BackgroundTimerListener`. `fireProcessingTimers()` also works. |

Firing path (shared): `fireDueTimers(now, queue)` iterates every
`KeyedProcessFunctionHarness`, `pollDue(now)` repeatedly, calls `harness.fireTimer(entry)`,
and routes the returned `FunctionResult` into the same BFS queue. `fireTimer` binds the
timer's key, records `onTimerTimestamp`, and invokes `onTimer(ts, onTimerContext, collector)`.
Because the fired outputs are routed like ordinary element outputs, **timers can trigger
downstream functions and sinks**.

Inside `onTimer`, the timer service is the same instance, so registering further timers
(chaining) or deleting timers works; the key is the timer's key (proved by
`timerCanRegisterAnotherTimerFromOnTimer`, `timerCorrectlyBindsKeyAndState`).

## Background timer thread

`BackgroundTimerThread`:

- A daemon `Thread` named `flink-harness-bg-timer`; `POLL_INTERVAL_MS = 100`.
- `State`: `RUNNING`, `FAILED`, `SHUTDOWN`.
- Loop: `action.fireDue()` (which locks the workflow), then `listener.onResult(result)`
  if non-null, then waits up to 100 ms; `notifyWake()` shortens the wait.
- Any `Exception`/`Throwable` sets `failure`, transitions to `FAILED`, calls
  `listener.onError("", t)`, and exits the loop.
- `close()` clears `running`, wakes the thread, and joins up to 2 s; interrupts on timeout.
- The listener is invoked **outside** the workflow lock (the lock is released in
  `finally` before `onResult`), so a listener may call back into the workflow without
  deadlocking.

On failure, `StandaloneWorkflow.checkFailed()` (called at the start of every locked
operation) rethrows a `RuntimeException("Background timer thread failed with: …")`.
This **permanently poisons** the workflow instance; subsequent `process()`/fire/clear
calls throw. Construct a new workflow to recover.

## WorkflowTimerService (public timer API)

`workflow.getTimerService()`:

- `WorkflowResult fireProcessingTimers()` — fires all currently due timers across all
  keyed nodes under the workflow lock; returns the aggregated result.
- `long pendingTimerCount()` — sum of `timerHeapSize()` across keyed nodes (counts
  active registrations, excluding lazily-cancelled entries).

## Interaction with state/modes

- `StandaloneWorkflow.clearState(nodeId)` / `clearStateAll()` → for keyed nodes this
  calls `KeyedProcessFunctionHarness.clearState()`, which clears the heap as well as
  keyed state (proved by `clearStateAlsoClearsTimers`).
- `Mode.TRANSIENT` resets everything (state + metrics + heap) after each `process()`;
  timers cannot be registered in the first place.
- CONTINUOUS leaves pending timers across `process()` calls.

## Invariants and contracts

- Timers are keyed-only; registration in a non-keyed or TRANSIENT context must throw UOE.
- One timer per `(key, timestamp)` per node; re-registration is a no-op.
- Deletion is best-effort and idempotent (silent when absent).
- `currentWatermark()` is always `Long.MIN_VALUE`; event-time methods are UOE.
- `onTimer` runs with the timer's key bound, so keyed state access inside it is correct.
- Background firing must hold the workflow lock; the listener must run after release.
- A background failure must be surfaced on the next locked operation, not swallowed.
- Fired outputs must be routed through the same BFS path as element outputs.

## Important implementation patterns

- Add firing behavior by extending the mode handling in `StandaloneWorkflow`
  (`doProcess`/`drainBfsQueueOpportunistic`/`fireProcessingTimersInternal`), reusing
  `fireDueTimers`.
- Keep timer state inside `TimerHeap`; the harness owns one heap and one service.
- Prefer lazy deletion (`cancelled` flag) over list removal to keep O(log n) polling.
- Wrap `onTimer` failures with the node id ("onTimer failed in …").

## Deliberate differences from Flink

- Flink's `InternalTimerServiceImpl` supports event-time and processing-time timers and
  is checkpointed; this heap holds only processing-time timers and is never snapshotted.
- Dedup and ordering are faithful (one timer per key+timestamp, timestamp-then-insertion
  order), but firing is driven by the harness (wall clock / explicit call / poll thread),
  not by a per-task timer service coordinating with watermarks.
- `currentWatermark()` does not advance; there is no watermark/event-time infrastructure.
- Event-time timers are explicitly future work (`FUTURE.md` §1).

## Common pitfalls / agent traps

- **Registering timers in TRANSIENT mode** — throws UOE by design; the build also
  rejects non-default modes.
- **Assuming `ProcessFunctionHarness.currentProcessingTime()` uses the workflow Clock** —
  it uses `System.currentTimeMillis()`; only the keyed service honors the `Clock`.
- **Forgetting that OPPORTUNISTIC fires after every element** — a timer scheduled for a
  past timestamp fires immediately after the registering element.
- **Blocking inside a `BackgroundTimerListener`** — it holds no workflow lock, so it is
  safe, but slow listeners delay the single poll loop.
- **Reusing a workflow after a background failure** — it stays poisoned; create a new one.
- **Expecting timers to survive a new JVM / checkpoint** — nothing is persisted.
- **Counting cancelled timers with `size()`** — `size()` uses the active map, so it is
  accurate after `clean()`; `pendingTimerCount()` reflects active registrations.

## Testing and verification

`flink-harness/src/test/java/org/flink/harness/timer/`:

- `TimerHeapTest` — ordering by timestamp then insertion, `(key,ts)` dedup, delete,
  delete+re-register, cancelled-entry skipping, clear, peek, earliest timestamp.
- `KeyedTimerIntegrationTest`:
  - `processingTimerFiresAfterElement` / `processingTimerDoesNotFireBeforeTimestamp`
  - `onTimerCanProduceSideOutputsAndMainOutputs`
  - `timerCorrectlyBindsKeyAndState`
  - `timerCanRegisterAnotherTimerFromOnTimer`
  - `manualModeTimersOnlyFireOnExplicitCall`,
    `manualFireProcessingTimersReturnsEmptyWhenNoTimersDue`
  - `backgroundModeDeliversFiredTimersViaListener`,
    `backgroundModeDeliversErrorOnTimerFailure` (timing-sensitive; use sleeps)
  - `transientModeRegisterProcessingTimeTimerThrowsUoe`,
    `transientModeWithExplicitTimerModeFailsAtBuild`
  - `pendingTimerCountIsCorrectAcrossModes`, `clearStateAlsoClearsTimers`

Background tests depend on real wall-clock polling (~100 ms) and `Thread.sleep`; keep
that in mind when running under load. Deterministic tests for OPPORTUNISTIC/MANUAL use
`WorkflowBuilder.clock(...)`.

## Debugging / investigation map

| Symptom | First inspect |
|---|---|
| timer never fires (OPPORTUNISTIC) | timestamp vs `clock.absoluteTimeMillis()`; registration guard |
| timer never fires (MANUAL) | call `getTimerService().fireProcessingTimers()` |
| `UnsupportedOperationException` on register | non-keyed function or TRANSIENT mode |
| `onTimer` sees wrong key/state | `fireTimer` key binding; the timer's `TimerKey` |
| workflow suddenly throws "Background timer thread failed" | `BackgroundTimerThread` state/failure, listener `onError` |
| `pendingTimerCount` not dropping | timers future-dated; `fireProcessingTimers` fires only due timers |

## Related agent references

- [workflow.md](./workflow.md) — where firing is invoked in the run loop and lock.
- [harnesses.md](./harnesses.md) — `KeyedProcessFunctionHarness` owns the heap/service.
- [state.md](./state.md) — keyed state correctness inside `onTimer`.
- [metrics.md](./metrics.md) — metrics produced by timer firings are aggregated like any run.
- [testing.md](./testing.md) — timing caveats and test inventory.

## External references

- Flink 2.3 `TimerService` (source, tag `release-2.3.0`) — interface constants reused
  verbatim, and the “keyed streams only” contract:
  https://raw.githubusercontent.com/apache/flink/release-2.3.0/flink-runtime/src/main/java/org/apache/flink/streaming/api/TimerService.java
- Flink 2.3 docs, timers in process functions:
  https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/process_function/
- Upstream internal timer service (semantic baseline, not used):
  `flink-runtime/src/main/java/org/apache/flink/streaming/api/operators/InternalTimerServiceImpl.java`
