# Keyed state

## Purpose

Documents the only state implementation: an in-memory, per-key, per-node map store
supporting both Flink's v1 and v2 keyed-state APIs. There is no state backend, no
serialization, and no checkpointing. If a change touches `getState` / state scoping /
clearing / TTL, it belongs here.

## Read this when

- changing keyed state behavior or adding a state kind
- debugging per-key isolation or "no bound key" failures
- touching v2 `StateFuture` / `StateIterator` semantics
- changing `clearState`/TRANSIENT behavior as it affects state
- deciding what to do about State TTL

## Mental model

```
FunctionHarness (one per node)
   └── StandaloneRuntimeContext
         └── InMemoryKeyedStateStore              one per node instance
               Map<Object key, Map<String stateName, Object value>>
                 stateName for v2 = "v2:" + descriptor.getStateId()   (namespace isolation)
```

Every state accessor is a thin handle resolving the current key's map **on each
operation** through a `Supplier<Map<String,Object>> backing`. That lazy resolution is
what makes per-key isolation correct while a single handle instance is retained in the
user function.

`flink-harness/src/main/java/org/flink/harness/state/InMemoryKeyedStateStore.java`
`flink-harness/src/main/java/org/flink/harness/state/InMemoryStateV2.java`
`flink-harness/src/main/java/org/flink/harness/state/CompletedStateFuture.java`
`flink-harness/src/main/java/org/flink/harness/state/CollectionStateIterator.java`

## Scoping and key binding

- `setCurrentKey(Object)` binds the key; `FunctionHarness` calls it before each
  invocation (see `harnesses.md`).
- All operations call `backing()`, which throws
  `IllegalStateException("Keyed state accessed with no bound key …")` when no key is
  bound. Creating a handle does **not** require a key; only using it does.
- State is scoped by `(node instance, key, state name)`. Each `FunctionHarness` has its
  own store, so two nodes with the same state name do not collide.
- v1 and v2 states with the same id are isolated by the `"v2:"` prefix (proved by
  `v1AndV2SameStateIdAreIsolated`).
- `clearAll()` drops every key's state; `clearCurrentKey()` drops only the bound key
  (the latter is used only in tests — the workflow exposes only whole-node clearing).

## v1 API

Accessors on `StandaloneRuntimeContext` delegate to the store. `ValueStateDescriptor`
and the v2 descriptors are initialized with `initializeSerializerUnlessSet(new ExecutionConfig())`
so default values work. Descriptor serializers are otherwise unused (no serialization).

| State | Behavior |
|---|---|
| `ValueState` | `value()` returns the stored value or `descriptor.getDefaultValue()`; `update(v)` stores (a stored `null` also yields the default on read); `clear()` removes. |
| `ListState` | `get()` returns the **live** stored list, or `List.of()` if unset; `add`, `addAll`, `update`, `clear`. |
| `ReducingState` | `get()` returns the reduced value or `null`; `add(v)` reduces with `descriptor.getReduceFunction()` (first add stores as-is); reducer failure → `RuntimeException("Reducing function threw")`. |
| `AggregatingState` | `get()` returns `aggregate.getResult(acc)` or `null`; `add(v)` creates an accumulator if absent; `clear()`. |
| `MapState` | `get/put/putAll/remove/contains/entries/keys/values/iterator/isEmpty/clear`; collections are **live map views**. |

**v1 State TTL is silently ignored.** Unlike v2, the v1 path never calls `checkTtl`;
a `StateTtlConfig`-enabled v1 descriptor is accepted and TTL has no effect. Do not
assume v1 TTL is rejected. (No test covers this.)

## v2 API

Same store, `"v2:"`-prefixed names. `TTL` is rejected eagerly:

```java
if (d.getTtlConfig().isEnabled()) throw new UnsupportedOperationException("State TTL … is not supported …");
```

v2 intentionally has **stronger null/copy semantics** than v1:

| State | Behavior |
|---|---|
| `ValueState` | `value()` is `null` if unset (no descriptor default); `update(null)` removes; `clear()`. |
| `ListState` | `get()` returns an immutable copy (`List.copyOf`) or `List.of()`; `update(empty/null)` removes; `addAll(null/empty)` is a no-op; `add`, `clear`. |
| `ReducingState` | same as v1. |
| `AggregatingState` | same as v1. |
| `MapState` | `put(k, null)` removes; `entries()/keys()/values()` return immutable copies; `iterator()` iterates a copy; other methods like v1. |

### Async v2 semantics (eager)

- Every `asyncX` method runs its synchronous twin immediately and wraps the result in
  `CompletedStateFuture`; an `Void` result uses `CompletedStateFuture.ofVoid()`.
- `StateIterator` methods (`onNext`) iterate the snapshot eagerly on the caller thread;
  `CollectionStateIterator` is the only implementation.
- `CompletedStateFuture.thenApply/thenAccept/thenCompose/thenCombine` and the six
  `thenConditionally*` variants execute immediately. Continuation failures are wrapped
  as `RuntimeException("<method> failed", cause)`.
- `thenCompose`/`thenCombine`/`thenConditionallyCompose` assume the returned future is
  a harness `CompletedStateFuture`; a foreign `StateFuture` throws
  `IllegalStateException("flink-harness supports only eagerly-completed StateFutures …")`.
  This is how `CompletedStateFuture.resolve` extracts synchronous values.

## Lifecycle integration

- `FunctionHarness.open()` may register state handles; lazy open binds the first key
  first, eager open (`initializeAtBuild()`) binds a dummy placeholder key so handle
  registration cannot fail (`harnesses.md`).
- `clearState(nodeId)` / `clearStateAll()` call `InMemoryKeyedStateStore.clearAll()`
  and null the current key. For a `KeyedProcessFunctionHarness`, `clearState` also
  clears the timer heap (`timers.md`).
- `Mode.TRANSIENT` invokes the same clear after every `process()` call; `Mode.CONTINUOUS`
  retains state until explicitly cleared.

## Invariants and contracts

- State must resolve `backing()` per operation; never cache a key's map in a handle.
- State is scoped by `(node, key, name)`; v2 uses the `"v2:"` prefix to coexist with v1.
- A state operation without a bound key must throw `IllegalStateException`.
- Values are stored by reference — there is no serialization, copy, or size accounting.
- v2 TTL-enabled descriptors must be rejected with UOE.
- `clearAll` must remove state for every key, including v2 (proved by `clearAllCoversV2`).

## Important implementation patterns

- New state kinds are nested private static classes in `InMemoryKeyedStateStore`
  (v1) or `InMemoryStateV2` (v2) taking `Supplier<Map<String,Object>>` + name.
- Follow v2's null/copy discipline for any new v2 state; follow v1's live-semantics for
  v1 to avoid surprising existing users.
- Route all `RuntimeContext.get*State` overloads through `stateStore`, never create a
  second store.
- TTL checks belong in `checkTtl` on the v2 accessor; keep v1 as-is unless a deliberate
  decision says otherwise.

## Deliberate differences from Flink

- **No state backend abstraction**: Flink separates `StateBackend`/`KeyedStateBackend`
  creation, storage, and snapshotting; here there is a single in-memory map.
- **No serialization**: Flink serializes state with managed/registered serializers;
  this store keeps Java references, so mutable objects are shared.
- **No checkpointing/savepoints**: no `snapshotState`, no restore, no state
  redistribution.
- **No operator state / `CheckpointedFunction`** (see `FUTURE.md` §5).
- **State TTL**: rejected in v2, silently ignored in v1. Flink supports TTL in both.
- **v1 collections are live views** whereas Flink's are iterable handles over backend
  storage; v2 returns copies. This asymmetry is repository-specific.

## Common pitfalls / agent traps

- **Confusing v1 and v2 null/default semantics.** v1 `ValueState.value()` falls back to
  the descriptor default; v2 returns `null` and `update(null)` removes.
- **Assuming v1 TTL is rejected.** It is not.
- **Caching the current map.** The key changes between invocations; always resolve lazily.
- **Expecting serialization/copies.** Mutating a stored object mutates state.
- **Exposing `clearCurrentKey` to the workflow API** without considering that
  `clearState` is defined as whole-node clearing.
- **Calling state in `open()` without a key** when the node was not eagerly opened and
  no element has arrived — the lazy path only exists because the first element binds
  the key.
- **Using `InMemoryStateV2` from outside `org.flink.harness.state`.** It is
  package-private.

## Testing and verification

- `flink-harness/.../state/StateStoreTest` — per-key isolation, descriptor defaults,
  list/map/reducing/aggregating operations, `clearCurrentKey` vs `clearAll`, and
  `stateRequiresBoundKey`.
- `flink-harness/.../state/StateStoreV2Test` — v2 operations, null/copy semantics,
  eager async chains, exception propagation, foreign-future rejection, all five TTL
  rejections, v1/v2 name isolation, clear coverage.
- `flink-harness/.../KeyedV2StateIntegrationTest` — v2 state through the full workflow
  in CONTINUOUS and TRANSIENT modes.
- `flink-harness/.../StandaloneWorkflowTest` — v1 state accumulation and TRANSIENT reset.

No test covers v1 TTL being ignored — record that as a gap if you touch TTL.

## Debugging / investigation map

| Symptom | First inspect |
|---|---|
| `IllegalStateException: … no bound key` | edge not keyed; state used outside an invocation; eager dummy key only at open |
| state bleeds across keys | a handle cached a map instead of `backing()` |
| state bleeds across nodes | shared store (should be per `FunctionHarness`) |
| v1 and v2 same name collide | `"v2:"` prefix removed |
| TTL descriptor accepted | v1 path — TTL is only checked for v2 |
| state not reset in tests | CONTINUOUS mode; call `clearState` or use TRANSIENT |

## Related agent references

- [runtime-context.md](./runtime-context.md) — the `get*State` entry points.
- [harnesses.md](./harnesses.md) — key binding and open lifecycle.
- [timers.md](./timers.md) — timers are cleared together with keyed state.
- [workflow.md](./workflow.md) — `clearState`/TRANSIENT orchestration.
- [testing.md](./testing.md) — which tests establish state semantics.

## External references

- Flink 2.3 docs, state:
  https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state/
  (semantic baseline for keyed state scoping and state kinds).
- Flink 2.3 `RuntimeContext` v1/v2 state accessors:
  https://raw.githubusercontent.com/apache/flink/release-2.3.0/flink-core/src/main/java/org/apache/flink/api/common/functions/RuntimeContext.java
- Upstream heap backend (the closest Flink analogue, not used here):
  `flink-runtime/src/main/java/org/apache/flink/runtime/state/heap/HeapKeyedStateBackend.java`
