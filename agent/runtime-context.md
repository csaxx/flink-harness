# RuntimeContext surface

## Purpose

`StandaloneRuntimeContext` is the emulated Flink `RuntimeContext` handed to every
function. It defines exactly which runtime services exist, which are stubbed with
sane defaults, and which throw. Agents touching function-visible behavior must know
this surface, because most "why does my Flink job fail here" questions land on it.

## Read this when

- a function calls a `RuntimeContext` method that may not be implemented
- adding or changing a `RuntimeContext` capability
- changing `globalJobParameters`, serializers, `JobInfo`/`TaskInfo`, or classloading
- deciding whether an out-of-scope feature should become a metric or an UOE

## Mental model

```
RichFunction.getRuntimeContext()
   └── StandaloneRuntimeContext            (implements org.apache.flink...RuntimeContext)
         ├── StandaloneOperatorMetricGroup  → metrics.md
         ├── InMemoryKeyedStateStore        → state.md (v1 + v2)
         ├── JobInfo / TaskInfo             (anonymous, single-subtask)
         ├── globalJobParameters            (immutable copy, build-time)
         └── createSerializer(TypeInformation)
```

`flink-harness/src/main/java/org/flink/harness/graph/StandaloneRuntimeContext.java`

## Implemented surface

| Member | Behavior |
|---|---|
| `StandaloneRuntimeContext(String functionName)` | Creates the operator metric group and state store; `functionName` is used as job/task name. |
| `getMetricGroup()` | Returns `StandaloneOperatorMetricGroup` (an `OperatorMetricGroup`). |
| `stateStore()` | Internal accessor used by harnesses to bind keys (not Flink API). |
| `setGlobalJobParameters(Map)` | Stores an immutable `Map.copyOf(params)`; null → `NullPointerException` (from `Map.copyOf`). Called once by the builder. |
| `getGlobalJobParameters()` | Defaults to `Map.of()`; immutable. |
| `getState/getListState/getReducingState/getAggregatingState/getMapState` (v1) | Delegate to `InMemoryKeyedStateStore`; behavior in `state.md`. |
| `getState/getListState/...` (v2, `org.apache.flink.api.common.state.v2.*`) | Same store, separate namespace; TTL-enabled descriptors rejected. |
| `createSerializer(TypeInformation<T>)` | `typeInformation.createSerializer(new SerializerConfigImpl())`. No runtime serializer registry. |
| `isObjectReuseEnabled()` | `false`. |
| `getUserCodeClassLoader()` | Returns `getClass().getClassLoader()` — the harness classloader. |
| `registerUserCodeClassLoaderReleaseHookIfAbsent(name, hook)` | No-op (the classloader is never released). |
| `getJobInfo()` | Anonymous `JobInfo`: `getJobId() == null`, `getJobName() == functionName`. |
| `getTaskInfo()` | Anonymous `TaskInfo`: `getTaskName() == functionName`, `getNumberOfParallelSubtasks() == 1`, `getMaxNumberOfParallelSubtasks() == 1`, `getIndexOfThisSubtask() == 0`, `getAttemptNumber() == 0`, `getTaskNameWithSubtasks() == functionName + "(1/1)"`, `getAllocationIDAsString() == "standalone"`. |

## Deliberately unsupported surface

Accumulators are permanently out of scope; the message steers callers to metrics:

| Method | Result |
|---|---|
| `addAccumulator`, `getAccumulator`, `getIntCounter`, `getLongCounter`, `getDoubleCounter`, `getHistogram` | `UnsupportedOperationException("… is permanently out of scope in flink-harness — use metrics (getMetricGroup()) instead")` |
| `getExternalResourceInfos` | `UnsupportedOperationException("… not supported by flink-harness standalone runtime")` |
| `hasBroadcastVariable` | `false` |
| `getBroadcastVariable`, `getBroadcastVariableWithInitializer` | UOE (broadcast variables) |
| `getDistributedCache` | UOE (distributed cache) |

## Invariants and contracts

- State accessors must always route through the single keyed store instance so that
  key binding and clearing stay consistent (`state.md`).
- `globalJobParameters` is immutable and never reset, not even in TRANSIENT mode
  (proved by `paramsSurviveTransientReset`).
- `getTaskInfo()` must always report one subtask; changing it violates the
  parallelism-1 architecture.
- `getJobInfo()`/`getTaskInfo()` may return `null` for the id — do not assume a real
  job id exists.
- Accumulators must remain UOE; do not implement a subset "for convenience"
  (frozen design decision in AGENTS.md).
- If a new Flink `RuntimeContext` method is added by a version bump, implement it
  explicitly (default or UOE) — the class is a concrete `implements`, so an unhandled
  abstract method will not compile, but default methods may silently do the wrong thing.

## Important implementation patterns

- Prefer defaults with no side effects for query-like methods.
- Throw `UnsupportedOperationException` with one of the two factory helpers:
  `permanentlyOutOfScope(what)` (accumulators) or `unsupported(what)` (everything else).
  Keeping the two channels distinct makes it obvious which omissions are policy.
- Keep `RuntimeContext` free of execution concerns: it must not know about the BFS
  queue, edges, or timer modes.

## Deliberate differences from Flink

- No TaskManager/JobManager metadata: job id is `null`, allocation id is a constant.
- No distributed cache, broadcast variables, external resources, or accumulators.
- Classloading is not isolated; the user-code classloader is whatever loaded the
  harness. This is compatible with the `RuntimeContext` contract only in the sense
  that a non-null `ClassLoader` is returned.
- `createSerializer` produces standard Flink serializers but nothing in the runtime
  actually serializes state or records.
- `isObjectReuseEnabled()` is always `false`; unlike Flink there is no reuse mode.

## Common pitfalls / agent traps

- **Assuming `getUserCodeClassLoader()` throws.** It returns the harness classloader.
  (Older AGENTS.md claimed UOE — that was wrong.)
- **Assuming `getJobId()` is usable.** It is `null`.
- **Implementing accumulators.** Permanently out of scope; use
  `getMetricGroup().counter(...)`.
- **Mutating the returned `globalJobParameters` map.** It is immutable; updates throw.
- **Using `createSerializer` as a persistence mechanism.** Nothing serializes.
- **Adding a runtime service that needs a Thread/TaskManager.** There is none.

## Testing and verification

`flink-harness/src/test/java/org/flink/harness/internal/RuntimeContextFillersTest.java`:

- default/immutability of `globalJobParameters`, copy-on-set, null rejection
- builder wiring (`builderWiresGlobalJobParameters_toRichMap`) and TRANSIENT survival
- `createSerializer` round-trips for `String`, `Integer`, `Tuple2`
- all accumulator methods throw with "permanently out of scope"

Note: `getUserCodeClassLoader`, the release hook, `JobInfo`/`TaskInfo`,
broadcast-variable/distributed-cache/external-resource UOEs, and
`getMetricGroup()` are **not** covered by a dedicated test — treat claim changes there
as requiring a new test.

## Debugging / investigation map

| Symptom | First inspect |
|---|---|
| `UnsupportedOperationException` in a function | method used; likely accumulator/broadcast/cache |
| params empty in a function | builder did not call `globalJobParameters`, or `open()` was never called |
| serializer NPE | `createSerializer` called with a null `TypeInformation` |
| job/task name confusing | `functionName` = the node id (anonymous `JobInfo`/`TaskInfo`) |

## Related agent references

- [state.md](./state.md) — the keyed state backing the `get*State` methods.
- [metrics.md](./metrics.md) — the metric group this context exposes.
- [harnesses.md](./harnesses.md) — how the context is attached via `setRuntimeContext`.
- [architecture.md](./architecture.md) — why these services are absent by design.

## External references

- Flink 2.3 `RuntimeContext` (source, tag `release-2.3.0`), the interface this class
  implements, including v1 state (`@PublicEvolving`) and v2 state (`@Experimental`):
  https://raw.githubusercontent.com/apache/flink/release-2.3.0/flink-core/src/main/java/org/apache/flink/api/common/functions/RuntimeContext.java
- Flink 2.3 docs, state access through the runtime context:
  https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state/
