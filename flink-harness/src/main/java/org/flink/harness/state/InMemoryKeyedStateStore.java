package org.flink.harness.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Minimal in-memory keyed state store backing {@code RuntimeContext.get*State(...)} v1 calls.
 * Accessors resolve the current key's map per operation (correct per-key isolation).
 */
public final class InMemoryKeyedStateStore {

    private static final ExecutionConfig EXECUTION_CONFIG = new ExecutionConfig();

    private final Map<Object, Map<String, Object>> stateByKey = new HashMap<>();
    private Object currentKey;

    /** Bind the current key. Called by the harness before invoking the wrapped function. */
    public void setCurrentKey(Object key) {
        this.currentKey = key;
    }

    /** Drop all state for every key. */
    public void clearAll() {
        stateByKey.clear();
    }

    /** Drop state only for the currently bound key (useful for targeted resets). */
    public void clearCurrentKey() {
        if (currentKey != null) {
            stateByKey.remove(currentKey);
        }
    }

    public <T> ValueState<T> getState(ValueStateDescriptor<T> descriptor) {
        return new InMemoryValueState<>(this::backing, descriptor.getName(), prepare(descriptor));
    }

    public <T> ListState<T> getListState(ListStateDescriptor<T> descriptor) {
        return new InMemoryListState<>(this::backing, descriptor.getName());
    }

    public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> descriptor) {
        return new InMemoryReducingState<>(this::backing, descriptor.getName(), descriptor.getReduceFunction());
    }

    public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
            AggregatingStateDescriptor<IN, ACC, OUT> descriptor) {
        return new InMemoryAggregatingState<>(
                this::backing, descriptor.getName(), descriptor.getAggregateFunction());
    }

    public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> descriptor) {
        return new InMemoryMapState<>(this::backing, descriptor.getName());
    }

    /** Initialize the descriptor's serializer (so defaults work) before use. */
    private static <D extends StateDescriptor> D prepare(D descriptor) {
        descriptor.initializeSerializerUnlessSet(EXECUTION_CONFIG);
        return descriptor;
    }

    /** Backing map for the current key; must have a key bound. */
    private Map<String, Object> backing() {
        if (currentKey == null) {
            throw new IllegalStateException(
                    "Keyed state accessed with no bound key — keyed state is only available "
                            + "when the function is invoked through a keyed edge.");
        }
        return stateByKey.computeIfAbsent(currentKey, k -> new HashMap<>());
    }

    // --------------------------------------------------------------------------------------------
    // State accessors — each resolves backing() per operation for correct per-key scoping
    // --------------------------------------------------------------------------------------------

    private static final class InMemoryValueState<T> implements ValueState<T> {
        private final Supplier<Map<String, Object>> backing;
        private final String name;
        private final ValueStateDescriptor<T> descriptor;

        InMemoryValueState(Supplier<Map<String, Object>> backing, String name, ValueStateDescriptor<T> descriptor) {
            this.backing = backing;
            this.name = name;
            this.descriptor = descriptor;
        }

        @Override
        @SuppressWarnings("unchecked")
        public T value() {
            Object v = backing.get().get(name);
            return v != null ? (T) v : descriptor.getDefaultValue();
        }

        @Override
        public void update(T value) {
            backing.get().put(name, value);
        }

        @Override
        public void clear() {
            backing.get().remove(name);
        }
    }

    private static final class InMemoryListState<T> implements ListState<T> {
        private final Supplier<Map<String, Object>> backing;
        private final String name;

        InMemoryListState(Supplier<Map<String, Object>> backing, String name) {
            this.backing = backing;
            this.name = name;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Iterable<T> get() {
            return (Iterable<T>) backing.get().getOrDefault(name, List.of());
        }

        @Override
        @SuppressWarnings("unchecked")
        public void add(T value) {
            list().add(value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void addAll(List<T> values) {
            List<T> all = new ArrayList<>(getList());
            all.addAll(values);
            backing.get().put(name, all);
        }

        @Override
        public void update(List<T> values) {
            backing.get().put(name, new ArrayList<>(values));
        }

        @Override
        public void clear() {
            backing.get().remove(name);
        }

        @SuppressWarnings("unchecked")
        private List<T> getList() {
            return (List<T>) backing.get().getOrDefault(name, List.of());
        }

        @SuppressWarnings("unchecked")
        private List<T> list() {
            Map<String, Object> map = backing.get();
            List<T> existing = (List<T>) map.get(name);
            if (existing == null) {
                List<T> created = new ArrayList<>();
                map.put(name, created);
                return created;
            }
            return existing;
        }
    }

    private static final class InMemoryReducingState<T> implements ReducingState<T> {
        private final Supplier<Map<String, Object>> backing;
        private final String name;
        private final ReduceFunction<T> reduce;

        InMemoryReducingState(Supplier<Map<String, Object>> backing, String name, ReduceFunction<T> reduce) {
            this.backing = backing;
            this.name = name;
            this.reduce = reduce;
        }

        @Override
        @SuppressWarnings("unchecked")
        public T get() {
            return (T) backing.get().get(name);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void add(T value) {
            Map<String, Object> map = backing.get();
            Object previous = map.get(name);
            if (previous == null) {
                map.put(name, value);
            } else {
                try {
                    map.put(name, reduce.reduce((T) previous, value));
                } catch (Exception e) {
                    throw new RuntimeException("Reducing function threw", e);
                }
            }
        }

        @Override
        public void clear() {
            backing.get().remove(name);
        }
    }

    private static final class InMemoryAggregatingState<IN, ACC, OUT> implements AggregatingState<IN, OUT> {
        private final Supplier<Map<String, Object>> backing;
        private final String name;
        private final AggregateFunction<IN, ACC, OUT> aggregate;

        InMemoryAggregatingState(Supplier<Map<String, Object>> backing, String name, AggregateFunction<IN, ACC, OUT> aggregate) {
            this.backing = backing;
            this.name = name;
            this.aggregate = aggregate;
        }

        @Override
        @SuppressWarnings("unchecked")
        public OUT get() {
            ACC acc = (ACC) backing.get().get(name);
            return acc == null ? null : aggregate.getResult(acc);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void add(IN value) {
            Map<String, Object> map = backing.get();
            ACC previous = (ACC) map.get(name);
            ACC accumulator = previous == null ? aggregate.createAccumulator() : previous;
            map.put(name, aggregate.add(value, accumulator));
        }

        @Override
        public void clear() {
            backing.get().remove(name);
        }
    }

    private static final class InMemoryMapState<UK, UV> implements MapState<UK, UV> {
        private final Supplier<Map<String, Object>> backing;
        private final String name;

        InMemoryMapState(Supplier<Map<String, Object>> backing, String name) {
            this.backing = backing;
            this.name = name;
        }

        @Override
        public UV get(UK key) {
            return map().get(key);
        }

        @Override
        public void put(UK key, UV value) {
            map().put(key, value);
        }

        @Override
        public void putAll(Map<UK, UV> map) {
            map().putAll(map);
        }

        @Override
        public void remove(UK key) {
            map().remove(key);
        }

        @Override
        public boolean contains(UK key) {
            return map().containsKey(key);
        }

        @Override
        public Iterable<Map.Entry<UK, UV>> entries() {
            return map().entrySet();
        }

        @Override
        public Iterable<UK> keys() {
            return map().keySet();
        }

        @Override
        public Iterable<UV> values() {
            return map().values();
        }

        @Override
        public Iterator<Map.Entry<UK, UV>> iterator() {
            return map().entrySet().iterator();
        }

        @Override
        public boolean isEmpty() {
            return map().isEmpty();
        }

        @Override
        public void clear() {
            backing.get().remove(name);
        }

        @SuppressWarnings("unchecked")
        private Map<UK, UV> map() {
            Map<String, Object> outer = backing.get();
            Map<UK, UV> existing = (Map<UK, UV>) outer.get(name);
            if (existing == null) {
                Map<UK, UV> created = new HashMap<>();
                outer.put(name, created);
                return created;
            }
            return existing;
        }
    }
}