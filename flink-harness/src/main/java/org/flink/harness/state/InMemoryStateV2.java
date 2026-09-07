package org.flink.harness.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Package-private v2 state implementations backed by the same per-key map infrastructure
 * as v1. Each resolves {@code backing.get()} per operation for correct per-key isolation.
 * Internal storage is namespaced via a {@code "v2:"} prefix on the state name
 * (handled by {@link InMemoryKeyedStateStore}).
 *
 * <p>All async methods delegate to their sync twin and wrap in {@link CompletedStateFuture}.
 */
final class InMemoryStateV2 {

    private InMemoryStateV2() {}

    // ----------------------------------------------------------------------------------------
    // ValueState<V2>
    // ----------------------------------------------------------------------------------------

    static final class ValueStateV2<T> implements org.apache.flink.api.common.state.v2.ValueState<T> {

        private final Supplier<Map<String, Object>> backing;
        private final String name;

        ValueStateV2(Supplier<Map<String, Object>> backing, String name) {
            this.backing = backing;
            this.name = name;
        }

        @Override
        @SuppressWarnings("unchecked")
        public T value() {
            return (T) backing.get().get(name);
        }

        @Override
        public void update(T value) {
            if (value == null) {
                backing.get().remove(name);
            } else {
                backing.get().put(name, value);
            }
        }

        @Override
        public void clear() {
            backing.get().remove(name);
        }

        @Override
        public StateFuture<T> asyncValue() {
            return CompletedStateFuture.of(value());
        }

        @Override
        public StateFuture<Void> asyncUpdate(T value) {
            update(value);
            return CompletedStateFuture.ofVoid();
        }

        @Override
        public StateFuture<Void> asyncClear() {
            clear();
            return CompletedStateFuture.ofVoid();
        }
    }

    // ----------------------------------------------------------------------------------------
    // ListState<V2>
    // ----------------------------------------------------------------------------------------

    static final class ListStateV2<T> implements org.apache.flink.api.common.state.v2.ListState<T> {

        private final Supplier<Map<String, Object>> backing;
        private final String name;

        ListStateV2(Supplier<Map<String, Object>> backing, String name) {
            this.backing = backing;
            this.name = name;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Iterable<T> get() {
            List<T> list = (List<T>) backing.get().get(name);
            return list != null ? List.copyOf(list) : List.of();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void add(T value) {
            list().add(value);
        }

        @Override
        public void addAll(List<T> values) {
            if (values != null && !values.isEmpty()) {
                list().addAll(values);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void update(List<T> values) {
            if (values == null || values.isEmpty()) {
                backing.get().remove(name);
            } else {
                backing.get().put(name, new ArrayList<>(values));
            }
        }

        @Override
        public void clear() {
            backing.get().remove(name);
        }

        @Override
        public StateFuture<StateIterator<T>> asyncGet() {
            return CompletedStateFuture.of(new CollectionStateIterator<>(get()));
        }

        @Override
        public StateFuture<Void> asyncAdd(T value) {
            add(value);
            return CompletedStateFuture.ofVoid();
        }

        @Override
        public StateFuture<Void> asyncAddAll(List<T> values) {
            addAll(values);
            return CompletedStateFuture.ofVoid();
        }

        @Override
        public StateFuture<Void> asyncUpdate(List<T> values) {
            update(values);
            return CompletedStateFuture.ofVoid();
        }

        @Override
        public StateFuture<Void> asyncClear() {
            clear();
            return CompletedStateFuture.ofVoid();
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

    // ----------------------------------------------------------------------------------------
    // MapState<V2>
    // ----------------------------------------------------------------------------------------

    static final class MapStateV2<UK, UV> implements org.apache.flink.api.common.state.v2.MapState<UK, UV> {

        private final Supplier<Map<String, Object>> backing;
        private final String name;

        MapStateV2(Supplier<Map<String, Object>> backing, String name) {
            this.backing = backing;
            this.name = name;
        }

        @Override
        @SuppressWarnings("unchecked")
        public UV get(UK key) {
            return (UV) map().get(key);
        }

        @Override
        public void put(UK key, UV value) {
            if (value == null) {
                map().remove(key);
            } else {
                map().put(key, value);
            }
        }

        @Override
        public void putAll(Map<UK, UV> m) {
            Map<UK, UV> inner = map();
            for (Map.Entry<UK, UV> e : m.entrySet()) {
                UK k = e.getKey();
                UV v = e.getValue();
                if (v == null) {
                    inner.remove(k);
                } else {
                    inner.put(k, v);
                }
            }
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
            return Map.copyOf(map()).entrySet();
        }

        @Override
        public Iterable<UK> keys() {
            return Set.copyOf(map().keySet());
        }

        @Override
        public Iterable<UV> values() {
            return List.copyOf(map().values());
        }

        @Override
        public Iterator<Map.Entry<UK, UV>> iterator() {
            return entries().iterator();
        }

        @Override
        public boolean isEmpty() {
            return map().isEmpty();
        }

        @Override
        public void clear() {
            backing.get().remove(name);
        }

        // -- async methods --

        @Override
        @SuppressWarnings("unchecked")
        public StateFuture<UV> asyncGet(UK key) {
            return CompletedStateFuture.of(get(key));
        }

        @Override
        public StateFuture<Void> asyncPut(UK key, UV value) {
            put(key, value);
            return CompletedStateFuture.ofVoid();
        }

        @Override
        public StateFuture<Void> asyncPutAll(Map<UK, UV> m) {
            putAll(m);
            return CompletedStateFuture.ofVoid();
        }

        @Override
        public StateFuture<Void> asyncRemove(UK key) {
            remove(key);
            return CompletedStateFuture.ofVoid();
        }

        @Override
        public StateFuture<Boolean> asyncContains(UK key) {
            return CompletedStateFuture.of(contains(key));
        }

        @Override
        public StateFuture<StateIterator<Map.Entry<UK, UV>>> asyncEntries() {
            return CompletedStateFuture.of(new CollectionStateIterator<>(entries()));
        }

        @Override
        public StateFuture<StateIterator<UK>> asyncKeys() {
            return CompletedStateFuture.of(new CollectionStateIterator<>(keys()));
        }

        @Override
        public StateFuture<StateIterator<UV>> asyncValues() {
            return CompletedStateFuture.of(new CollectionStateIterator<>(values()));
        }

        @Override
        public StateFuture<Boolean> asyncIsEmpty() {
            return CompletedStateFuture.of(isEmpty());
        }

        @Override
        public StateFuture<Void> asyncClear() {
            clear();
            return CompletedStateFuture.ofVoid();
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

    // ----------------------------------------------------------------------------------------
    // ReducingState<V2>
    // ----------------------------------------------------------------------------------------

    static final class ReducingStateV2<T> implements org.apache.flink.api.common.state.v2.ReducingState<T> {

        private final Supplier<Map<String, Object>> backing;
        private final String name;
        private final ReduceFunction<T> reduce;

        ReducingStateV2(
                Supplier<Map<String, Object>> backing, String name, ReduceFunction<T> reduce) {
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
            T previous = (T) map.get(name);
            if (previous == null) {
                map.put(name, value);
            } else {
                try {
                    map.put(name, reduce.reduce(previous, value));
                } catch (Exception e) {
                    throw new RuntimeException("Reducing function threw", e);
                }
            }
        }

        @Override
        public void clear() {
            backing.get().remove(name);
        }

        @Override
        public StateFuture<T> asyncGet() {
            return CompletedStateFuture.of(get());
        }

        @Override
        public StateFuture<Void> asyncAdd(T value) {
            add(value);
            return CompletedStateFuture.ofVoid();
        }

        @Override
        public StateFuture<Void> asyncClear() {
            clear();
            return CompletedStateFuture.ofVoid();
        }
    }

    // ----------------------------------------------------------------------------------------
    // AggregatingState<V2>
    // ----------------------------------------------------------------------------------------

    static final class AggregatingStateV2<IN, ACC, OUT>
            implements org.apache.flink.api.common.state.v2.AggregatingState<IN, OUT> {

        private final Supplier<Map<String, Object>> backing;
        private final String name;
        private final AggregateFunction<IN, ACC, OUT> aggregate;

        AggregatingStateV2(
                Supplier<Map<String, Object>> backing,
                String name,
                AggregateFunction<IN, ACC, OUT> aggregate) {
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
            ACC acc = previous == null ? aggregate.createAccumulator() : previous;
            map.put(name, aggregate.add(value, acc));
        }

        @Override
        public void clear() {
            backing.get().remove(name);
        }

        @Override
        public StateFuture<OUT> asyncGet() {
            return CompletedStateFuture.of(get());
        }

        @Override
        public StateFuture<Void> asyncAdd(IN value) {
            add(value);
            return CompletedStateFuture.ofVoid();
        }

        @Override
        public StateFuture<Void> asyncClear() {
            clear();
            return CompletedStateFuture.ofVoid();
        }
    }
}