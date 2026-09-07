package org.flink.harness.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.v2.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.ReducingStateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StateStoreV2Test {

    private InMemoryKeyedStateStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyedStateStore();
    }

    // -- basic preconditions ----------------------------------------------------------------

    @Test
    void stateRequiresBoundKey() {
        store.setCurrentKey("k");
        org.apache.flink.api.common.state.v2.ValueState<String> state =
                store.getState(new ValueStateDescriptor<>("v", String.class));
        store.setCurrentKey(null);
        assertThatThrownBy(state::value)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no bound key");
    }

    // -- ValueState -------------------------------------------------------------------------

    @Test
    void valueStatePerKeyIsolation() {
        store.setCurrentKey("a");
        var state = store.getState(new ValueStateDescriptor<>("v", String.class));
        state.update("1");
        store.setCurrentKey("b");
        var other = store.getState(new ValueStateDescriptor<>("v", String.class));
        assertThat(other.value()).isNull();
        other.update("2");
        store.setCurrentKey("a");
        assertThat(store.<String>getState(new ValueStateDescriptor<>("v", String.class)).value()).isEqualTo("1");
        store.setCurrentKey("b");
        assertThat(store.<String>getState(new ValueStateDescriptor<>("v", String.class)).value()).isEqualTo("2");
    }

    @Test
    void valueStateNullWhenUnset() {
        store.setCurrentKey("k");
        var state = store.getState(new ValueStateDescriptor<>("v", String.class));
        assertThat(state.value()).isNull();
    }

    @Test
    void valueStateUpdateNullRemoves() {
        store.setCurrentKey("k");
        var state = store.getState(new ValueStateDescriptor<>("v", String.class));
        state.update("x");
        assertThat(state.value()).isEqualTo("x");
        state.update(null);
        assertThat(state.value()).isNull();
    }

    // -- ListState --------------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void listStateOperations() {
        store.setCurrentKey("k");
        var state = store.getListState(new ListStateDescriptor<>("l", String.class));
        state.add("one");
        state.add("two");
        assertThat(state.get()).containsExactly("one", "two");
        state.addAll(List.of("three"));
        assertThat(state.get()).containsExactly("one", "two", "three");
        state.update(List.of("replaced"));
        assertThat(state.get()).containsExactly("replaced");
        state.clear();
        assertThat(state.get()).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void listStateUpdateEmptyRemoves() {
        store.setCurrentKey("k");
        var state = store.getListState(new ListStateDescriptor<>("l", String.class));
        state.add("x");
        assertThat(state.get()).isNotEmpty();
        state.update(List.of());
        assertThat(state.get()).isEmpty();
    }

    // -- MapState ---------------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void mapStateOperations() {
        store.setCurrentKey("k");
        var state = store.getMapState(new MapStateDescriptor<>("m", String.class, Integer.class));
        state.put("a", 1);
        state.put("b", 2);
        assertThat(state.contains("a")).isTrue();
        assertThat(state.get("b")).isEqualTo(2);
        assertThat(state.isEmpty()).isFalse();
        state.remove("a");
        assertThat(state.contains("a")).isFalse();
        state.putAll(Map.of("x", 7));
        assertThat(state.get("x")).isEqualTo(7);
        assertThat(state.keys()).containsExactly("x", "b");
        assertThat(state.values()).containsExactlyInAnyOrder(7, 2);
        state.clear();
        assertThat(state.isEmpty()).isTrue();
    }

    @Test
    @SuppressWarnings("unchecked")
    void mapStatePutNullRemoves() {
        store.setCurrentKey("k");
        var state = store.getMapState(new MapStateDescriptor<>("m", String.class, String.class));
        state.put("a", "x");
        assertThat(state.get("a")).isEqualTo("x");
        state.put("a", null);
        assertThat(state.contains("a")).isFalse();
    }

    // -- ReducingState ---------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void reducingStateAggregates() {
        store.setCurrentKey("k");
        var state = store.getReducingState(
                new ReducingStateDescriptor<>("r", (a, b) -> a + b, Long.class));
        state.add(5L);
        state.add(7L);
        assertThat(state.get()).isEqualTo(12L);
        state.clear();
        assertThat(state.get()).isNull();
    }

    // -- AggregatingState ------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void aggregatingStateAggregates() {
        store.setCurrentKey("k");
        var state = store.getAggregatingState(
                new AggregatingStateDescriptor<>("a", new SumAgg(), Long.class));
        state.add(3L);
        state.add(4L);
        assertThat(state.get()).isEqualTo(7L);
        state.clear();
        assertThat(state.get()).isNull();
    }

    // -- Async semantics (eager chaining) --------------------------------------------------

    @Test
    void asyncValueStateChain() {
        store.setCurrentKey("k");
        var state = store.getState(new ValueStateDescriptor<>("increment", Integer.class));

        AtomicInteger result = new AtomicInteger();
        state.asyncUpdate(10)
                .thenCompose(v -> state.asyncValue())
                .thenApply(v -> v + 5)
                .thenAccept(result::set);

        assertThat(result.get()).isEqualTo(15);
    }

    @Test
    @SuppressWarnings("unchecked")
    void asyncListStateGetIteratorOnNextCollect() {
        store.setCurrentKey("k");
        var state = store.getListState(new ListStateDescriptor<>("l", String.class));
        state.add("a");
        state.add("b");
        state.add("c");

        // eager: iterator onNext runs immediately, accessing value via package-private cast
        var resultFuture = state.asyncGet()
                .thenCompose(iter -> iter.onNext(
                        (FunctionWithException<String, StateFuture<? extends String>, Exception>)
                                s -> CompletedStateFuture.of(s.toUpperCase())));
        var cf = (CompletedStateFuture<Collection<String>>) resultFuture;
        assertThat(cf.value()).containsExactly("A", "B", "C");
    }

    @Test
    @SuppressWarnings("unchecked")
    void asyncListStateGetIteratorOnNextConsume() {
        store.setCurrentKey("k");
        var state = store.getListState(new ListStateDescriptor<>("l", String.class));
        state.add("x");
        state.add("y");

        List<String> consumed = new ArrayList<>();
        // eager: the consumer runs during thenCompose
        state.asyncGet()
                .thenCompose(iter -> iter.onNext((ThrowingConsumer<String, Exception>) consumed::add));
        assertThat(consumed).containsExactly("x", "y");
    }

    @Test
    @SuppressWarnings("unchecked")
    void asyncMapStateIterators() {
        store.setCurrentKey("k");
        var state = store.getMapState(new MapStateDescriptor<>("m", String.class, Integer.class));
        state.put("a", 1);
        state.put("b", 2);

        // eager: cast to CompletedStateFuture to verify (package-private access)
        var future = state.asyncKeys()
                .thenCompose(iter -> iter.onNext(
                        (FunctionWithException<String, StateFuture<? extends String>, Exception>)
                                CompletedStateFuture::of));
        var cf = (CompletedStateFuture<Collection<String>>) future;
        assertThat(cf.value()).containsExactlyInAnyOrder("a", "b");
    }

    // -- All then* methods run eagerly ----------------------------------------------------

    @Test
    void stateFutureThenApply() {
        StateFuture<Integer> f = CompletedStateFuture.of(10);
        StateFuture<String> result = f.thenApply(v -> "got:" + v);
        assertThat(((CompletedStateFuture<String>) result).value()).isEqualTo("got:10");
    }

    @Test
    void stateFutureThenAccept() {
        AtomicReference<Integer> ref = new AtomicReference<>();
        StateFuture<Integer> f = CompletedStateFuture.of(42);
        f.thenAccept(ref::set);
        assertThat(ref.get()).isEqualTo(42);
    }

    @Test
    void stateFutureThenCompose() {
        StateFuture<Integer> f = CompletedStateFuture.of(5);
        StateFuture<String> result = f.thenCompose(v -> CompletedStateFuture.of("n=" + v));
        assertThat(((CompletedStateFuture<String>) result).value()).isEqualTo("n=5");
    }

    @Test
    void stateFutureThenCombine() {
        StateFuture<Integer> a = CompletedStateFuture.of(3);
        StateFuture<Integer> b = CompletedStateFuture.of(4);
        StateFuture<Integer> result = a.thenCombine(b, (x, y) -> x + y);
        assertThat(((CompletedStateFuture<Integer>) result).value()).isEqualTo(7);
    }

    @Test
    void stateFutureThenConditionallyApplyTwoArg() {
        StateFuture<Integer> f = CompletedStateFuture.of(5);
        // condition: value > 3 → true branch: "big"
        var result = f.thenConditionallyApply(
                v -> v > 3,
                v -> "big",
                v -> "small");
        assertThat(((CompletedStateFuture<?>) result).value())
                .isInstanceOfSatisfying(
                        org.apache.flink.api.java.tuple.Tuple2.class,
                        t -> assertThat(t.f0).isEqualTo(true));
    }

    @Test
    void stateFutureThenConditionallyApplyOneArg() {
        StateFuture<Integer> f = CompletedStateFuture.of(1);
        // condition: value > 3 → false → Tuple2(false, null)
        var result = f.thenConditionallyApply(v -> v > 3, v -> "big");
        assertThat(((CompletedStateFuture<?>) result).value())
                .isInstanceOfSatisfying(
                        org.apache.flink.api.java.tuple.Tuple2.class,
                        t -> assertThat(t.f0).isEqualTo(false));
    }

    @Test
    void stateFutureThenConditionallyAcceptTwoArg() {
        StateFuture<Integer> f = CompletedStateFuture.of(5);
        AtomicBoolean ran = new AtomicBoolean(false);
        f.thenConditionallyAccept(
                v -> v > 3,
                v -> ran.set(true),
                v -> ran.set(false));
        assertThat(ran).isTrue();
    }

    @Test
    void stateFutureThenConditionallyAcceptOneArg() {
        StateFuture<Integer> f = CompletedStateFuture.of(1);
        AtomicBoolean ran = new AtomicBoolean(false);
        f.thenConditionallyAccept(v -> v > 3, v -> ran.set(true));
        assertThat(ran).isFalse();
    }

    @Test
    void stateFutureThenConditionallyComposeTwoArg() {
        StateFuture<Integer> f = CompletedStateFuture.of(5);
        var result = f.thenConditionallyCompose(
                v -> v > 3,
                v -> CompletedStateFuture.of("big:" + v),
                v -> CompletedStateFuture.of("small:" + v));
        assertThat(((CompletedStateFuture<?>) result).value())
                .isInstanceOfSatisfying(
                        org.apache.flink.api.java.tuple.Tuple2.class,
                        t -> assertThat(t.f0).isEqualTo(true));
    }

    @Test
    void stateFutureThenConditionallyComposeOneArg() {
        StateFuture<Integer> f = CompletedStateFuture.of(1);
        var result = f.thenConditionallyCompose(
                v -> v > 3,
                v -> CompletedStateFuture.of("big:" + v));
        assertThat(((CompletedStateFuture<?>) result).value())
                .isInstanceOfSatisfying(
                        org.apache.flink.api.java.tuple.Tuple2.class,
                        t -> assertThat(t.f0).isEqualTo(false));
    }

    // -- Exception propagation -------------------------------------------------------------

    @Test
    void continuationExceptionPropagates() {
        StateFuture<Integer> f = CompletedStateFuture.of(10);
        assertThatThrownBy(() -> f.thenApply(v -> { throw new RuntimeException("fail"); }))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("thenApply failed");
    }

    // -- Foreign StateFuture rejection ------------------------------------------------------

    @Test
    void foreignStateFutureRejectedInThenCompose() {
        StateFuture<Integer> f = CompletedStateFuture.of(1);
        StateFuture<Integer> foreign = ForeignFuture.INSTANCE;
        assertThatThrownBy(() -> f.thenCompose(v -> foreign))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("thenCompose failed")
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("flink-harness supports only eagerly-completed StateFutures created by its own v2 state handles");
    }

    // -- TTL rejection ----------------------------------------------------------------------

    @Test
    void ttlEnabledValueStateRejected() {
        store.setCurrentKey("k");
        var descriptor = new ValueStateDescriptor<>("ttl", String.class);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofSeconds(1)).build());
        assertThatThrownBy(() -> store.getState(descriptor))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("State TTL");
    }

    @Test
    void ttlEnabledListStateRejected() {
        store.setCurrentKey("k");
        var descriptor = new ListStateDescriptor<>("ttl", String.class);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofSeconds(1)).build());
        assertThatThrownBy(() -> store.getListState(descriptor))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("State TTL");
    }

    @Test
    void ttlEnabledMapStateRejected() {
        store.setCurrentKey("k");
        var descriptor = new MapStateDescriptor<>("ttl", String.class, Integer.class);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofSeconds(1)).build());
        assertThatThrownBy(() -> store.getMapState(descriptor))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("State TTL");
    }

    @Test
    void ttlEnabledReducingStateRejected() {
        store.setCurrentKey("k");
        var descriptor = new ReducingStateDescriptor<>("ttl", (a, b) -> a, Long.class);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofSeconds(1)).build());
        assertThatThrownBy(() -> store.getReducingState(descriptor))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("State TTL");
    }

    @Test
    void ttlEnabledAggregatingStateRejected() {
        store.setCurrentKey("k");
        var descriptor = new AggregatingStateDescriptor<>("ttl", new SumAgg(), Long.class);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofSeconds(1)).build());
        assertThatThrownBy(() -> store.getAggregatingState(descriptor))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("State TTL");
    }

    // -- v1 / v2 isolation -----------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void v1AndV2SameStateIdAreIsolated() throws Exception {
        store.setCurrentKey("k");

        // v1
        var v1 = store.getState(new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                "shared", String.class));
        // v2
        var v2 = store.getState(new ValueStateDescriptor<>("shared", String.class));

        v1.update("v1-value");
        v2.update("v2-value");

        assertThat(v1.value()).isEqualTo("v1-value");
        assertThat(v2.value()).isEqualTo("v2-value");
    }

    // -- clearCurrentKey / clearAll cover v2 state -----------------------------------------

    @Test
    void clearCurrentKeyCoversV2() {
        store.setCurrentKey("a");
        store.getState(new ValueStateDescriptor<>("v", String.class)).update("v2-data");
        store.setCurrentKey("b");
        store.getState(new ValueStateDescriptor<>("v", String.class)).update("other");
        store.setCurrentKey("a");
        store.clearCurrentKey();
        assertThat(store.<String>getState(new ValueStateDescriptor<>("v", String.class)).value()).isNull();
        store.setCurrentKey("b");
        assertThat(store.<String>getState(new ValueStateDescriptor<>("v", String.class)).value()).isEqualTo("other");
    }

    @Test
    void clearAllCoversV2() {
        store.setCurrentKey("a");
        store.getState(new ValueStateDescriptor<>("v", String.class)).update("x");
        store.setCurrentKey("b");
        store.getState(new ValueStateDescriptor<>("v", String.class)).update("y");
        store.clearAll();
        store.setCurrentKey("a");
        assertThat(store.<String>getState(new ValueStateDescriptor<>("v", String.class)).value()).isNull();
        store.setCurrentKey("b");
        assertThat(store.<String>getState(new ValueStateDescriptor<>("v", String.class)).value()).isNull();
    }

    // -- Async clear -----------------------------------------------------------------------

    @Test
    void asyncClearRemovesValue() {
        store.setCurrentKey("k");
        var state = store.getState(new ValueStateDescriptor<>("c", String.class));
        state.update("present");
        state.asyncClear(); // eager — side effect happens immediately
        assertThat(state.value()).isNull();
    }

    // -- helpers ---------------------------------------------------------------------------

    private static final class SumAgg implements AggregateFunction<Long, Long, Long> {
        @Override
        public Long createAccumulator() { return 0L; }

        @Override
        public Long add(Long value, Long accumulator) { return value + accumulator; }

        @Override
        public Long getResult(Long accumulator) { return accumulator; }

        @Override
        public Long merge(Long a, Long b) { return a + b; }
    }

    /** A non-{@link CompletedStateFuture} {@link StateFuture} for rejection tests. */
    private static final class ForeignFuture implements StateFuture<Integer> {
        static final ForeignFuture INSTANCE = new ForeignFuture();

        @Override
        public <U> StateFuture<U> thenApply(
                FunctionWithException<? super Integer, ? extends U, ? extends Exception> fn) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StateFuture<Void> thenAccept(
                ThrowingConsumer<? super Integer, ? extends Exception> action) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U> StateFuture<U> thenCompose(
                FunctionWithException<? super Integer, ? extends StateFuture<U>, ? extends Exception>
                        action) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U, V> StateFuture<V> thenCombine(
                StateFuture<? extends U> other,
                BiFunctionWithException<? super Integer, ? super U, ? extends V, ? extends Exception>
                        fn) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U, V> StateFuture<Tuple2<Boolean, Object>> thenConditionallyApply(
                FunctionWithException<? super Integer, Boolean, ? extends Exception> condition,
                FunctionWithException<? super Integer, ? extends U, ? extends Exception> actionIfTrue,
                FunctionWithException<? super Integer, ? extends V, ? extends Exception>
                        actionIfFalse) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U> StateFuture<Tuple2<Boolean, U>> thenConditionallyApply(
                FunctionWithException<? super Integer, Boolean, ? extends Exception> condition,
                FunctionWithException<? super Integer, ? extends U, ? extends Exception>
                        actionIfTrue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StateFuture<Boolean> thenConditionallyAccept(
                FunctionWithException<? super Integer, Boolean, ? extends Exception> condition,
                ThrowingConsumer<? super Integer, ? extends Exception> actionIfTrue,
                ThrowingConsumer<? super Integer, ? extends Exception> actionIfFalse) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StateFuture<Boolean> thenConditionallyAccept(
                FunctionWithException<? super Integer, Boolean, ? extends Exception> condition,
                ThrowingConsumer<? super Integer, ? extends Exception> actionIfTrue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U, V> StateFuture<Tuple2<Boolean, Object>> thenConditionallyCompose(
                FunctionWithException<? super Integer, Boolean, ? extends Exception> condition,
                FunctionWithException<? super Integer, ? extends StateFuture<U>, ? extends Exception>
                        actionIfTrue,
                FunctionWithException<? super Integer, ? extends StateFuture<V>, ? extends Exception>
                        actionIfFalse) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U> StateFuture<Tuple2<Boolean, U>> thenConditionallyCompose(
                FunctionWithException<? super Integer, Boolean, ? extends Exception> condition,
                FunctionWithException<? super Integer, ? extends StateFuture<U>, ? extends Exception>
                        actionIfTrue) {
            throw new UnsupportedOperationException();
        }
    }
}