package org.flink.harness.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StateStoreTest {

    private InMemoryKeyedStateStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyedStateStore();
    }

    @Test
    void stateRequiresBoundKey() {
        ValueStateDescriptor<String> d = new ValueStateDescriptor<>("v", String.class);
        ValueState<String> state = store.getState(d);
        assertThatThrownBy(state::value)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no bound key");
    }

    @Test
    void valueStatePerKeyIsolation() throws Exception {
        store.setCurrentKey("a");
        ValueState<String> state = store.getState(new ValueStateDescriptor<>("v", String.class));
        state.update("1");
        store.setCurrentKey("b");
        ValueState<String> other = store.getState(new ValueStateDescriptor<>("v", String.class));
        assertThat(other.value()).isNull();
        other.update("2");
        store.setCurrentKey("a");
        assertThat(store.<String>getState(new ValueStateDescriptor<>("v", String.class)).value()).isEqualTo("1");
        store.setCurrentKey("b");
        assertThat(store.<String>getState(new ValueStateDescriptor<>("v", String.class)).value()).isEqualTo("2");
    }

    @Test
    void valueStateDefaultFromDescriptor() throws Exception {
        store.setCurrentKey("a");
        ValueState<Long> state = store.getState(new ValueStateDescriptor<>("v", Long.class, 42L));
        assertThat(state.value()).isEqualTo(42L);
    }

    @Test
    void listStateOperations() throws Exception {
        store.setCurrentKey("k");
        ListState<String> state = store.getListState(new ListStateDescriptor<>("l", String.class));
        state.add("one");
        state.add("two");
        assertThat(state.get()).containsExactly("one", "two");
        state.addAll(List.of("three"));
        state.update(List.of("replaced"));
        assertThat(state.get()).containsExactly("replaced");
        state.clear();
        assertThat(state.get()).isEmpty();
    }

    @Test
    void mapStateOperations() throws Exception {
        store.setCurrentKey("k");
        MapState<String, Integer> state = store.getMapState(new MapStateDescriptor<>("m", String.class, Integer.class));
        state.put("a", 1);
        state.put("b", 2);
        assertThat(state.contains("a")).isTrue();
        assertThat(state.get("b")).isEqualTo(2);
        assertThat(state.isEmpty()).isFalse();
        state.remove("a");
        assertThat(state.contains("a")).isFalse();
        state.putAll(java.util.Map.of("x", 7));
        assertThat(state.get("x")).isEqualTo(7);
        assertThat(state.keys()).containsExactly("b", "x");
        state.clear();
        assertThat(state.isEmpty()).isTrue();
    }

    @Test
    void reducingStateAggregates() throws Exception {
        store.setCurrentKey("k");
        ReducingState<Long> state = store.getReducingState(
                new ReducingStateDescriptor<>("r", (a, b) -> a + b, Long.class));
        state.add(5L);
        state.add(7L);
        assertThat(state.get()).isEqualTo(12L);
        state.clear();
        assertThat(state.get()).isNull();
    }

    @Test
    void aggregatingStateAggregates() throws Exception {
        store.setCurrentKey("k");
        AggregatingState<Long, Long> state = store.getAggregatingState(
                new AggregatingStateDescriptor<>("a", new SumAgg(), Long.class));
        state.add(3L);
        state.add(4L);
        assertThat(state.get()).isEqualTo(7L);
        state.clear();
        assertThat(state.get()).isNull();
    }

    @Test
    void clearResetsOnlyCurrentKey() throws Exception {
        store.setCurrentKey("a");
        store.getState(new ValueStateDescriptor<>("v", String.class)).update("1");
        store.setCurrentKey("b");
        store.getState(new ValueStateDescriptor<>("v", String.class)).update("2");
        store.setCurrentKey("a");
        store.clearCurrentKey();
        assertThat(store.<String>getState(new ValueStateDescriptor<>("v", String.class)).value()).isNull();
        store.setCurrentKey("b");
        assertThat(store.<String>getState(new ValueStateDescriptor<>("v", String.class)).value()).isEqualTo("2");
    }

    @Test
    void clearAllResetsEverything() throws Exception {
        store.setCurrentKey("a");
        store.getState(new ValueStateDescriptor<>("v", String.class)).update("1");
        store.setCurrentKey("b");
        store.getState(new ValueStateDescriptor<>("v", String.class)).update("2");
        store.clearAll();
        store.setCurrentKey("a");
        assertThat(store.<String>getState(new ValueStateDescriptor<>("v", String.class)).value()).isNull();
        store.setCurrentKey("b");
        assertThat(store.<String>getState(new ValueStateDescriptor<>("v", String.class)).value()).isNull();
    }

    private static final class SumAgg implements AggregateFunction<Long, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Long value, Long accumulator) {
            return value + accumulator;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}