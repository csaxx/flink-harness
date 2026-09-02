package org.flink.harness;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KeyedProcessFunctionHarnessTest {

    @Test
    void statePerKeyAndCurrentKeyExposure() {
        CountingKeyFn fn = new CountingKeyFn();
        KeyedProcessFunctionHarness harness = new KeyedProcessFunctionHarness("count", fn, false, "String", "String");
        KeySelector<String, String> selector = new KeySelector<String, String>() {
            @Override
            public String getKey(String value) {
                return value.substring(0, 1);
            }
        };
        Edge edge = new Edge("src", "count", selector);

        FunctionResult<?> r1 = harness.processViaEdge("apple", edge);
        FunctionResult<?> r2 = harness.processViaEdge("apricot", edge);
        FunctionResult<?> r3 = harness.processViaEdge("banana", edge);
        FunctionResult<?> r4 = harness.processViaEdge("apple", edge);

        assertThat((List<Object>) r1.outputs()).containsExactly("apple→key=a→count=1");
        assertThat((List<Object>) r2.outputs()).containsExactly("apricot→key=a→count=2");
        assertThat((List<Object>) r3.outputs()).containsExactly("banana→key=b→count=1");
        assertThat((List<Object>) r4.outputs()).containsExactly("apple→key=a→count=3");
    }

    @Test
    void unkeyedEdgeFailsLoudlyAtRuntime() {
        CountingKeyFn fn = new CountingKeyFn();
        KeyedProcessFunctionHarness harness = new KeyedProcessFunctionHarness("count", fn, false, "String", "String");
        // no key bound → state access must fail with a clear message
        try {
            harness.processViaEdge("apple", null);
        } catch (RuntimeException e) {
            assertThat(e).hasMessageContaining("failed");
            return;
        }
        throw new AssertionError("expected failure on unbound key");
    }

    private static final class CountingKeyFn extends KeyedProcessFunction<String, String, String> {
        private transient ValueState<Long> count;

        @Override
        public void open(OpenContext openContext) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            long current = count.value() == null ? 0 : count.value();
            count.update(current + 1);
            out.collect(value + "→key=" + ctx.getCurrentKey() + "→count=" + (current + 1));
        }
    }
}