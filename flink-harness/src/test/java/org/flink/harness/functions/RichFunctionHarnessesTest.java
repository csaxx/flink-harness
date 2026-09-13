package org.flink.harness.functions;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;
import org.flink.harness.graph.StreamEdge;
import org.flink.harness.graph.function.rich.RichFilterFunctionHarness;
import org.flink.harness.graph.function.rich.RichFlatMapFunctionHarness;
import org.flink.harness.graph.function.rich.RichMapFunctionHarness;
import org.flink.harness.graph.result.FunctionResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RichFunctionHarnessesTest {

    @Test
    @SuppressWarnings("unchecked")
    void mapDropsNullResult() {
        RichMapFunctionHarness harness = new RichMapFunctionHarness(
                "map", new RichMapFunction<String, String>() {
                    @Override
                    public String map(String value) {
                        return value.isEmpty() ? null : value.toUpperCase(java.util.Locale.ROOT);
                    }
                });

        assertThat((List<Object>) harness.processElement("a", null).outputs()).containsExactly("A");
        assertThat((List<Object>) harness.processElement("", null).outputs()).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void flatMapEmitsViaCollector() {
        RichFlatMapFunctionHarness harness = new RichFlatMapFunctionHarness(
                "flatMap", new RichFlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) {
                        for (String token : value.split(",")) {
                            out.collect(token);
                        }
                    }
                });

        FunctionResult<?> result = harness.processElement("a,b,c", null);
        assertThat((List<Object>) result.outputs()).containsExactly("a", "b", "c");
    }

    @Test
    @SuppressWarnings("unchecked")
    void filterPassesThroughOnlyWhenAccepted() {
        RichFilterFunctionHarness harness = new RichFilterFunctionHarness(
                "filter", new RichFilterFunction<String>() {
                    @Override
                    public boolean filter(String value) {
                        return value.startsWith("ok");
                    }
                });

        assertThat((List<Object>) harness.processElement("ok-1", null).outputs()).containsExactly("ok-1");
        assertThat((List<Object>) harness.processElement("no-1", null).outputs()).isEmpty();
    }

    /** Flink-faithful: any rich function on a keyed edge may use keyed state (mirrors
     * keyed-stream semantics, not just KeyedProcessFunction). */
    @Test
    @SuppressWarnings("unchecked")
    void mapUsesKeyedStateOnKeyedEdge() {
        RichMapFunctionHarness harness = new RichMapFunctionHarness("map", new PerKeyCountingMap());
        KeySelector<String, String> firstChar = value -> value.substring(0, 1);
        StreamEdge edge = new StreamEdge("src", "map", firstChar);

        FunctionResult<?> a1 = harness.processElement("apple", edge);
        FunctionResult<?> a2 = harness.processElement("apricot", edge);
        FunctionResult<?> b1 = harness.processElement("banana", edge);

        assertThat((List<Object>) a1.outputs()).containsExactly("apple→1");
        assertThat((List<Object>) a2.outputs()).containsExactly("apricot→2");
        assertThat((List<Object>) b1.outputs()).containsExactly("banana→1");
    }

    private static final class PerKeyCountingMap extends RichMapFunction<String, String> {
        private transient ValueState<Long> count;

        @Override
        public void open(OpenContext openContext) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
        }

        @Override
        public String map(String value) throws Exception {
            long current = count.value() == null ? 0 : count.value();
            count.update(current + 1);
            return value + "→" + (current + 1);
        }
    }
}
