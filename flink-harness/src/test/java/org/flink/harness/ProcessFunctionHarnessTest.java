package org.flink.harness;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.flink.harness.harness.ProcessFunctionHarness;
import org.flink.harness.result.FunctionResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessFunctionHarnessTest {

    private static final OutputTag<String> REJECTED = new OutputTag<>("rejected") {};

    @Test
    @SuppressWarnings("unchecked")
    void invokesAndCollectsOutputsAndSideOutputs() {
        RoutingFunction fn = new RoutingFunction();
        ProcessFunctionHarness harness = new ProcessFunctionHarness("route", fn);

        FunctionResult<?> result = harness.processViaEdge("hello", null);
        assertThat((List<Object>) result.outputs()).containsExactly("HELLO");
        assertThat(result.sideOutputs()).isEmpty();

        result = harness.processViaEdge("bad!", null);
        assertThat((List<Object>) result.outputs()).containsExactly("BAD!");
        assertThat((List<Object>) result.sideOutputs().get(REJECTED)).containsExactly("bad!");
    }

    @Test
    void metricsSnapshotUpdateAfterInvocation() {
        MetaFunction fn = new MetaFunction();
        ProcessFunctionHarness harness = new ProcessFunctionHarness("meta", fn);
        harness.processViaEdge("x", null);
        harness.processViaEdge("y", null);
        assertThat(harness.metricsSnapshot()).containsEntry("total", 2L);
    }

    @Test
    void resetClearsMetricsAndState() {
        MetaFunction fn = new MetaFunction();
        ProcessFunctionHarness harness = new ProcessFunctionHarness("meta", fn);
        harness.processViaEdge("x", null);
        harness.resetAll();
        assertThat(harness.metricsSnapshot()).containsEntry("total", 0L);
    }

    private static final class RoutingFunction extends ProcessFunction<String, String> {
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            if (value.contains("!")) {
                ctx.output(REJECTED, value);
            }
            out.collect(value.toUpperCase(java.util.Locale.ROOT));
        }
    }

    private static final class MetaFunction extends ProcessFunction<Object, Object> {
        private Counter total;

        @Override
        public void open(OpenContext openContext) {
            total = getRuntimeContext().getMetricGroup().counter("total");
        }

        @Override
        public void processElement(Object value, Context ctx, Collector<Object> out) {
            total.inc();
            out.collect(value);
        }
    }
}