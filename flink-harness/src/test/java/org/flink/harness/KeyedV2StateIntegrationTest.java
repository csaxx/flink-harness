package org.flink.harness;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.flink.harness.graph.result.WorkflowResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests verifying that v2 state works through the full workflow path
 * (RuntimeContext → InMemoryKeyedStateStore → per-key isolation → TRANSIENT clearing).
 */
class KeyedV2StateIntegrationTest {

    @Test
    void v2StateThroughKeyedFunction() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("counter", new V2KeyedCount(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out", TypeInformation.of(String.class))
                .addKeyedEdge("in", "counter", new FirstCharKey())
                .addEdge("counter", "out")
                .build();

        WorkflowResult result = wf.process(List.of("alpha", "apricot", "beta"), "in");

        // Per-key counts: "alpha"+"apricot" share key "A" → #1 and #2; "beta" key "B" → #1
        assertThat(result.outputsOf("out"))
                .containsExactly("ALPHA#1", "APRICOT#2", "BETA#1");
    }

    @Test
    void v2StateAccumulatesAcrossProcessCallsInContinuousMode() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("counter", new V2KeyedCount(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out", TypeInformation.of(String.class))
                .addKeyedEdge("in", "counter", new FirstCharKey())
                .addEdge("counter", "out")
                .build();

        wf.process(List.of("a"), "in");
        WorkflowResult result = wf.process(List.of("a"), "in");

        assertThat(result.outputsOf("out")).containsExactly("A#2");
    }

    @Test
    void v2StateClearedEachRunInTransientMode() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.TRANSIENT)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("counter", new V2KeyedCount(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out", TypeInformation.of(String.class))
                .addKeyedEdge("in", "counter", new FirstCharKey())
                .addEdge("counter", "out")
                .build();

        WorkflowResult first = wf.process(List.of("a"), "in");
        assertThat(first.outputsOf("out")).containsExactly("A#1");

        WorkflowResult second = wf.process(List.of("a"), "in");
        assertThat(second.outputsOf("out")).containsExactly("A#1");
    }

    // -- test function and key selector -----------------------------------------------

    private static final class V2KeyedCount extends KeyedProcessFunction<String, String, String> {
        private org.apache.flink.api.common.state.v2.ValueState<Long> count;

        @Override
        public void open(OpenContext ctx) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("v2count", Long.class));
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            long cur = count.value() == null ? 0 : count.value();
            count.update(cur + 1);
            out.collect(value.toUpperCase(Locale.ROOT) + "#" + (cur + 1));
        }
    }

    private static final class FirstCharKey implements KeySelector<String, String> {
        @Override
        public String getKey(String value) {
            return value.substring(0, 1).toUpperCase(Locale.ROOT);
        }
    }
}