package org.flink.harness;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

class StandaloneWorkflowTest {

    private static final OutputTag<String> SHOUT = new OutputTag<>("shout") {};

    @Test
    void multiStageFlowWithFanOutAndKeyedBranch() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS, false)
                .registerFunction("entry", new UpperCase(), TypeInformation.of(String.class), TypeInformation.of(String.class))
                .registerFunction("shouter", new Shouter(), TypeInformation.of(String.class), TypeInformation.of(String.class))
                .registerKeyedFunction("keyedCount", new KeyedCount(), TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addEdge("entry", "shouter")
                .addKeyedEdge("entry", "keyedCount", new FirstCharKey())
                .activateOutput("shouter")
                .activateOutput("keyedCount")
                .activateSideOutput("shouter", SHOUT)
                .build();

        WorkflowResult result = wf.process(List.of("alpha", "apricot", "beta"), "entry");

        assertThat(result.outputs().get("shouter")).containsExactly("ALPHA", "APRICOT", "BETA");
        assertThat(result.outputs().get("keyedCount"))
                .containsExactly("ALPHA#1(A)", "APRICOT#2(A)", "BETA#1(B)");
        // both ALPHA and BETA end with "A" → both routed to the side output
        assertThat(result.sideOutputs().get(new WorkflowResult.SideOutputKey<>("shouter", SHOUT)))
                .containsExactly("ALPHA", "BETA");
    }

    @Test
    void transienteModeClearsStateBetweenRuns() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.TRANSIENT, false)
                .registerKeyedFunction("keyedCount", new KeyedCount(), new FirstCharKey())
                .activateOutput("keyedCount")
                .build(true);
        WorkflowResult first = wf.process(List.of("a"), "keyedCount");
        assertThat(first.outputs().get("keyedCount")).containsExactly("a#1(A)");
        WorkflowResult second = wf.process(List.of("a"), "keyedCount");
        assertThat(second.outputs().get("keyedCount")).containsExactly("a#1(A)");
    }

    @Test
    void continuousAccumulates() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS, false)
                .registerKeyedFunction("keyedCount", new KeyedCount(), new FirstCharKey())
                .activateOutput("keyedCount")
                .build(true);
        wf.process(List.of("a"), "keyedCount");
        WorkflowResult second = wf.process(List.of("a"), "keyedCount");
        assertThat(second.outputs().get("keyedCount")).containsExactly("a#2(A)");
        // clear state resets continuation
        wf.clearState("keyedCount");
        WorkflowResult third = wf.process(List.of("a"), "keyedCount");
        assertThat(third.outputs().get("keyedCount")).containsExactly("a#1(A)");
    }

    @Test
    void getWorkflowGraphReturnsTypesAndSuccessors() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS, false)
                .registerFunction("a", new UpperCase(), TypeInformation.of(String.class), TypeInformation.of(String.class))
                .registerFunction("b", new Shouter(), TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addEdge("a", "b")
                .activateOutput("b")
                .build();
        List<WorkflowNode> graph = wf.getWorkflow();
        assertThat(graph).hasSize(2);
        WorkflowNode a = graph.stream().filter(n -> n.functionId().equals("a")).findFirst().orElseThrow();
        assertThat(a.successors()).containsExactly("b");
        assertThat(a.inputType()).isNotEqualTo(WorkflowNode.UNKNOWN_TYPE);
        assertThat(a.outputType()).isNotEqualTo(WorkflowNode.UNKNOWN_TYPE);
    }

    // --------------------------------------------------------------------------------------------

    private static final class UpperCase extends RichMapFunction<String, String> {
        private Counter total;

        @Override
        public void open(OpenContext ctx) {
            total = getRuntimeContext().getMetricGroup().counter("total");
        }

        @Override
        public String map(String value) {
            total.inc();
            return value.toUpperCase(Locale.ROOT);
        }
    }

    private static final class Shouter extends ProcessFunction<String, String> {
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            if (value.endsWith("A")) {
                ctx.output(SHOUT, value);
            }
            out.collect(value);
        }
    }

    private static final class KeyedCount extends KeyedProcessFunction<String, String, String> {
        private transient ValueState<Long> count;

        @Override
        public void open(OpenContext ctx) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            long cur = count.value() == null ? 0 : count.value();
            count.update(cur + 1);
            out.collect(value + "#" + (cur + 1) + "(" + ctx.getCurrentKey() + ")");
        }
    }

    private static final class FirstCharKey implements KeySelector<String, String> {
        @Override
        public String getKey(String value) {
            return value.substring(0, 1).toUpperCase(Locale.ROOT);
        }
    }
}