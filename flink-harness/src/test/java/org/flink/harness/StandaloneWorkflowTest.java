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
import org.flink.harness.result.WorkflowResult;
import org.flink.harness.source.StandaloneSource;
import org.flink.harness.sink.StandaloneSink;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

class StandaloneWorkflowTest {

    private static final OutputTag<String> SHOUT = new OutputTag<>("shout") {};

    @Test
    void multiStageFlowWithFanOutAndKeyedBranch() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerFunction("entry", new UpperCase(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .registerFunction("shouter", new Shouter(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .registerKeyedFunction("keyedCount", new KeyedCount(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("shoutOut", TypeInformation.of(String.class))
                .addSink("keyedOut", TypeInformation.of(String.class))
                .addSink("sideOut", TypeInformation.of(String.class))
                .addSourceEdge("in", "entry")
                .addEdge("entry", "shouter")
                .addKeyedEdge("entry", "keyedCount", new FirstCharKey())
                .addEdge("shouter", "shoutOut")
                .addEdge("keyedCount", "keyedOut")
                .addSinkEdge("shouter", "sideOut", SHOUT)
                .build();

        WorkflowResult result = wf.process(List.of("alpha", "apricot", "beta"), "in");

        assertThat(result.outputsOf("shoutOut")).containsExactly("ALPHA", "APRICOT", "BETA");
        assertThat(result.outputsOf("keyedOut"))
                .containsExactly("ALPHA#1(A)", "APRICOT#2(A)", "BETA#1(B)");
        // side output SHOUT routed to its sink
        assertThat(result.outputsOf("sideOut")).containsExactly("ALPHA", "BETA");
    }

    @Test
    void transientModeClearsStateBetweenRuns() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.TRANSIENT)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("keyedCount", new KeyedCount(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out")
                .addKeyedEdge("in", "keyedCount", new FirstCharKey())
                .addEdge("keyedCount", "out")
                .build(true);

        WorkflowResult first = wf.process(List.of("a"), "in");
        assertThat(first.outputsOf("out")).containsExactly("a#1(A)");
        WorkflowResult second = wf.process(List.of("a"), "in");
        assertThat(second.outputsOf("out")).containsExactly("a#1(A)");
    }

    @Test
    void continuousAccumulates() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("keyedCount", new KeyedCount(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out")
                .addKeyedEdge("in", "keyedCount", new FirstCharKey())
                .addEdge("keyedCount", "out")
                .build(true);

        wf.process(List.of("a"), "in");
        WorkflowResult second = wf.process(List.of("a"), "in");
        assertThat(second.outputsOf("out")).containsExactly("a#2(A)");

        wf.clearState("keyedCount");
        WorkflowResult third = wf.process(List.of("a"), "in");
        assertThat(third.outputsOf("out")).containsExactly("a#1(A)");
    }

    @Test
    void getWorkflowGraphReturnsTypesAndKindsAndSuccessors() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerFunction("a", new UpperCase(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .registerFunction("b", new Shouter(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out", TypeInformation.of(String.class))
                .addSourceEdge("in", "a")
                .addEdge("a", "b")
                .addEdge("b", "out")
                .build();

        List<WorkflowNode> graph = wf.getWorkflow();
        assertThat(graph).hasSize(4);
        WorkflowNode a = graph.stream().filter(n -> n.functionId().equals("a")).findFirst().orElseThrow();
        assertThat(a.successors()).containsExactly("b");
        assertThat(a.kind()).isEqualTo(WorkflowNode.Kind.FUNCTION);
        assertThat(a.inputType()).isNotEqualTo(WorkflowNode.UNKNOWN_TYPE);
        assertThat(a.outputType()).isNotEqualTo(WorkflowNode.UNKNOWN_TYPE);

        WorkflowNode src = graph.stream().filter(n -> n.functionId().equals("in")).findFirst().orElseThrow();
        assertThat(src.kind()).isEqualTo(WorkflowNode.Kind.SOURCE);

        WorkflowNode sink = graph.stream().filter(n -> n.functionId().equals("out")).findFirst().orElseThrow();
        assertThat(sink.kind()).isEqualTo(WorkflowNode.Kind.SINK);
    }

    @Test
    void concurrentContinuousWorkflowAccumulatesCorrectly() throws InterruptedException {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("keyedCount", new KeyedCount(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out")
                .addKeyedEdge("in", "keyedCount", new FirstCharKey())
                .addEdge("keyedCount", "out")
                .build(true);

        int threads = 8;
        int runsPerThread = 50;

        CountDownLatch start = new CountDownLatch(1);
        try (var pool = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < threads; i++) {
                pool.execute(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException ignored) {
                    }
                    for (int r = 0; r < runsPerThread; r++) {
                        wf.process(List.of("a"), "in");
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            pool.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
        }

        WorkflowResult latest = wf.process(List.of("a"), "in");
        List<Object> outs = latest.outputsOf("out");
        long expected = (long) threads * runsPerThread + 1;
        String out = (String) outs.get(0);
        assertThat(outs).hasSize(1);
        assertThat(out).isEqualTo("a#" + expected + "(A)");
        wf.close();
    }

    @Test
    void concurrentTransientWorkflowIsDeterministic() throws InterruptedException {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.TRANSIENT)
                .addSource("in", TypeInformation.of(String.class))
                .registerFunction("shout", new Shouter(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out", TypeInformation.of(String.class))
                .addSourceEdge("in", "shout")
                .addEdge("shout", "out")
                .build(true);

        int threads = 16;
        int runsPerThread = 40;

        CountDownLatch start = new CountDownLatch(1);
        try (var pool = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < threads; i++) {
                pool.execute(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException ignored) {
                    }
                    for (int r = 0; r < runsPerThread; r++) {
                        WorkflowResult result = wf.process(List.of("X"), "in");
                        // TRANSIENT reset happens inside the lock, before unlock
                        // → no concurrent reset can interfere mid-run
                        assertThat(result.outputsOf("out")).containsExactly("X");
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            pool.awaitTermination(20, java.util.concurrent.TimeUnit.SECONDS);
        }
        wf.close();
    }

    @Test
    void aggregatedMetricsSumCountersAcrossFunctions() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerFunction("a", new TotalCounter())
                .registerFunction("b", new TotalCounter())
                .addSink("bOut")
                .addSourceEdge("in", "a")
                .addEdge("a", "b")
                .addEdge("b", "bOut")
                .build(true);

        WorkflowResult result = wf.process(List.of("x", "y"), "in");
        assertThat(result.aggregatedMetrics()).containsEntry("total", 4L);
        wf.close();
    }

    @Test
    void aggregatedMetricsGaugeLastWins() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerFunction("a", new GaugeWriter(10))
                .registerFunction("b", new GaugeWriter(20))
                .addSink("bOut")
                .addSourceEdge("in", "a")
                .addEdge("a", "b")
                .addEdge("b", "bOut")
                .build(true);

        WorkflowResult result = wf.process(List.of("x"), "in");
        assertThat(result.aggregatedMetrics()).containsEntry("gauge", 20);
        wf.close();
    }

    // --------------------------------------------------------------------------------------------

    @Test
    void customSourceTransformsInput() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", new StandaloneSource<Integer, String>() {
                    @Override
                    protected void process(Integer element, Collector<Object> out) throws Exception {
                        out.collect("num=" + element);
                    }
                }, TypeInformation.of(Integer.class), TypeInformation.of(String.class))
                .registerFunction("upper", new UpperCase(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out")
                .addSourceEdge("in", "upper")
                .addEdge("upper", "out")
                .build(true);

        WorkflowResult result = wf.process(List.of(1, 2, 3), "in");
        assertThat(result.outputsOf("out")).containsExactly("NUM=1", "NUM=2", "NUM=3");
        wf.close();
    }

    @Test
    void customSinkTransformsOutput() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerFunction("upper", new UpperCase(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out", new StandaloneSink<String>() {
                    @Override
                    protected void accept(String element, Collector<Object> collected) throws Exception {
                        collected.collect("[[ " + element + " ]]");
                    }
                })
                .addSourceEdge("in", "upper")
                .addEdge("upper", "out")
                .build(true);

        WorkflowResult result = wf.process(List.of("hello", "world"), "in");
        assertThat(result.outputsOf("out")).containsExactly("[[ HELLO ]]", "[[ WORLD ]]");
        wf.close();
    }

    @Test
    void sinkFiltersElements() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(Integer.class))
                .addSink("out", new StandaloneSink<Integer>() {
                    @Override
                    protected void accept(Integer element, Collector<Object> collected) throws Exception {
                        if (element % 2 == 0) {
                            collected.collect(element);
                        }
                    }
                })
                .addSourceEdge("in", "out")
                .build(true);

        WorkflowResult result = wf.process(List.of(1, 2, 3, 4), "in");
        assertThat(result.outputsOf("out")).containsExactly(2, 4);
        wf.close();
    }

    @Test
    void sideOutputEdgeToSinkCollectsRejectedElements() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerFunction("splitter", new Shouter(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("mainOut")
                .addSink("sideOut")
                .addSourceEdge("in", "splitter")
                .addEdge("splitter", "mainOut")
                .addSinkEdge("splitter", "sideOut", SHOUT)
                .build(true);

        WorkflowResult result = wf.process(List.of("ALPHA", "APRICOT", "BETA"), "in");
        // SHOUT fires for elements ending with "A" (ALPHA and BETA, but APRICOT does not)
        assertThat(result.outputsOf("mainOut")).containsExactly("ALPHA", "APRICOT", "BETA");
        assertThat(result.outputsOf("sideOut")).containsExactly("ALPHA", "BETA");
        wf.close();
    }

    @Test
    void sideOutputEdgeToIntermediateFunction() {
        OutputTag<String> odds = new OutputTag<>("odds") {};
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(Integer.class))
                .registerFunction("oddSplit", new ProcessFunction<Integer, Integer>() {
                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Integer> out) {
                        if (value % 2 == 0) {
                            out.collect(value);
                        } else {
                            ctx.output(odds, value.toString());
                        }
                    }
                }, TypeInformation.of(Integer.class), TypeInformation.of(Integer.class))
                .registerFunction("toUpper", new UpperCase(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("evenOut")
                .addSink("oddOut")
                .addSourceEdge("in", "oddSplit")
                .addEdge("oddSplit", "evenOut")
                .addSideOutputEdge("oddSplit", "toUpper", odds)
                .addEdge("toUpper", "oddOut")
                .build(true);

        WorkflowResult result = wf.process(List.of(1, 2, 3, 4), "in");
        assertThat(result.outputsOf("evenOut")).containsExactly(2, 4);
        assertThat(result.outputsOf("oddOut")).containsExactly("1", "3");
        wf.close();
    }

    @Test
    void unknownSourceIdThrows() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerFunction("id", new Passive())
                .addSink("out")
                .addSourceEdge("in", "id")
                .addEdge("id", "out")
                .build(true);
        try {
            wf.process(List.of("x"), "nope");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("unknown source id");
        }
        wf.close();
    }

    @Test
    void customSourceMetricsAvailable() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", new StandaloneSource<String, String>() {
                    private Counter counter;
                    @Override
                    protected void init() {
                        counter = getMetricGroup().counter("sourceIn");
                    }
                    @Override
                    protected void process(String element, Collector<Object> out) throws Exception {
                        counter.inc();
                        out.collect(element);
                    }
                }, TypeInformation.of(String.class), TypeInformation.of(String.class))
                .registerFunction("id", new Passive())
                .addSink("out")
                .addSourceEdge("in", "id")
                .addEdge("id", "out")
                .build(true);

        WorkflowResult result = wf.process(List.of("x", "y"), "in");
        assertThat(result.functionResults().get("in").metrics()).containsEntry("sourceIn", 2L);
        wf.close();
    }

    @Test
    void sinkMetricsAvailable() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerFunction("id", new Passive())
                .addSink("out", new StandaloneSink<String>() {
                    private Counter counter;
                    @Override
                    protected void init() {
                        counter = getMetricGroup().counter("received");
                    }
                    @Override
                    protected void accept(String element, Collector<Object> collected) throws Exception {
                        counter.inc();
                        collected.collect(element);
                    }
                })
                .addSourceEdge("in", "id")
                .addEdge("id", "out")
                .build(true);

        WorkflowResult result = wf.process(List.of("a", "b", "c"), "in");
        assertThat(result.outputsOf("out")).containsExactly("a", "b", "c");
        assertThat(result.functionResults().get("out").metrics()).containsEntry("received", 3L);
        wf.close();
    }

    @Test
    void multiSourceMultiSinkWorkflow() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("srcA", TypeInformation.of(String.class))
                .addSource("srcB", TypeInformation.of(Integer.class))
                .addSink("outA")
                .addSink("outB")
                .addSourceEdge("srcA", "outA")
                .addSourceEdge("srcB", "outB")
                .build(true);

        WorkflowResult r1 = wf.process(List.of("x", "y"), "srcA");
        assertThat(r1.outputsOf("outA")).containsExactly("x", "y");
        assertThat(r1.outputsOf("outB")).isEmpty();

        WorkflowResult r2 = wf.process(List.of(1, 2, 3), "srcB");
        assertThat(r2.outputsOf("outA")).isEmpty();
        assertThat(r2.outputsOf("outB")).containsExactly(1, 2, 3);
        wf.close();
    }

    // --------------------------------------------------------------------------------------------
    // harness-level test helpers
    // --------------------------------------------------------------------------------------------

    private static final class TotalCounter extends ProcessFunction<Object, Object> {
        private Counter counter;
        @Override
        public void open(OpenContext ctx) {
            counter = getRuntimeContext().getMetricGroup().counter("total");
        }
        @Override
        public void processElement(Object value, Context ctx, Collector<Object> out) {
            counter.inc();
            out.collect(value);
        }
    }

    private static final class GaugeWriter extends ProcessFunction<Object, Object> {
        private final int value;
        GaugeWriter(int value) { this.value = value; }
        @Override
        public void open(OpenContext ctx) {
            getRuntimeContext().getMetricGroup().gauge("gauge", new org.apache.flink.metrics.Gauge<Integer>() {
                @Override
                public Integer getValue() { return value; }
            });
        }
        @Override
        public void processElement(Object value, Context ctx, Collector<Object> out) {
            out.collect(value);
        }
    }

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

    private static final class Passive extends ProcessFunction<Object, Object> {
        @Override
        public void processElement(Object value, Context ctx, Collector<Object> out) {
            out.collect(value);
        }
    }

    private static final class FirstCharKey implements KeySelector<String, String> {
        @Override
        public String getKey(String value) {
            return value.substring(0, 1).toUpperCase(Locale.ROOT);
        }
    }
}