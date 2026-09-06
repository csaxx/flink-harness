package org.flink.harness.timer;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.flink.harness.Mode;
import org.flink.harness.StandaloneWorkflow;
import org.flink.harness.WorkflowBuilder;
import org.flink.harness.result.WorkflowResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeyedTimerIntegrationTest {

    private static final OutputTag<String> SIDE = new OutputTag<>("side") {};

    // -----------------------------------------------------------------------
    // OPPORTUNISTIC mode tests
    // -----------------------------------------------------------------------

    @Test
    void processingTimerFiresAfterElement() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("proc", new TimerRegisteringFunction(50),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out")
                .addKeyedEdge("in", "proc", new FirstCharKey())
                .addEdge("proc", "out")
                .build(true);

        // timer set for ts=50; after processing the element, OPPORTUNISTIC fires it
        WorkflowResult result = wf.process(List.of("hello"), "in");
        assertThat(result.outputsOf("out")).contains("fired:50");
        wf.close();
    }

    @Test
    void processingTimerDoesNotFireBeforeTimestamp() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("proc", new TimerRegisteringFunction(System.currentTimeMillis() + 99999),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out")
                .addKeyedEdge("in", "proc", new FirstCharKey())
                .addEdge("proc", "out")
                .build(true);

        WorkflowResult result = wf.process(List.of("hello"), "in");
        assertThat(result.outputsOf("out")).doesNotContain("fired:");
        wf.close();
    }

    @Test
    void onTimerCanProduceSideOutputsAndMainOutputs() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("proc", new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) {
                        ctx.timerService().registerProcessingTimeTimer(1);
                        out.collect(value);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
                        ctx.output(SIDE, "side:" + ctx.getCurrentKey());
                        out.collect("timer:" + timestamp);
                    }
                }, TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("mainOut")
                .addSink("sideOut")
                .addKeyedEdge("in", "proc", new FirstCharKey())
                .addEdge("proc", "mainOut")
                .addSinkEdge("proc", "sideOut", SIDE)
                .build(true);

        WorkflowResult result = wf.process(List.of("alice"), "in");
        assertThat(result.outputsOf("mainOut")).contains("alice", "timer:1");
        assertThat(result.outputsOf("sideOut")).contains("side:A");
        wf.close();
    }

    @Test
    void timerCorrectlyBindsKeyAndState() {
StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("proc", new CountingTimerFunction(),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out")
                .addKeyedEdge("in", "proc", new FirstCharKey())
                .addEdge("proc", "out")
                .build(true);

        // First call processes "apple" for key A, fires timer, produces output including timer
        wf.process(List.of("apple"), "in");
        // Second call processes "banana" for key B, fires timer
        WorkflowResult r = wf.process(List.of("banana"), "in");
        // The result contains outputs from this call only: the element and key B's timer
        assertThat(r.outputsOf("out")).contains("elem:banana:B:1", "timer:B:1");
        wf.close();
    }

    @Test
    void timerCanRegisterAnotherTimerFromOnTimer() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("proc", new KeyedProcessFunction<String, String, String>() {
                    private int count = 0;

                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) {
                        ctx.timerService().registerProcessingTimeTimer(1);
                        out.collect("in:" + value);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
                        count++;
                        out.collect("chain:" + count + "@" + timestamp);
                        if (count < 3) {
                            ctx.timerService().registerProcessingTimeTimer(timestamp + 1);
                        }
                    }
                }, TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out")
                .addKeyedEdge("in", "proc", new FirstCharKey())
                .addEdge("proc", "out")
                .build(true);

        WorkflowResult result = wf.process(List.of("hello"), "in");
        assertThat(result.outputsOf("out")).contains(
                "in:hello", "chain:1@1", "chain:2@2", "chain:3@3");
        wf.close();
    }

    // -----------------------------------------------------------------------
    // MANUAL mode tests
    // -----------------------------------------------------------------------

    @Test
    void manualModeTimersOnlyFireOnExplicitCall() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .setProcessingTimerMode(ProcessingTimerMode.MANUAL)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("proc", new TimerRegisteringFunction(500),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out")
                .addKeyedEdge("in", "proc", new FirstCharKey())
                .addEdge("proc", "out")
                .build(true);

        WorkflowResult first = wf.process(List.of("hello"), "in");
        assertThat(first.outputsOf("out")).doesNotContain("fired:500");
        assertThat(first.outputsOf("out")).contains("hello");

        WorkflowResult fired = wf.getTimerService().fireProcessingTimers();
        assertThat(fired.outputsOf("out")).contains("fired:500");
        assertThat(wf.getTimerService().pendingTimerCount()).isEqualTo(0);
        wf.close();
    }

    @Test
    void manualFireProcessingTimersReturnsEmptyWhenNoTimersDue() {
        long farFuture = System.currentTimeMillis() + 99999;
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .setProcessingTimerMode(ProcessingTimerMode.MANUAL)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("proc", new TimerRegisteringFunction(farFuture),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out")
                .addKeyedEdge("in", "proc", new FirstCharKey())
                .addEdge("proc", "out")
                .build(true);

        wf.process(List.of("hello"), "in");
        WorkflowResult fired = wf.getTimerService().fireProcessingTimers();
        assertThat(fired.outputsOf("out")).isEmpty();
        assertThat(wf.getTimerService().pendingTimerCount()).isEqualTo(1);
        wf.close();
    }

    // -----------------------------------------------------------------------
    // BACKGROUND mode tests
    // -----------------------------------------------------------------------

    @Test
    void backgroundModeDeliversFiredTimersViaListener() throws InterruptedException {
        List<WorkflowResult> received = new CopyOnWriteArrayList<>();
        List<String> errors = new CopyOnWriteArrayList<>();

        long fireAt = System.currentTimeMillis() + 150;

        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .setProcessingTimerMode(ProcessingTimerMode.BACKGROUND,
                        new BackgroundTimerListener() {
                            @Override
                            public void onResult(WorkflowResult result) {
                                received.add(result);
                            }

                            @Override
                            public void onError(String nodeId, Throwable error) {
                                errors.add(error.getMessage());
                            }
                        })
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("proc", new TimerRegisteringFunction(fireAt),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out")
                .addKeyedEdge("in", "proc", new FirstCharKey())
                .addEdge("proc", "out")
                .build(true);

        wf.process(List.of("hello"), "in");

        Thread.sleep(1000);
        assertThat(received).isNotEmpty();
        assertThat(received.get(0).outputsOf("out"))
                .anyMatch(s -> ((String) s).startsWith("fired:"));
        wf.close();
    }

    @Test
    void backgroundModeDeliversErrorOnTimerFailure() throws InterruptedException {
        List<String> errors = new CopyOnWriteArrayList<>();

        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .setProcessingTimerMode(ProcessingTimerMode.BACKGROUND,
                        new BackgroundTimerListener() {
                            @Override
                            public void onResult(WorkflowResult result) {
                            }

                            @Override
                            public void onError(String nodeId, Throwable error) {
                                errors.add(error.getMessage());
                            }
                        })
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("failProc", new FailingOnTimerFunction(50),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out")
                .addKeyedEdge("in", "failProc", new FirstCharKey())
                .addEdge("failProc", "out")
                .build(true);

        wf.process(List.of("hello"), "in");

        Thread.sleep(1500);
        assertThat(errors).isNotEmpty();
        wf.close();
    }

    // -----------------------------------------------------------------------
    // TRANSIENT mode tests
    // -----------------------------------------------------------------------

    @Test
    void transientModeRegisterProcessingTimeTimerThrowsUoe() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.TRANSIENT)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("proc", new TimerRegisteringFunction(50),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out")
                .addKeyedEdge("in", "proc", new FirstCharKey())
                .addEdge("proc", "out")
                .build(true);

        assertThatThrownBy(() -> wf.process(List.of("hello"), "in"))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("processElement failed");
        wf.close();
    }

    @Test
    void transientModeWithExplicitTimerModeFailsAtBuild() {
        assertThatThrownBy(() -> new WorkflowBuilder(Mode.TRANSIENT)
                .setProcessingTimerMode(ProcessingTimerMode.MANUAL)
                .addSource("in")
                .registerKeyedFunction("proc", new TimerRegisteringFunction(50))
                .addSink("out")
                .addKeyedEdge("in", "proc", new FirstCharKey())
                .addEdge("proc", "out")
                .build(true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("TRANSIENT");
    }

    // -----------------------------------------------------------------------
    // Misc tests
    // -----------------------------------------------------------------------

    @Test
    void pendingTimerCountIsCorrectAcrossModes() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .setProcessingTimerMode(ProcessingTimerMode.MANUAL)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("proc", new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) {
                        ctx.timerService().registerProcessingTimeTimer(100);
                        ctx.timerService().registerProcessingTimeTimer(200);
                        out.collect(value);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
                        out.collect("fired:" + timestamp);
                    }
                }, TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out")
                .addKeyedEdge("in", "proc", new FirstCharKey())
                .addEdge("proc", "out")
                .build(true);

        wf.process(List.of("hello"), "in");
        assertThat(wf.getTimerService().pendingTimerCount()).isEqualTo(2);

        wf.getTimerService().fireProcessingTimers();
        assertThat(wf.getTimerService().pendingTimerCount()).isEqualTo(0);
        wf.close();
    }

    @Test
    void clearStateAlsoClearsTimers() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .setProcessingTimerMode(ProcessingTimerMode.MANUAL)
                .addSource("in", TypeInformation.of(String.class))
                .registerKeyedFunction("proc", new TimerRegisteringFunction(100),
                        TypeInformation.of(String.class), TypeInformation.of(String.class))
                .addSink("out")
                .addKeyedEdge("in", "proc", new FirstCharKey())
                .addEdge("proc", "out")
                .build(true);

        wf.process(List.of("hello"), "in");
        wf.clearState("proc");
        assertThat(wf.getTimerService().pendingTimerCount()).isEqualTo(0);
        wf.close();
    }

    // -----------------------------------------------------------------------
    // test helper functions
    // -----------------------------------------------------------------------

    static class TimerRegisteringFunction extends KeyedProcessFunction<String, String, String> {
        private final long timerTimestamp;

        TimerRegisteringFunction(long timerTimestamp) {
            this.timerTimestamp = timerTimestamp;
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
            out.collect("fired:" + timestamp);
        }
    }

    static class CountingTimerFunction extends KeyedProcessFunction<String, String, String> {
        private transient ValueState<Long> count;

        @Override
        public void open(OpenContext ctx) throws Exception {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            long c = count.value() == null ? 0 : count.value();
            count.update(c + 1);
            ctx.timerService().registerProcessingTimeTimer(1);
            out.collect("elem:" + value + ":" + ctx.getCurrentKey() + ":" + (c + 1));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            long c = count.value() == null ? 0 : count.value();
            out.collect("timer:" + ctx.getCurrentKey() + ":" + c);
        }
    }

    static class FailingOnTimerFunction extends KeyedProcessFunction<String, String, String> {
        private final long timerTimestamp;

        FailingOnTimerFunction(long timerTimestamp) {
            this.timerTimestamp = timerTimestamp;
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            throw new RuntimeException("intentional onTimer failure");
        }
    }

    static class FirstCharKey implements KeySelector<String, String> {
        @Override
        public String getKey(String value) {
            return value.substring(0, 1).toUpperCase();
        }
    }
}