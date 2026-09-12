package org.flink.harness.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.flink.harness.Mode;
import org.flink.harness.StandaloneWorkflow;
import org.flink.harness.WorkflowBuilder;
import org.flink.harness.graph.function.single.FilterFunctionHarness;
import org.flink.harness.graph.function.single.FlatMapFunctionHarness;
import org.flink.harness.graph.function.single.MapFunctionHarness;
import org.flink.harness.graph.result.FunctionResult;
import org.flink.harness.graph.result.WorkflowResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AbstractSingleStreamFunctionHarnessTest {

    @Test
    @SuppressWarnings("unchecked")
    void mapTransformsAndDropsNullResult() {
        MapFunctionHarness harness = new MapFunctionHarness(
                "map", (MapFunction<String, String>) value ->
                        value.isEmpty() ? null : value.toUpperCase(Locale.ROOT));

        assertThat((List<Object>) harness.processElement("a", null).outputs()).containsExactly("A");
        assertThat((List<Object>) harness.processElement("", null).outputs()).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void flatMapEmitsViaCollector() {
        FlatMapFunctionHarness harness = new FlatMapFunctionHarness(
                "flatMap", (FlatMapFunction<String, String>) (value, out) -> {
                    for (String token : value.split(",")) {
                        out.collect(token);
                    }
                });

        FunctionResult<?> result = harness.processElement("a,b,c", null);
        assertThat((List<Object>) result.outputs()).containsExactly("a", "b", "c");
    }

    @Test
    @SuppressWarnings("unchecked")
    void filterPassesThroughOnlyWhenAccepted() {
        FilterFunctionHarness harness = new FilterFunctionHarness(
                "filter", (FilterFunction<String>) value -> value.startsWith("ok"));

        assertThat((List<Object>) harness.processElement("ok-1", null).outputs()).containsExactly("ok-1");
        assertThat((List<Object>) harness.processElement("no-1", null).outputs()).isEmpty();
    }

    @Test
    void failureWrapsWithOperationNameAndNodeId() {
        MapFunctionHarness harness = new MapFunctionHarness(
                "map", (MapFunction<String, String>) value -> {
                    throw new IllegalStateException("boom");
                });

        assertThatThrownBy(() -> harness.processElement("a", null))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("map failed in map")
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    /** Non-rich functions have no lifecycle: open()/close() stay StreamNode no-ops and the
     * wrapped function never observes a RuntimeContext. */
    @Test
    void noLifecycleForNonRichFunctions() {
        MapFunctionHarness harness = new MapFunctionHarness(
                "map", (MapFunction<String, String>) String::trim);

        harness.open();
        harness.close();
        assertThat(harness.metricsSnapshot()).isEmpty();
    }

    /** Proves the HarnessFactory dispatch: plain interfaces registered via WorkflowBuilder
     * run through the full graph. */
    @Test
    void plainFunctionsWorkEndToEndThroughWorkflowBuilder() {
        StandaloneWorkflow workflow = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in")
                .registerFunction("upper",
                        (MapFunction<String, String>) value -> value.toUpperCase(Locale.ROOT))
                .registerFunction("split", (FlatMapFunction<String, String>) (value, out) -> {
                    for (String token : value.split(",")) {
                        out.collect(token);
                    }
                })
                .registerFunction("nonEmpty", (FilterFunction<String>) value -> !value.isEmpty())
                .addSink("out")
                .addSourceEdge("in", "upper")
                .addEdge("upper", "split")
                .addEdge("split", "nonEmpty")
                .addEdge("nonEmpty", "out")
                .build(true);
        try {
            WorkflowResult result = workflow.process(List.of("a,,b"), "in");
            assertThat(result.outputsOf("out")).containsExactly("A", "B");
        } finally {
            workflow.close();
        }
    }

    /** Flink-faithful: a map on a keyed stream is legal — the key is simply never bound
     * because a non-rich function cannot observe keyed state. */
    @Test
    void keyedEdgeIntoPlainMapIsLegal() {
        KeySelector<String, String> firstChar = value -> value.substring(0, 1);
        StandaloneWorkflow workflow = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("in")
                .registerFunction("map", (MapFunction<String, String>) String::trim)
                .addSink("out")
                .addKeyedEdge("in", "map", firstChar)
                .addEdge("map", "out")
                .build(true);
        try {
            WorkflowResult result = workflow.process(List.of(" a "), "in");
            assertThat(result.outputsOf("out")).containsExactly("a");
        } finally {
            workflow.close();
        }
    }
}
