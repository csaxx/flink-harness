package org.flink.standalone;

import org.flink.harness.Mode;
import org.flink.harness.StandaloneWorkflow;
import org.flink.harness.WorkflowNode;
import org.flink.harness.result.WorkflowResult;
import org.flink.test.DemoFunctions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("unchecked")
class StandaloneRunnerTest {

    private static final List<String> SAMPLE_LINES = List.of(
            "Alice,book,2,12.50",
            "Bob,pen,0,2.00",
            "Alice,notebook,1,8.00",
            "Charlie,eraser,3,1.50",
            "Bob,marker,4,3.00",
            "Alice,pen,1,5.00");

    @Test
    void runReturnsAccumOutputs() {
        WorkflowResult result = StandaloneRunner.run(SAMPLE_LINES, Mode.CONTINUOUS);
        List<?> accumOut = result.outputsOf("accumOut");
        assertThat((List<Object>) accumOut).containsExactly(
                "customer=Alice orders=1 total=25.00",
                "customer=Alice orders=2 total=33.00",
                "customer=Charlie orders=1 total=4.50",
                "customer=Bob orders=1 total=12.00",
                "customer=Alice orders=3 total=38.00");
    }

    @Test
    void runReturnsReportOutputs() {
        WorkflowResult result = StandaloneRunner.run(SAMPLE_LINES, Mode.CONTINUOUS);
        List<?> reportOut = result.outputsOf("reportOut");
        assertThat((List<Object>) reportOut).containsExactly(
                "product=book qty=2 total=25.00",
                "product=notebook qty=1 total=8.00",
                "product=eraser qty=3 total=4.50",
                "product=marker qty=4 total=12.00",
                "product=pen qty=1 total=5.00");
    }

    @Test
    void runReturnsSideOutputsInSink() {
        WorkflowResult result = StandaloneRunner.run(SAMPLE_LINES, Mode.CONTINUOUS);
        List<?> rejectedOut = result.outputsOf("rejectedOut");
        assertThat(rejectedOut).hasSize(1);
        DemoFunctions.ParsedOrder order = (DemoFunctions.ParsedOrder) rejectedOut.get(0);
        assertThat(order.customer()).isEqualTo("Bob");
        assertThat(order.product()).isEqualTo("pen");
        assertThat(order.quantity()).isEqualTo(0);
    }

    @Test
    void runReturnsMetrics() {
        WorkflowResult result = StandaloneRunner.run(SAMPLE_LINES, Mode.CONTINUOUS);
        assertThat(result.functionResults().keySet()).containsAll(
                List.of("parse", "route", "accum", "report"));
        assertThat(result.functionResults().get("parse").metrics())
                .containsEntry("parsedCount", 6L);
        assertThat(result.functionResults().get("route").metrics())
                .containsEntry("good", 5L);
        assertThat(result.functionResults().get("accum").metrics())
                .containsEntry("aggregated", 5L);
        assertThat(result.functionResults().get("report").metrics())
                .containsEntry("reported", 5L);
        assertThat(result.aggregatedMetrics()).containsEntry("parsedCount", 6L)
                .containsEntry("good", 5L)
                .containsEntry("aggregated", 5L)
                .containsEntry("reported", 5L);
    }

    @Test
    void continuousAccumulatesAcrossProcessCalls() {
        StandaloneWorkflow wf = StandaloneRunner.buildWorkflow(Mode.CONTINUOUS);
        try {
            WorkflowResult first = wf.process(List.of("Alice,book,2,12.50"), "csv");
            assertThat(first.outputsOf("accumOut")).containsExactly(
                    "customer=Alice orders=1 total=25.00");

            WorkflowResult second = wf.process(List.of("Alice,pen,1,5.00"), "csv");
            assertThat(second.outputsOf("accumOut")).containsExactly(
                    "customer=Alice orders=2 total=30.00");
        } finally {
            wf.close();
        }
    }

    @Test
    void transientModeClearsStateBetweenRuns() {
        StandaloneWorkflow wf = StandaloneRunner.buildWorkflow(Mode.TRANSIENT);
        try {
            WorkflowResult first = wf.process(List.of("Alice,book,2,12.50", "Alice,pen,1,5.00"), "csv");
            assertThat(first.outputsOf("accumOut")).containsExactly(
                    "customer=Alice orders=1 total=25.00",
                    "customer=Alice orders=2 total=30.00");

            WorkflowResult second = wf.process(List.of("Alice,book,2,12.50", "Alice,pen,1,5.00"), "csv");
            assertThat(second.outputsOf("accumOut")).containsExactly(
                    "customer=Alice orders=1 total=25.00",
                    "customer=Alice orders=2 total=30.00");
        } finally {
            wf.close();
        }
    }

    @Test
    void clearStateResetsAccumulator() {
        StandaloneWorkflow wf = StandaloneRunner.buildWorkflow(Mode.CONTINUOUS);
        try {
            wf.process(List.of("Alice,book,2,12.50"), "csv");
            wf.clearState("accum");
            WorkflowResult afterClear = wf.process(List.of("Alice,pen,1,5.00"), "csv");
            assertThat(afterClear.outputsOf("accumOut")).containsExactly(
                    "customer=Alice orders=1 total=5.00");
        } finally {
            wf.close();
        }
    }

    @Test
    void clearStateAllResetsEverything() {
        StandaloneWorkflow wf = StandaloneRunner.buildWorkflow(Mode.CONTINUOUS);
        try {
            wf.process(List.of("Alice,book,2,12.50", "Bob,pen,1,3.00"), "csv");
            wf.clearStateAll();
            WorkflowResult afterClear = wf.process(List.of("Alice,pen,1,5.00"), "csv");
            assertThat(afterClear.outputsOf("accumOut")).containsExactly(
                    "customer=Alice orders=1 total=5.00");
        } finally {
            wf.close();
        }
    }

    @Test
    void clearMetricsResetsCounters() {
        StandaloneWorkflow wf = StandaloneRunner.buildWorkflow(Mode.CONTINUOUS);
        try {
            wf.process(List.of("Alice,book,2,12.50"), "csv");
            wf.clearMetrics("parse");
            WorkflowResult afterClear = wf.process(List.of("Bob,pen,1,3.00"), "csv");
            assertThat(afterClear.functionResults().get("parse").metrics().get("parsedCount")).isEqualTo(1L);
        } finally {
            wf.close();
        }
    }

    @Test
    void getWorkflowReturnsDagWithTypesAndKinds() {
        StandaloneWorkflow wf = StandaloneRunner.buildWorkflow(Mode.CONTINUOUS);
        var nodes = wf.getWorkflow();
        // 8 nodes: csv(source) + parse/route/accum/report(functions) + accumOut/reportOut/rejectedOut(sinks)
        assertThat(nodes).hasSize(8);

        assertThat(nodes.stream().filter(n -> n.functionId().equals("csv")).findFirst().get().kind())
                .isEqualTo(WorkflowNode.Kind.SOURCE);

        assertThat(nodes.stream().filter(n -> n.functionId().equals("parse")).findFirst().get().successors())
                .containsExactly("route");
        assertThat(nodes.stream().filter(n -> n.functionId().equals("route")).findFirst().get().successors())
                .containsExactly("accum", "report", "rejectedOut");
        assertThat(nodes.stream().filter(n -> n.functionId().equals("accumOut")).findFirst().get().kind())
                .isEqualTo(WorkflowNode.Kind.SINK);
    }

    @Test
    void getNodeIdsReturnsAllIds() {
        StandaloneWorkflow wf = StandaloneRunner.buildWorkflow(Mode.CONTINUOUS);
        assertThat(wf.getNodeIds()).containsExactlyInAnyOrder(
                "csv", "parse", "route", "accum", "report", "accumOut", "reportOut", "rejectedOut");
    }

    @Test
    void runFromFile(@TempDir Path tempDir) throws IOException {
        Path inputFile = tempDir.resolve("input.csv");
        Files.write(inputFile, SAMPLE_LINES);
        List<String> lines = Files.readAllLines(inputFile);
        WorkflowResult result = StandaloneRunner.run(lines, Mode.CONTINUOUS);
        assertThat(result.outputsOf("accumOut")).isNotEmpty();
    }
}