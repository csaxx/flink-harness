package org.flink.standalone;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.flink.harness.Mode;
import org.flink.harness.StandaloneWorkflow;
import org.flink.harness.WorkflowBuilder;
import org.flink.harness.graph.result.WorkflowResult;
import org.flink.test.DemoFunctions;

import java.util.List;

/**
 * Public entry point for building and running the demo order-processing pipeline
 * as a standalone workflow.
 */
public final class StandaloneRunner {

    private StandaloneRunner() {}

    /** Build the workflow graph (same topology as flink-test's DemoFunctions). Thread-safe by default. */
    public static StandaloneWorkflow buildWorkflow(Mode mode) {
        return new WorkflowBuilder(mode)
                .addSource("csv", TypeInformation.of(String.class))
                .registerFunction("parse", new DemoFunctions.ParseFn())
                .registerFunction("route", new DemoFunctions.RouteFn())
                .registerKeyedFunction("accum", new DemoFunctions.AccumulateFn())
                .registerFunction("report", new DemoFunctions.ReportFn())
                .addSink("accumOut")
                .addSink("reportOut")
                .addSink("rejectedOut")
                .addSourceEdge("csv", "parse")
                .addEdge("parse", "route")
                .addKeyedEdge("route", "accum", DemoFunctions.ParsedOrder::customer)
                .addEdge("route", "report")
                .addEdge("accum", "accumOut")
                .addEdge("report", "reportOut")
                .addSinkEdge("route", "rejectedOut", DemoFunctions.REJECTED_TAG)
                .build(true);
    }

    /**
     * Run the workflow with the given CSV lines and return the aggregated result.
     * @param csvLines raw order lines in CSV format
     * @param mode     execution mode (CONTINUOUS or TRANSIENT)
     * @return aggregated outputs, side outputs, and metrics
     */
    public static WorkflowResult run(List<String> csvLines, Mode mode) {
        StandaloneWorkflow wf = buildWorkflow(mode);
        try {
            return wf.process(csvLines, "csv");
        } finally {
            wf.close();
        }
    }
}