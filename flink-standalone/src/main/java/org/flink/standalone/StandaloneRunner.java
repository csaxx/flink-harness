package org.flink.standalone;

import org.flink.harness.Mode;
import org.flink.harness.StandaloneWorkflow;
import org.flink.harness.WorkflowBuilder;
import org.flink.harness.WorkflowResult;
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
                .registerFunction("parse", new DemoFunctions.ParseFn())
                .registerFunction("route", new DemoFunctions.RouteFn())
                .registerKeyedFunction("accum", new DemoFunctions.AccumulateFn(),
                        DemoFunctions.ParsedOrder::customer)
                .registerFunction("report", new DemoFunctions.ReportFn())
                .addEdge("parse", "route")
                .addKeyedEdge("route", "accum", DemoFunctions.ParsedOrder::customer)
                .addEdge("route", "report")
                .activateOutput("accum")
                .activateOutput("report")
                .activateSideOutput("route", DemoFunctions.REJECTED_TAG)
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
            return wf.process(csvLines, "parse");
        } finally {
            wf.close();
        }
    }
}