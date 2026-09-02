package org.flink.harness;

import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * Aggregated outputs of a workflow run.
 */
public record WorkflowResult(
        /** Main outputs per activateOutput'd function id. */
        Map<String, List<Object>> outputs,
        /** Side outputs per (functionId, tag) via activateSideOutput. */
        Map<SideOutputKey<?>, List<Object>> sideOutputs,
        /** Metrics snapshot per function id. */
        Map<String, Map<String, Object>> metrics) {

    /** Key for a side output channel. */
    public record SideOutputKey<X>(String functionId, OutputTag<X> tag) {}
}