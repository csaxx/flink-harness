package org.flink.harness.graph.result;

import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * Aggregated outputs of a workflow run.
 *
 * @param functionResults per-function results — only functions that produced at least one
 *                        non-empty output, side output, or metrics snapshot during the run
 * @param aggregatedMetrics top-level flat metrics map (dot-separated name → value):
 *                          counters/meters/histogram counts summed across functions,
 *                          gauges last-wins (registration order)
 */
public record WorkflowResult(
        Map<String, FunctionResult<Object>> functionResults,
        Map<String, Object> aggregatedMetrics) {

    /** Convenience: outputs of one function (empty if absent/not produced). */
    public List<Object> outputsOf(String functionId) {
        FunctionResult<Object> r = functionResults.get(functionId);
        return r == null ? List.of() : r.outputs();
    }

    /** Convenience: side outputs of one function for a tag (empty if absent). */
    public List<Object> sideOutputsOf(String functionId, OutputTag<?> tag) {
        FunctionResult<Object> r = functionResults.get(functionId);
        if (r == null) {
            return List.of();
        }
        List<?> so = r.sideOutputs().get(tag);
        if (so == null) {
            return List.of();
        }
        @SuppressWarnings("unchecked")
        List<Object> casted = (List<Object>) so;
        return casted;
    }
}