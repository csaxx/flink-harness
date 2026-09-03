package org.flink.harness.result;

import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * Outputs of a single function invocation.
 * @param <O> main output element type
 */
public record FunctionResult<O>(
        List<O> outputs,
        Map<OutputTag<?>, List<?>> sideOutputs,
        Map<String, Object> metrics) {

    /** Empty result for filters that drop the element. */
    public static <O> FunctionResult<O> empty() {
        return new FunctionResult<>(List.of(), Map.of(), Map.of());
    }
}