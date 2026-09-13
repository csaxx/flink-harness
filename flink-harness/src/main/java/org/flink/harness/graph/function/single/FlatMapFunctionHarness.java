package org.flink.harness.graph.function.single;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.flink.harness.graph.result.FunctionResult;

import java.util.Map;

/**
 * Harness wrapping a non-rich {@link FlatMapFunction}; emissions go through the collector.
 */
public final class FlatMapFunctionHarness
        extends AbstractSingleStreamFunctionHarness<FlatMapFunction<Object, Object>> {

    @SuppressWarnings("unchecked")
    public FlatMapFunctionHarness(String id, FlatMapFunction<?, ?> function) {
        super(id, (FlatMapFunction<Object, Object>) function);
    }

    /** Reuses the collector's per-node buffer, so it must be cleared before every invocation. */
    @Override
    protected FunctionResult<?> invoke(Object element) {
        collector().clear();
        try {
            getFunction().flatMap(element, collector());
        } catch (Exception exception) {
            throw new RuntimeException("flatMap failed in " + getId(), exception);
        }
        return new FunctionResult<>(collector().recorded(), Map.of(), Map.of());
    }
}
