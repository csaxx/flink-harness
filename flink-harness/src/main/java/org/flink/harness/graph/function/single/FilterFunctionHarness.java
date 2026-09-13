package org.flink.harness.graph.function.single;

import org.apache.flink.api.common.functions.FilterFunction;
import org.flink.harness.graph.result.FunctionResult;

import java.util.Map;

/**
 * Harness wrapping a non-rich {@link FilterFunction}: the element passes through unchanged
 * when accepted, otherwise it is dropped.
 */
public final class FilterFunctionHarness
        extends AbstractSingleStreamFunctionHarness<FilterFunction<Object>> {

    @SuppressWarnings("unchecked")
    public FilterFunctionHarness(String id, FilterFunction<?> function) {
        super(id, (FilterFunction<Object>) function);
    }

    /** Reuses the collector's per-node buffer, so it must be cleared before every invocation. */
    @Override
    protected FunctionResult<?> invoke(Object element) {
        collector().clear();
        try {
            if (getFunction().filter(element)) {
                collector().collect(element);
            }
        } catch (Exception exception) {
            throw new RuntimeException("filter failed in " + getId(), exception);
        }
        return new FunctionResult<>(collector().recorded(), Map.of(), Map.of());
    }
}
