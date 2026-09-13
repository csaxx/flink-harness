package org.flink.harness.graph.function.single;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Harness wrapping a non-rich {@link FilterFunction}: the element passes through unchanged
 * when accepted, otherwise it is dropped.
 */
public final class FilterFunctionHarness
        extends AbstractSingleStreamFunctionHarness<FilterFunction<Object>> {

    @SuppressWarnings("unchecked")
    public FilterFunctionHarness(String id, FilterFunction<?> function) {
        super(id, (FilterFunction<Object>) function, "filter");
    }

    @Override
    protected void invoke(Object element) throws Exception {
        if (getFunction().filter(element)) {
            collector().collect(element);
        }
    }
}
