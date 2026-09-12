package org.flink.harness.graph.function.single;

import org.apache.flink.api.common.functions.FlatMapFunction;

/**
 * Harness wrapping a non-rich {@link FlatMapFunction}; emissions go through the collector.
 */
public final class FlatMapFunctionHarness
        extends AbstractSingleStreamFunctionHarness<FlatMapFunction<Object, Object>> {

    @SuppressWarnings("unchecked")
    public FlatMapFunctionHarness(String id, FlatMapFunction<?, ?> function) {
        super(id, (FlatMapFunction<Object, Object>) function, "flatMap");
    }

    @Override
    protected void invoke(Object element) throws Exception {
        getFunction().flatMap(element, collector());
    }
}
