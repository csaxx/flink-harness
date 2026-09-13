package org.flink.harness.graph.function.single;

import org.apache.flink.api.common.functions.MapFunction;
import org.flink.harness.graph.function.rich.RichMapFunctionHarness;

/**
 * Harness wrapping a non-rich {@link MapFunction}. A {@code null} map result produces no
 * output (same deliberate deviation as {@link RichMapFunctionHarness}).
 */
public final class MapFunctionHarness
        extends AbstractSingleStreamFunctionHarness<MapFunction<Object, Object>> {

    @SuppressWarnings("unchecked")
    public MapFunctionHarness(String id, MapFunction<?, ?> function) {
        super(id, (MapFunction<Object, Object>) function, "map");
    }

    @Override
    protected void invoke(Object element) throws Exception {
        Object mapped = getFunction().map(element);
        if (mapped != null) {
            collector().collect(mapped);
        }
    }
}
