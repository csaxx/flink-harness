package org.flink.harness.graph.function.single;

import org.apache.flink.api.common.functions.MapFunction;
import org.flink.harness.graph.function.rich.RichMapFunctionHarness;
import org.flink.harness.graph.result.FunctionResult;

import java.util.Map;

/**
 * Harness wrapping a non-rich {@link MapFunction}. A {@code null} map result produces no
 * output (same deliberate deviation as {@link RichMapFunctionHarness}).
 */
public final class MapFunctionHarness
        extends AbstractSingleStreamFunctionHarness<MapFunction<Object, Object>> {

    @SuppressWarnings("unchecked")
    public MapFunctionHarness(String id, MapFunction<?, ?> function) {
        super(id, (MapFunction<Object, Object>) function);
    }

    /** Reuses the collector's per-node buffer, so it must be cleared before every invocation. */
    @Override
    protected FunctionResult<?> invoke(Object element) {
        collector().clear();
        try {
            Object mapped = getFunction().map(element);
            if (mapped != null) {
                collector().collect(mapped);
            }
        } catch (Exception exception) {
            throw new RuntimeException("map failed in " + getId(), exception);
        }
        return new FunctionResult<>(collector().recorded(), Map.of(), Map.of());
    }
}
