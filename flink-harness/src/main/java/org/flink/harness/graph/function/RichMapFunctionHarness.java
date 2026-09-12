package org.flink.harness.graph.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.flink.harness.graph.result.FunctionResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Harness wrapping a {@link RichMapFunction}. A {@code null} map result produces no output.
 * Optionally keyed via edge selectors (keyed state works on keyed edges).
 */
public final class RichMapFunctionHarness
        extends AbstractRichFunctionHarness<RichMapFunction<Object, Object>> {

    private final List<Object> outputs = new ArrayList<>();

    public RichMapFunctionHarness(String id, RichMapFunction<?, ?> function) {
        this(id, function, Map.of());
    }

    @SuppressWarnings("unchecked")
    public RichMapFunctionHarness(
            String id, RichMapFunction<?, ?> function, Map<String, String> globalJobParameters) {
        super(id, (RichMapFunction<Object, Object>) function, globalJobParameters);
    }

    /** Reuses the per-node output buffer, so it must be cleared before every invocation. */
    @Override
    protected FunctionResult<?> processElement(Object element) {
        outputs.clear();
        try {
            Object mapped = getFunction().map(element);
            if (mapped != null) {
                outputs.add(mapped);
            }
        } catch (Exception exception) {
            throw new RuntimeException("map failed in " + getId(), exception);
        }
        return new FunctionResult<>(List.copyOf(outputs), Map.of(), metricsSnapshot());
    }

    @Override
    public boolean requiresKeyedEdge() {
        return false;
    }
}
