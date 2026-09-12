package org.flink.harness.graph.function;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.flink.harness.graph.result.FunctionResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Harness wrapping a {@link RichFilterFunction}: the element passes through unchanged when
 * accepted, otherwise it is dropped. Optionally keyed via edge selectors (keyed state works
 * on keyed edges).
 */
public final class RichFilterFunctionHarness
        extends AbstractRichFunctionHarness<RichFilterFunction<Object>> {

    private final List<Object> outputs = new ArrayList<>();

    public RichFilterFunctionHarness(String id, RichFilterFunction<?> function) {
        this(id, function, Map.of());
    }

    @SuppressWarnings("unchecked")
    public RichFilterFunctionHarness(
            String id, RichFilterFunction<?> function, Map<String, String> globalJobParameters) {
        super(id, (RichFilterFunction<Object>) function, globalJobParameters);
    }

    /** Reuses the per-node output buffer, so it must be cleared before every invocation. */
    @Override
    protected FunctionResult<?> processElement(Object element) {
        outputs.clear();
        try {
            if (getFunction().filter(element)) {
                outputs.add(element);
            }
        } catch (Exception exception) {
            throw new RuntimeException("filter failed in " + getId(), exception);
        }
        return new FunctionResult<>(List.copyOf(outputs), Map.of(), metricsSnapshot());
    }

    @Override
    public boolean requiresKeyedEdge() {
        return false;
    }
}
