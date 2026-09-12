package org.flink.harness.graph.function;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.flink.harness.graph.RecordingCollector;
import org.flink.harness.graph.result.FunctionResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Harness wrapping a {@link RichFlatMapFunction}; emissions go through the collector.
 * Optionally keyed via edge selectors (keyed state works on keyed edges).
 */
public final class RichFlatMapFunctionHarness
        extends AbstractRichFunctionHarness<RichFlatMapFunction<Object, Object>> {

    private final List<Object> outputs = new ArrayList<>();
    private final RecordingCollector<Object> collector = new RecordingCollector<>(outputs);

    public RichFlatMapFunctionHarness(String id, RichFlatMapFunction<?, ?> function) {
        this(id, function, Map.of());
    }

    @SuppressWarnings("unchecked")
    public RichFlatMapFunctionHarness(
            String id, RichFlatMapFunction<?, ?> function, Map<String, String> globalJobParameters) {
        super(id, (RichFlatMapFunction<Object, Object>) function, globalJobParameters);
    }

    /** Reuses the per-node output buffer, so it must be cleared before every invocation. */
    @Override
    protected FunctionResult<?> processElement(Object element) {
        outputs.clear();
        try {
            getFunction().flatMap(element, collector);
        } catch (Exception exception) {
            throw new RuntimeException("flatMap failed in " + getId(), exception);
        }
        return new FunctionResult<>(List.copyOf(outputs), Map.of(), metricsSnapshot());
    }

    @Override
    public boolean requiresKeyedEdge() {
        return false;
    }
}
