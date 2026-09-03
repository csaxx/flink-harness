package org.flink.harness.harness;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.flink.harness.FunctionResult;
import org.flink.harness.internal.RecordingCollector;

import java.util.ArrayList;
import java.util.List;

/**
 * Harness for rich single-IO functions: RichMap, RichFlatMap, RichFilter.
 * Optionally keyed via edge selectors; filter functions pass through elements.
 */
public final class RichFunctionHarness extends FunctionHarness {

    /** Supported rich function kinds. */
    public enum Kind { MAP, FLATMAP, FILTER }

    private final RichFunction function;
    private final Kind kind;
    private final List<Object> outputs = new ArrayList<>();
    private final RecordingCollector<Object> collector = new RecordingCollector<>(outputs);
    private final String inputType;
    private final String outputType;

    public RichFunctionHarness(String id, RichFunction function, Kind kind, boolean threadSafe, String inputType, String outputType) {
        super(id, threadSafe);
        this.function = function;
        this.kind = kind;
        this.inputType = inputType;
        this.outputType = outputType;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected FunctionResult<?> invokeUnchecked(Object element) {
        outputs.clear();
        try {
            switch (kind) {
                case MAP -> {
                    Object out = ((RichMapFunction<Object, Object>) function).map(element);
                    if (out != null) {
                        outputs.add(out);
                    }
                }
                case FLATMAP -> ((RichFlatMapFunction<Object, Object>) function).flatMap(element, collector);
                case FILTER -> {
                    if (((RichFilterFunction<Object>) function).filter(element)) {
                        outputs.add(element);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("rich function failed in " + getId(), e);
        }
        return new FunctionResult<>(List.copyOf(outputs), java.util.Map.of(), metricsSnapshot());
    }

    @Override
    public RichFunction unwrap() {
        return function;
    }

    @Override
    public boolean requiresKeyedEdge() {
        return false;
    }

    @Override
    public String inputTypeName() {
        return inputType;
    }

    @Override
    public String outputTypeName() {
        return outputType;
    }
}