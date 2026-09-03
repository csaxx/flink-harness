package org.flink.harness.harness;

import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.OutputTag;
import org.flink.harness.result.FunctionResult;
import org.flink.harness.internal.RecordingCollector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Harness wrapping a non-keyed {@link ProcessFunction}. Context instantiated through the
 * function instance (permitted by Flink's inner-class pattern). Timers unsupported in v1.
 */
public final class ProcessFunctionHarness extends FunctionHarness {

    private final ProcessFunction<Object, Object> function;
    private final ProcessFunction<Object, Object>.Context context;
    private final List<Object> mainOutputs = new ArrayList<>();
    private final RecordingCollector<Object> mainCollector = new RecordingCollector<>(mainOutputs);
    private final Map<OutputTag<?>, List<Object>> sideOutputs = new LinkedHashMap<>();
    private final String inputType;
    private final String outputType;

    @SuppressWarnings("unchecked")
    public ProcessFunctionHarness(String id, ProcessFunction<?, ?> function, String inputType, String outputType) {
        super(id);
        this.function = (ProcessFunction<Object, Object>) function;
        this.inputType = inputType;
        this.outputType = outputType;
        this.context = this.function.new Context() {
            @Override
            public Long timestamp() {
                return null;
            }

            @Override
            public TimerService timerService() {
                throw new UnsupportedOperationException("timers are not supported in flink-harness v1");
            }

            @Override
            public <X> void output(OutputTag<X> outputTag, X value) {
                sideOutputs.computeIfAbsent(outputTag, t -> new ArrayList<>()).add(value);
            }
        };
    }

    @Override
    protected FunctionResult<?> invokeUnchecked(Object element) {
        mainOutputs.clear();
        sideOutputs.clear();
        try {
            function.processElement(element, context, mainCollector);
        } catch (Exception e) {
            throw new RuntimeException("processElement failed in " + getId(), e);
        }
        Map<OutputTag<?>, List<?>> sideCopy = new LinkedHashMap<>();
        sideOutputs.forEach((tag, list) -> sideCopy.put(tag, new ArrayList<>(list)));
        return new FunctionResult<>(List.copyOf(mainOutputs), sideCopy, metricsSnapshot());
    }

    @Override
    public org.apache.flink.api.common.functions.RichFunction unwrap() {
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