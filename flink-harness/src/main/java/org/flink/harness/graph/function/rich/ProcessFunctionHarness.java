package org.flink.harness.graph.function.rich;

import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.OutputTag;
import org.flink.harness.graph.result.FunctionResult;
import org.flink.harness.graph.RecordingCollector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Harness wrapping a non-keyed {@link ProcessFunction}. Context instantiated through the
 * function instance (permitted by Flink's inner-class pattern). {@link TimerService}
 * returned by {@code ctx.timerService()} is query-only: {@code currentProcessingTime()}
 * and {@code currentWatermark()} work; register/delete throw UnsupportedOperationException
 * (matching Flink's non-keyed {@code ProcessOperator} behavior).
 */
public final class ProcessFunctionHarness
        extends AbstractRichFunctionHarness<ProcessFunction<Object, Object>> {

    private final ProcessFunction<Object, Object>.Context context;
    private final List<Object> mainOutputs = new ArrayList<>();
    private final RecordingCollector<Object> mainCollector = new RecordingCollector<>(mainOutputs);
    private final Map<OutputTag<?>, List<Object>> sideOutputs = new LinkedHashMap<>();

    public ProcessFunctionHarness(String id, ProcessFunction<?, ?> function) {
        this(id, function, Map.of());
    }

    /** Non-keyed functions get a query-only timer service, mirroring Flink where timers exist only
     * on keyed streams. Note {@code currentProcessingTime()} uses the wall clock, not the workflow
     * Clock — only the keyed service honors a custom clock. */
    @SuppressWarnings("unchecked")
    public ProcessFunctionHarness(
            String id, ProcessFunction<?, ?> function, Map<String, String> globalJobParameters) {
        super(id, (ProcessFunction<Object, Object>) function, globalJobParameters);
        final TimerService queryOnly = new TimerService() {
            @Override
            public long currentProcessingTime() {
                return System.currentTimeMillis();
            }

            @Override
            public long currentWatermark() {
                return Long.MIN_VALUE;
            }

            @Override
            public void registerProcessingTimeTimer(long time) {
                throw new UnsupportedOperationException(UNSUPPORTED_REGISTER_TIMER_MSG);
            }

            @Override
            public void registerEventTimeTimer(long time) {
                throw new UnsupportedOperationException(UNSUPPORTED_REGISTER_TIMER_MSG);
            }

            @Override
            public void deleteProcessingTimeTimer(long time) {
                throw new UnsupportedOperationException(UNSUPPORTED_DELETE_TIMER_MSG);
            }

            @Override
            public void deleteEventTimeTimer(long time) {
                throw new UnsupportedOperationException(UNSUPPORTED_DELETE_TIMER_MSG);
            }
        };
        this.context = getFunction().new Context() {
            @Override
            public Long timestamp() {
                return null;
            }

            @Override
            public TimerService timerService() {
                return queryOnly;
            }

            @Override
            public <X> void output(OutputTag<X> outputTag, X value) {
                sideOutputs.computeIfAbsent(outputTag, tag -> new ArrayList<>()).add(value);
            }
        };
    }

    /** Reuses the per-node output buffers, so they must be cleared before every invocation. */
    @Override
    protected FunctionResult<?> processElement(Object element) {
        mainOutputs.clear();
        sideOutputs.clear();
        try {
            getFunction().processElement(element, context, mainCollector);
        } catch (Exception exception) {
            throw new RuntimeException("processElement failed in " + getId(), exception);
        }
        Map<OutputTag<?>, List<?>> sideCopy = new LinkedHashMap<>();
        sideOutputs.forEach((tag, list) -> sideCopy.put(tag, new ArrayList<>(list)));
        return new FunctionResult<>(List.copyOf(mainOutputs), sideCopy, metricsSnapshot());
    }

    @Override
    public boolean requiresKeyedEdge() {
        return false;
    }
}
