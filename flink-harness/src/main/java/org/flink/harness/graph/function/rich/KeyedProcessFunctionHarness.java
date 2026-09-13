package org.flink.harness.graph.function.rich;

import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.flink.harness.graph.result.FunctionResult;
import org.flink.harness.graph.RecordingCollector;
import org.flink.harness.timer.StandaloneTimerService;
import org.flink.harness.timer.TimerHeap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Harness wrapping a {@link KeyedProcessFunction}: keyed access via the bound edge selector
 * and {@code #getCurrentKey()}. Supports processing-time timers.
 */
public final class KeyedProcessFunctionHarness
        extends AbstractRichFunctionHarness<KeyedProcessFunction<Object, Object, Object>> {

    private final KeyedProcessFunction<Object, Object, Object>.Context context;
    private final KeyedProcessFunction<Object, Object, Object>.OnTimerContext onTimerContext;
    private final RecordingCollector<Object> mainCollector = new RecordingCollector<>();
    private final Map<OutputTag<?>, List<Object>> sideOutputs = new LinkedHashMap<>();

    private final TimerHeap timerHeap = new TimerHeap();
    private final StandaloneTimerService timerService;
    private long onTimerTimestamp;

    public KeyedProcessFunctionHarness(String id, KeyedProcessFunction<?, ?, ?> function) {
        this(id, function, SystemClock.getInstance(), true, Map.of());
    }

    /** Contexts are created through the function instance because Flink declares Context and
     * OnTimerContext as non-static inner classes (same trick as Flink's own operators). The timer
     * service is shared by both contexts and honors {@code allowTimerRegistration}. */
    @SuppressWarnings("unchecked")
    public KeyedProcessFunctionHarness(
            String id,
            KeyedProcessFunction<?, ?, ?> function,
            Clock clock,
            boolean allowTimerRegistration,
            Map<String, String> globalJobParameters) {
        super(id, (KeyedProcessFunction<Object, Object, Object>) function, globalJobParameters);
        this.context = getFunction().new Context() {
            @Override
            public Long timestamp() {
                return null;
            }

            @Override
            public TimerService timerService() {
                return KeyedProcessFunctionHarness.this.timerService;
            }

            @Override
            public <X> void output(OutputTag<X> outputTag, X value) {
                sideOutputs.computeIfAbsent(outputTag, tag -> new ArrayList<>()).add(value);
            }

            @Override
            public Object getCurrentKey() {
                return currentKey();
            }
        };
        this.onTimerContext = getFunction().new OnTimerContext() {
            @Override
            public Long timestamp() {
                return onTimerTimestamp;
            }

            @Override
            public TimeDomain timeDomain() {
                return TimeDomain.PROCESSING_TIME;
            }

            @Override
            public TimerService timerService() {
                return KeyedProcessFunctionHarness.this.timerService;
            }

            @Override
            public <X> void output(OutputTag<X> outputTag, X value) {
                sideOutputs.computeIfAbsent(outputTag, tag -> new ArrayList<>()).add(value);
            }

            @Override
            public Object getCurrentKey() {
                return currentKey();
            }
        };
        this.timerService = new StandaloneTimerService(
                clock, timerHeap, this::currentKey, true, allowTimerRegistration);
    }

    /** Eager open happens before any element, so no key is bound yet: bind a dummy placeholder
     * key to keep state-handle registration in open() from failing. The lazy path already has
     * the first element's real key bound (key binding precedes lazy open), so it skips the
     * dummy. The dummy is replaced on the first keyed element. */
    @Override
    public void open() {
        if (!isOpened() && currentKey() == null) {
            setCurrentKey(new Object());
        }
        super.open();
    }

    /** Reuses the collector's per-node output buffer, so it must be cleared before every invocation. */
    @Override
    protected FunctionResult<?> processElement(Object element) {
        mainCollector.clear();
        sideOutputs.clear();
        try {
            getFunction().processElement(element, context, mainCollector);
        } catch (Exception exception) {
            throw new RuntimeException("processElement failed in " + getId(), exception);
        }
        return buildResult();
    }

    /** Workflow-only entry point for a due timer. Binds the timer's key and timestamp so state and
     * {@code ctx.getCurrentKey()} are correct inside {@code onTimer}. */
    public FunctionResult<?> fireTimer(TimerHeap.TimerEntry entry) {
        mainCollector.clear();
        sideOutputs.clear();
        setCurrentKey(entry.key());
        onTimerTimestamp = entry.timestamp();
        try {
            getFunction().onTimer(entry.timestamp(), onTimerContext, mainCollector);
        } catch (Exception exception) {
            throw new RuntimeException("onTimer failed in " + getId(), exception);
        }
        return buildResult();
    }

    public TimerHeap timerHeap() {
        return timerHeap;
    }

    public int timerHeapSize() {
        return timerHeap.size();
    }

    @Override
    public void resetState() {
        super.resetState();
        // timers are keyed state too: clearing state must not leave timers to fire later
        timerHeap.clear();
    }

    @Override
    public boolean requiresKeyedEdge() {
        return true;
    }

    private FunctionResult<?> buildResult() {
        Map<OutputTag<?>, List<?>> sideCopy = new LinkedHashMap<>();
        sideOutputs.forEach((tag, list) -> sideCopy.put(tag, new ArrayList<>(list)));
        return new FunctionResult<>(mainCollector.recorded(), sideCopy, metricsSnapshot());
    }
}
