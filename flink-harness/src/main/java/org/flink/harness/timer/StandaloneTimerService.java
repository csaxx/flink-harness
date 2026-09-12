package org.flink.harness.timer;

import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.clock.Clock;

import java.util.function.Supplier;

public final class StandaloneTimerService implements TimerService {

    private final Clock clock;
    private final TimerHeap heap;
    private final Supplier<Object> currentKey;
    private final boolean keyed;
    private final boolean allowRegister;

    public StandaloneTimerService(
            Clock clock,
            TimerHeap heap,
            Supplier<Object> currentKey,
            boolean keyed,
            boolean allowRegister) {
        this.clock = clock;
        this.heap = heap;
        this.currentKey = currentKey;
        this.keyed = keyed;
        this.allowRegister = allowRegister;
    }

    @Override
    public long currentProcessingTime() {
        return clock.absoluteTimeMillis();
    }

    @Override
    public long currentWatermark() {
        return Long.MIN_VALUE;
    }

    /** Keyed-only, matching Flink; re-registering the same (key, timestamp) is a silent no-op. */
    @Override
    public void registerProcessingTimeTimer(long time) {
        if (!keyed || !allowRegister) {
            throw new UnsupportedOperationException(UNSUPPORTED_REGISTER_TIMER_MSG);
        }
        heap.tryRegister(currentKey.get(), time);
    }

    /** No event-time infrastructure exists yet (watermarks never advance) — see FUTURE.md. */
    @Override
    public void registerEventTimeTimer(long time) {
        throw new UnsupportedOperationException(
                "event-time timers are not supported yet — future work");
    }

    @Override
    public void deleteProcessingTimeTimer(long time) {
        if (!keyed || !allowRegister) {
            throw new UnsupportedOperationException(UNSUPPORTED_DELETE_TIMER_MSG);
        }
        heap.tryDelete(currentKey.get(), time);
    }

    @Override
    public void deleteEventTimeTimer(long time) {
        throw new UnsupportedOperationException(
                "event-time timers are not supported yet — future work");
    }
}