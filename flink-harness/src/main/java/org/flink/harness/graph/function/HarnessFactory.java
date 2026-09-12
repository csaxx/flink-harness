package org.flink.harness.graph.function;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.flink.harness.Mode;

public final class HarnessFactory {

    public static FunctionHarness create(String id, Object function) {
        return create(id, function, SystemClock.getInstance(), Mode.CONTINUOUS);
    }

    /** Single dispatch point from a user function instance to its harness. Keyed functions only
     * accept timer registration in CONTINUOUS mode; anything unsupported fails here at build time. */
    public static FunctionHarness create(String id, Object function, Clock clock, Mode mode) {
        if (function instanceof ProcessFunction<?, ?> p) {
            return new ProcessFunctionHarness(id, p);
        }
        if (function instanceof KeyedProcessFunction<?, ?, ?> k) {
            boolean allowTimers = mode == Mode.CONTINUOUS;
            return new KeyedProcessFunctionHarness(id, k, clock, allowTimers);
        }
        if (function instanceof RichMapFunction<?, ?> m) {
            return new RichFunctionHarness(id, m, RichFunctionHarness.Kind.MAP);
        }
        if (function instanceof RichFlatMapFunction<?, ?> f) {
            return new RichFunctionHarness(id, f, RichFunctionHarness.Kind.FLATMAP);
        }
        if (function instanceof RichFilterFunction<?> f) {
            return new RichFunctionHarness(id, f, RichFunctionHarness.Kind.FILTER);
        }
        if (function instanceof RichFunction) {
            throw new IllegalArgumentException(
                    "Unsupported function type for " + id + ": " + function.getClass().getName()
                            + " — flink-harness supports ProcessFunction, KeyedProcessFunction, "
                            + "RichMapFunction, RichFlatMapFunction, RichFilterFunction in v1");
        }
        throw new IllegalArgumentException(
                "Unsupported function type for " + id + ": " + function.getClass().getName()
                        + " (not a RichFunction). v1 supports Process/KeyProcess/Rich(Map|FlatMap|Filter)");
    }
}