package org.flink.harness.graph.function;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.clock.Clock;
import org.flink.harness.Mode;

import java.util.Map;

public final class HarnessFactory {

    /** Single dispatch point from a user function instance to its harness. Keyed functions only
     * accept timer registration in CONTINUOUS mode; anything unsupported fails here at build time. */
    public static FunctionHarness<?> create(
            String id,
            Object function,
            Clock clock,
            Mode mode,
            Map<String, String> globalJobParameters) {
        if (function instanceof ProcessFunction<?, ?> processFunction) {
            return new ProcessFunctionHarness(id, processFunction, globalJobParameters);
        }
        if (function instanceof KeyedProcessFunction<?, ?, ?> keyedProcessFunction) {
            boolean allowTimerRegistration = mode == Mode.CONTINUOUS;
            return new KeyedProcessFunctionHarness(
                    id, keyedProcessFunction, clock, allowTimerRegistration, globalJobParameters);
        }
        if (function instanceof RichMapFunction<?, ?> mapFunction) {
            return new RichMapFunctionHarness(id, mapFunction, globalJobParameters);
        }
        if (function instanceof RichFlatMapFunction<?, ?> flatMapFunction) {
            return new RichFlatMapFunctionHarness(id, flatMapFunction, globalJobParameters);
        }
        if (function instanceof RichFilterFunction<?> filterFunction) {
            return new RichFilterFunctionHarness(id, filterFunction, globalJobParameters);
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
