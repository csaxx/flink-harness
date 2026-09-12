package org.flink.harness.graph.function;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.clock.Clock;
import org.flink.harness.Mode;
import org.flink.harness.graph.function.rich.*;
import org.flink.harness.graph.function.single.FilterFunctionHarness;
import org.flink.harness.graph.function.single.FlatMapFunctionHarness;
import org.flink.harness.graph.function.single.MapFunctionHarness;

import java.util.Map;

public final class HarnessFactory {

    /** Single dispatch point from a user function instance to its harness. Rich variants must be
     * checked before their plain super-interfaces (a RichMapFunction is also a MapFunction).
     * Keyed functions only accept timer registration in CONTINUOUS mode; anything unsupported
     * fails here at build time. */
    public static AbstractFunctionHarness<?> create(
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
        if (function instanceof MapFunction<?, ?> mapFunction) {
            return new MapFunctionHarness(id, mapFunction);
        }
        if (function instanceof FlatMapFunction<?, ?> flatMapFunction) {
            return new FlatMapFunctionHarness(id, flatMapFunction);
        }
        if (function instanceof FilterFunction<?> filterFunction) {
            return new FilterFunctionHarness(id, filterFunction);
        }
        throw new IllegalArgumentException(
                "Unsupported function type for " + id + ": " + function.getClass().getName()
                        + " — supported: ProcessFunction, KeyedProcessFunction, "
                        + "RichMapFunction, RichFlatMapFunction, RichFilterFunction, "
                        + "MapFunction, FlatMapFunction, FilterFunction");
    }
}
