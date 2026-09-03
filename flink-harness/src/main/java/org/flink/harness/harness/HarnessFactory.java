package org.flink.harness.harness;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;

/** Factory into the right harness subtype per function class. Public for cross-package access from {@code WorkflowBuilder}; not part of the consumer API. */
public final class HarnessFactory {

    /** Create the appropriate harness for the given function instance. */
    public static FunctionHarness create(String id, Object function, String inputType, String outputType) {
        if (function instanceof ProcessFunction<?, ?> p) {
            return new ProcessFunctionHarness(id, p, inputType, outputType);
        }
        if (function instanceof KeyedProcessFunction<?, ?, ?> k) {
            return new KeyedProcessFunctionHarness(id, k, inputType, outputType);
        }
        if (function instanceof RichMapFunction<?, ?> m) {
            return new RichFunctionHarness(id, m, RichFunctionHarness.Kind.MAP, inputType, outputType);
        }
        if (function instanceof RichFlatMapFunction<?, ?> f) {
            return new RichFunctionHarness(id, f, RichFunctionHarness.Kind.FLATMAP, inputType, outputType);
        }
        if (function instanceof RichFilterFunction<?> f) {
            return new RichFunctionHarness(id, f, RichFunctionHarness.Kind.FILTER, inputType, outputType);
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