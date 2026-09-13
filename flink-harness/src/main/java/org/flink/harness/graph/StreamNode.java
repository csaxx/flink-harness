package org.flink.harness.graph;

import org.flink.harness.graph.function.AbstractFunctionHarness;
import org.flink.harness.graph.sink.StandaloneSink;
import org.flink.harness.graph.source.StandaloneSource;
import org.flink.harness.graph.result.FunctionResult;

/**
 * Minimal contract consumed by {@link org.flink.harness.StandaloneWorkflow}.
 * Implemented by {@link AbstractFunctionHarness} for Flink functions and by
 * {@link StandaloneSource} /
 * {@link StandaloneSink} for synthetic graph nodes.
 */
public interface StreamNode {

    FunctionResult<?> processElement(Object element, StreamEdge inboundEdge);

    /** Open eagerly at build time; no-op default for synthetic nodes. */
    default void open() {}

    default void close() {}

    default void resetState() {}

    default void resetMetrics() {}

    default void resetAll() {
        resetState();
        resetMetrics();
    }

    default boolean requiresKeyedEdge() {
        return false;
    }
}
