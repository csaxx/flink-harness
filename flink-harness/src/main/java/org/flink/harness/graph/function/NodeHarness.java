package org.flink.harness.graph.function;

import org.apache.flink.metrics.MetricGroup;
import org.flink.harness.Edge;
import org.flink.harness.graph.sink.StandaloneSink;
import org.flink.harness.graph.source.StandaloneSource;
import org.flink.harness.graph.result.FunctionResult;

import java.util.Map;

/**
 * Minimal contract consumed by {@link org.flink.harness.StandaloneWorkflow}.
 * Implemented by {@link FunctionHarness} for Flink functions and by
 * {@link StandaloneSource} /
 * {@link StandaloneSink} for synthetic graph nodes.
 *
 * <p>Public because package visibility does not cross packages; not part of the
 * consumer API — implement this only through the provided abstract classes.
 */
public interface NodeHarness {

    FunctionResult<?> processViaEdge(Object element, Edge inboundEdge);

    /** Open eagerly at build time; no-op default for synthetic nodes. */
    default void openOnceEager() {}

    default void close() {}

    default void clearState() {}

    default void clearMetrics() {}

    default void resetAll() {
        clearState();
        clearMetrics();
    }

    default boolean requiresKeyedEdge() {
        return false;
    }

    Map<String, Object> metricsSnapshot();

    MetricGroup unwrapMetricGroup();

    Object unwrap();
}