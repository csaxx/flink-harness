package org.flink.harness.graph.sink;

import org.apache.flink.util.Collector;
import org.flink.harness.graph.DataStreamEdge;
import org.flink.harness.graph.StreamNode;
import org.flink.harness.graph.RecordingCollector;
import org.flink.harness.graph.result.FunctionResult;

import java.util.Map;

/**
 * Synthetic sink node — subclasses override {@link #accept(Object, Collector)}.
 * Default implementation records the element as-is into the workflow result.
 *
 * <p>Sinks are terminal graph nodes: elements flow in, they are recorded, and
 * no outbound edges may attach.
 *
 * <p>Deliberately has no metrics: unlike a Flink sink operator it is a synthetic
 * node, not a rich function, so it gets no metric group.
 *
 * @param <IN> element type accepted by this sink
 */
public class StandaloneSink<IN> implements StreamNode {

    private final RecordingCollector<Object> collector = new RecordingCollector<>();

    /**
     * Override to transform, filter, or record elements. Default: pass through unchanged
     * (element is recorded in the workflow result). To filter, simply do not call
     * {@code collected.collect()}. Lifecycle is deliberately absent.
     */
    @SuppressWarnings("unchecked")
    protected void accept(IN element, Collector<Object> collected) throws Exception {
        collected.collect(element);
    }

    // ------------------------------------------------------------------------
    // StreamNode
    // ------------------------------------------------------------------------

    /** Per-input entry: cast to the declared input type, run {@link #accept}, and
     * return the elements collected by the sink. Lifecycle is the inherited no-op default. */
    @Override
    public FunctionResult<?> processElement(Object element, DataStreamEdge inboundEdge) {
        collector.clear();
        try {
            @SuppressWarnings("unchecked")
            IN typed = (IN) element;
            accept(typed, collector);
        } catch (Exception exception) {
            throw new RuntimeException("StandaloneSink accept failed", exception);
        }
        return new FunctionResult<>(collector.recorded(), Map.of(), Map.of());
    }
}
