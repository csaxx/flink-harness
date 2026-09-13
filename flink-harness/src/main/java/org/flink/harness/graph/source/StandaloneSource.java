package org.flink.harness.graph.source;

import org.apache.flink.util.Collector;
import org.flink.harness.graph.DataStreamEdge;
import org.flink.harness.graph.StreamNode;
import org.flink.harness.graph.RecordingCollector;
import org.flink.harness.graph.result.FunctionResult;

import java.util.Map;

/**
 * Synthetic source node — subclasses override {@link #process(Object, Collector)}.
 * Default implementation is passthrough: emits the received element unchanged.
 *
 * @param <IN>  element type received via {@code process()}
 * @param <OUT> element type emitted downstream
 */
public class StandaloneSource<IN, OUT> implements StreamNode {

    private final RecordingCollector<Object> collector = new RecordingCollector<>();

    /**
     * Override to transform or generate elements. Default: emit the input element as-is.
     * Any element pushed via {@code out.collect()} flows downstream via outbound edges.
     */
    @SuppressWarnings("unchecked")
    protected void process(IN element, Collector<Object> out) throws Exception {
        out.collect(element);
    }

    // ------------------------------------------------------------------------
    // StreamNode
    // ------------------------------------------------------------------------

    /** Per-input entry: cast to the declared input type, run {@link #process}, and
     * return the emitted elements. Lifecycle is the inherited no-op default. */
    @Override
    public FunctionResult<?> processElement(Object element, DataStreamEdge inboundEdge) {
        collector.clear();
        try {
            @SuppressWarnings("unchecked")
            IN typed = (IN) element;
            process(typed, collector);
        } catch (Exception exception) {
            throw new RuntimeException("StandaloneSource process failed", exception);
        }
        return new FunctionResult<>(collector.recorded(), Map.of(), Map.of());
    }
}
