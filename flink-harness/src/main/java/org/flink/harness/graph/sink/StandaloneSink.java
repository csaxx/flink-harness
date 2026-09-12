package org.flink.harness.graph.sink;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;
import org.flink.harness.graph.DataStreamEdge;
import org.flink.harness.graph.StreamNode;
import org.flink.harness.graph.RecordingCollector;
import org.flink.harness.metrics.StandaloneOperatorMetricGroup;
import org.flink.harness.graph.result.FunctionResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Synthetic sink node — subclasses override {@link #accept(Object, Collector)}.
 * Default implementation records the element as-is into the workflow result.
 *
 * <p>Sinks are terminal graph nodes: elements flow in, they are recorded, and
 * no outbound edges may attach.
 *
 * @param <IN> element type accepted by this sink
 */
public class StandaloneSink<IN> implements StreamNode {

    private boolean opened;
    private final StandaloneOperatorMetricGroup metricGroup =
            new StandaloneOperatorMetricGroup(getClass().getSimpleName());
    private final List<Object> outputs = new ArrayList<>();
    private final RecordingCollector<Object> collector = new RecordingCollector<>(outputs);

    /**
     * Override to transform, filter, or record elements. Default: pass through unchanged
     * (element is recorded in the workflow result). To filter, simply do not call
     * {@code collected.collect()}.
     */
    @SuppressWarnings("unchecked")
    protected void accept(IN element, Collector<Object> collected) throws Exception {
        collected.collect(element);
    }

    /** Lifecycle hook, called once on first input (or eagerly at {@code build()} if configured). */
    protected void init() /* intentionally empty */ {}

    /** Lifecycle hook, called on workflow close. */
    protected void dispose() /* intentionally empty */ {}

    /** Metrics group for this sink (useful for custom subclasses). */
    protected final MetricGroup getMetricGroup() {
        return metricGroup;
    }

    // ------------------------------------------------------------------------
    // StreamNode
    // -------------------------------------------------------------------

    /** Per-input entry: lazily init, cast to the declared input type, run {@link #accept}, and
     * return the elements collected by the sink to the workflow result. */
    @Override
    public final FunctionResult<?> processElement(Object element, DataStreamEdge inboundEdge) {
        open();
        outputs.clear();
        try {
            @SuppressWarnings("unchecked")
            IN typed = (IN) element;
            accept(typed, collector);
        } catch (Exception exception) {
            throw new RuntimeException("StandaloneSink accept failed", exception);
        }
        return new FunctionResult<>(List.copyOf(outputs), Map.of(), metricGroup.snapshot());
    }

    /** Runs init() once, on first input (or eagerly when the builder requests it). */
    @Override
    public final void open() {
        if (!opened) {
            init();
            opened = true;
        }
    }

    @Override
    public void close() {
        if (opened) {
            dispose();
            opened = false;
        }
    }

    @Override
    public Map<String, Object> metricsSnapshot() {
        return metricGroup.snapshot();
    }

    @Override
    public MetricGroup metricGroup() {
        return metricGroup;
    }
}