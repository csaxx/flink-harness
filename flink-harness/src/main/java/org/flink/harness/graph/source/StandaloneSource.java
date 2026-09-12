package org.flink.harness.graph.source;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;
import org.flink.harness.Edge;
import org.flink.harness.graph.function.NodeHarness;
import org.flink.harness.graph.RecordingCollector;
import org.flink.harness.metrics.StandaloneOperatorMetricGroup;
import org.flink.harness.graph.result.FunctionResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Synthetic source node — subclasses override {@link #process(Object, Collector)}.
 * Default implementation is passthrough: emits the received element unchanged.
 *
 * @param <IN>  element type received via {@code process()}
 * @param <OUT> element type emitted downstream
 */
public class StandaloneSource<IN, OUT> implements NodeHarness {

    private boolean opened;
    private final StandaloneOperatorMetricGroup metricGroup =
            new StandaloneOperatorMetricGroup(getClass().getSimpleName());
    private final List<Object> outputs = new ArrayList<>();
    private final RecordingCollector<Object> collector = new RecordingCollector<>(outputs);

    /**
     * Override to transform or generate elements. Default: emit the input element as-is.
     * Any element pushed via {@code out.collect()} flows downstream via outbound edges.
     */
    @SuppressWarnings("unchecked")
    protected void process(IN element, Collector<Object> out) throws Exception {
        out.collect(element);
    }

    /** Lifecycle hook, called once on first input (or eagerly at {@code build()} if configured). */
    protected void init() /* intentionally empty */ {}

    /** Lifecycle hook, called on workflow close. */
    protected void dispose() /* intentionally empty */ {}

    /** Metrics group for this source (useful for custom subclasses). */
    protected final MetricGroup getMetricGroup() {
        return metricGroup;
    }

    // ------------------------------------------------------------------------
    // NodeHarness
    // ------------------------------------------------------------------------

    /** Per-input entry: lazily init, cast to the declared input type, run {@link #process}, and
     * return the emitted elements to the workflow. */
    @Override
    public final FunctionResult<?> processViaEdge(Object element, Edge inboundEdge) {
        ensureOpened();
        outputs.clear();
        try {
            @SuppressWarnings("unchecked")
            IN typed = (IN) element;
            process(typed, collector);
        } catch (Exception e) {
            throw new RuntimeException("StandaloneSource process failed", e);
        }
        return new FunctionResult<>(List.copyOf(outputs), Map.of(), metricGroup.snapshot());
    }

    @Override
    public final void openOnceEager() {
        ensureOpened();
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
    public MetricGroup unwrapMetricGroup() {
        return metricGroup;
    }

    @Override
    public Object unwrap() {
        return this;
    }

    // ------------------------------------------------------------------------

    /** Runs init() once, on first input (or eagerly when the builder requests it). */
    private void ensureOpened() {
        if (!opened) {
            init();
            opened = true;
        }
    }
}