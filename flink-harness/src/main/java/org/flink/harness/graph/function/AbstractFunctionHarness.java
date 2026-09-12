package org.flink.harness.graph.function;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.metrics.MetricGroup;
import org.flink.harness.graph.DataStreamEdge;
import org.flink.harness.graph.StreamNode;
import org.flink.harness.graph.function.rich.AbstractRichFunctionHarness;
import org.flink.harness.graph.function.single.AbstractSingleStreamFunctionHarness;
import org.flink.harness.graph.result.FunctionResult;
import org.flink.harness.metrics.StandaloneOperatorMetricGroup;

import java.util.Map;

/**
 * Base of all Flink-function harnesses: wraps a {@link Function} and owns the node's
 * identity and metric group so the workflow can snapshot/reset metrics uniformly.
 * Lifecycle (open/close), runtime-context wiring and key management only exist for
 * rich functions and therefore live one level down in {@link AbstractRichFunctionHarness};
 * non-rich single-stream functions are served by {@link AbstractSingleStreamFunctionHarness}.
 * Implements {@link StreamNode} so it participates in the workflow graph.
 *
 * @param <F> the wrapped Flink function type
 */
public abstract class AbstractFunctionHarness<F extends Function> implements StreamNode {

    private final String id;
    private final F function;
    private final StandaloneOperatorMetricGroup metricGroup;

    protected AbstractFunctionHarness(String id, F function) {
        this.id = id;
        this.function = function;
        this.metricGroup = new StandaloneOperatorMetricGroup(id);
    }

    public final String getId() {
        return id;
    }

    public final F getFunction() {
        return function;
    }

    protected final StandaloneOperatorMetricGroup operatorMetricGroup() {
        return metricGroup;
    }

    @Override
    public void resetMetrics() {
        metricGroup.resetCounters();
    }

    @Override
    public Map<String, Object> metricsSnapshot() {
        return metricGroup.snapshot();
    }

    @Override
    public MetricGroup metricGroup() {
        return metricGroup;
    }

    /** Per-element entry point: no lifecycle here — rich subtypes override to bind the
     * edge key and open lazily before delegating to the same abstract invocation. */
    @Override
    public FunctionResult<?> processElement(Object element, DataStreamEdge edge) {
        return processElement(element);
    }

    protected abstract FunctionResult<?> processElement(Object element);
}
