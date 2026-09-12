package org.flink.harness.graph.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.metrics.MetricGroup;
import org.flink.harness.Edge;
import org.flink.harness.graph.result.FunctionResult;
import org.flink.harness.metrics.StandaloneOperatorMetricGroup;
import org.flink.harness.graph.StandaloneRuntimeContext;

import java.util.Map;

/**
 * Base of all Flink-function harnesses: wraps a {@link RichFunction}, wires its runtime
 * context, and handles the open/close lifecycle and metrics. Key and keyed-state
 * management lives in {@link AbstractRichFunctionHarness}; implements {@link NodeHarness}
 * so it participates in the workflow graph.
 *
 * @param <F> the wrapped Flink function type
 */
public abstract class FunctionHarness<F extends RichFunction> implements NodeHarness {

    private static final OpenContext OPEN_CONTEXT = new OpenContext() {};

    private final String id;
    private final F function;
    private final StandaloneRuntimeContext runtimeContext;
    private boolean opened;

    protected FunctionHarness(String id, F function, Map<String, String> globalJobParameters) {
        this.id = id;
        this.function = function;
        this.runtimeContext = new StandaloneRuntimeContext(id, globalJobParameters);
    }

    public final String getId() {
        return id;
    }

    public final F getFunction() {
        return function;
    }

    protected final StandaloneRuntimeContext runtimeContext() {
        return runtimeContext;
    }

    protected final boolean isOpened() {
        return opened;
    }

    /** Wires the runtime context then opens the function exactly once. Called eagerly after
     * workflow construction when configured, and lazily from {@link #processElement(Object, Edge)}
     * while the flag is not yet set. On the lazy path the first element's key is already bound
     * (see {@link AbstractRichFunctionHarness#processElement(Object, Edge)}), so open() may use
     * keyed state. */
    @Override
    public void open() {
        if (!opened) {
            function.setRuntimeContext(runtimeContext);
            try {
                function.open(OPEN_CONTEXT);
            } catch (Exception exception) {
                throw new RuntimeException("open() failed for " + id, exception);
            }
            opened = true;
        }
    }

    /** Closes only nodes that were actually opened; untriggered nodes are left alone. */
    @Override
    public void close() {
        if (opened) {
            try {
                function.close();
            } catch (Exception exception) {
                throw new RuntimeException("close() failed for " + id, exception);
            }
        }
    }

    @Override
    public void resetMetrics() {
        ((StandaloneOperatorMetricGroup) runtimeContext.getMetricGroup())
                .resetCounters();
    }

    @Override
    public Map<String, Object> metricsSnapshot() {
        return ((StandaloneOperatorMetricGroup) runtimeContext.getMetricGroup()).snapshot();
    }

    @Override
    public MetricGroup metricGroup() {
        return runtimeContext.getMetricGroup();
    }

    /** Per-element entry point: open lazily, then delegate to the subtype's invocation. */
    @Override
    public FunctionResult<?> processElement(Object element, Edge edge) {
        open();
        return processElement(element);
    }

    protected abstract FunctionResult<?> processElement(Object element);
}
