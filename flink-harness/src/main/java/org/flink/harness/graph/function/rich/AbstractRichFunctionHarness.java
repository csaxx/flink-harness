package org.flink.harness.graph.function.rich;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.MetricGroup;
import org.flink.harness.graph.DataStreamEdge;
import org.flink.harness.graph.StandaloneRuntimeContext;
import org.flink.harness.graph.function.AbstractFunctionHarness;
import org.flink.harness.graph.result.FunctionResult;
import org.flink.harness.metrics.StandaloneOperatorMetricGroup;
import org.flink.harness.state.InMemoryKeyedStateStore;

import java.util.Map;

/**
 * Base for harnesses wrapping an {@link AbstractRichFunction}. Adds the rich-function
 * lifecycle (runtime-context wiring plus exactly-once {@link #open()}/{@link #close()}),
 * key binding with keyed-state scoping, and the per-node metric group on top of
 * {@link AbstractFunctionHarness}: on a keyed edge the element's key is bound before
 * invocation, mirroring Flink where any rich function on a keyed stream may use keyed
 * state (only non-keyed streams throw). Metrics live here because only a rich function
 * receives a {@code RuntimeContext} and thus a metric group.
 *
 * @param <F> the wrapped Flink function type
 */
public abstract class AbstractRichFunctionHarness<F extends AbstractRichFunction>
        extends AbstractFunctionHarness<F> {

    private static final OpenContext OPEN_CONTEXT = new OpenContext() {};

    private final StandaloneRuntimeContext runtimeContext;
    private final InMemoryKeyedStateStore stateStore;
    private final StandaloneOperatorMetricGroup metricGroup;
    private Object currentKey;
    private boolean opened;

    protected AbstractRichFunctionHarness(
            String id, F function, Map<String, String> globalJobParameters) {
        super(id, function);
        this.metricGroup = new StandaloneOperatorMetricGroup(id);
        this.runtimeContext =
                new StandaloneRuntimeContext(id, globalJobParameters, metricGroup);
        this.stateStore = runtimeContext.stateStore();
    }

    protected final StandaloneRuntimeContext runtimeContext() {
        return runtimeContext;
    }

    public final StandaloneOperatorMetricGroup metricGroup() {
        return metricGroup;
    }

    public final Map<String, Object> metricsSnapshot() {
        return metricGroup.snapshot();
    }

    @Override
    public void resetMetrics() {
        metricGroup.resetCounters();
    }

    protected final boolean isOpened() {
        return opened;
    }

    protected final Object currentKey() {
        return currentKey;
    }

    protected final void setCurrentKey(Object key) {
        this.currentKey = key;
        stateStore.setCurrentKey(key);
    }

    protected final InMemoryKeyedStateStore stateStore() {
        return stateStore;
    }

    /** Wires the runtime context then opens the function exactly once. Called eagerly after
     * workflow construction when configured, and lazily from
     * {@link #processElement(Object, DataStreamEdge)} while the flag is not yet set. On the
     * lazy path the first element's key is already bound (see below), so open() may use
     * keyed state. */
    @Override
    public void open() {
        if (!opened) {
            getFunction().setRuntimeContext(runtimeContext);
            try {
                getFunction().open(OPEN_CONTEXT);
            } catch (Exception exception) {
                throw new RuntimeException("open() failed for " + getId(), exception);
            }
            opened = true;
        }
    }

    /** Closes only nodes that were actually opened; untriggered nodes are left alone. */
    @Override
    public void close() {
        if (opened) {
            try {
                getFunction().close();
            } catch (Exception exception) {
                throw new RuntimeException("close() failed for " + getId(), exception);
            }
        }
    }

    /** Order matters: bind the key first so a lazy {@link #open()} observes the element's
     * key when registering state handles. */
    @Override
    public FunctionResult<?> processElement(Object element, DataStreamEdge edge) {
        bindKey(element, edge);
        open();
        return invoke(element);
    }

    /** Confined raw cast for KeySelector invocation; failures identify the offending edge. */
    @SuppressWarnings("unchecked")
    private void bindKey(Object element, DataStreamEdge edge) {
        if (edge != null && edge.keyed()) {
            try {
                setCurrentKey(((KeySelector<Object, Object>) edge.keySelector()).getKey(element));
            } catch (Exception exception) {
                throw new RuntimeException(
                        "keySelector failed on edge " + edge.src() + "→" + edge.dst(), exception);
            }
        }
    }

    /** Drops every key's state and unbinds the current key (keyed subclasses also clear timers). */
    @Override
    public void resetState() {
        stateStore.clearAll();
        currentKey = null;
    }
}
