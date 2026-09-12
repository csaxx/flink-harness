package org.flink.harness.graph.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.MetricGroup;
import org.flink.harness.Edge;
import org.flink.harness.graph.result.FunctionResult;
import org.flink.harness.state.InMemoryKeyedStateStore;
import org.flink.harness.metrics.StandaloneOperatorMetricGroup;
import org.flink.harness.graph.StandaloneRuntimeContext;

import java.util.Map;

/**
 * Base of all Flink-function harnesses. Handles open/close lifecycle, wire runtime context,
 * key-binding. Implements {@link NodeHarness} so it participates in the workflow graph.
 */
public abstract class FunctionHarness implements NodeHarness {

    private static final OpenContext OPEN_CONTEXT = new OpenContext() {};

    private final String id;
    private final StandaloneRuntimeContext runtimeContext;
    private final InMemoryKeyedStateStore stateStore;
    private boolean opened;
    private Object currentKey;

    protected FunctionHarness(String id) {
        this.id = id;
        this.runtimeContext = new StandaloneRuntimeContext(id);
        this.stateStore = runtimeContext.stateStore();
    }

    public final String getId() {
        return id;
    }

    /** Wires the runtime context then opens the function exactly once. On the lazy path the first
     * element's key is already bound (see {@link #processViaEdge}), so open() may use keyed state. */
    public void openOnce() {
        if (!opened) {
            RichFunction fn = unwrap();
            fn.setRuntimeContext(runtimeContext);
            try {
                fn.open(OPEN_CONTEXT);
            } catch (Exception e) {
                throw new RuntimeException("open() failed for " + id, e);
            }
            opened = true;
        }
    }

    /** Open eagerly at build time. For keyed functions a dummy placeholder key is bound
     * so state handle registration in open() cannot fail. Safe no-op if already opened. */
    @Override
    public void openOnceEager() {
        if (!opened && requiresKeyedEdge()) {
            stateStore.setCurrentKey(new Object());
        }
        openOnce();
    }

    /** Closes only nodes that were actually opened; untriggered nodes are left alone. */
    @Override
    public void close() {
        if (opened) {
            try {
                unwrap().close();
            } catch (Exception e) {
                throw new RuntimeException("close() failed for " + id, e);
            }
        }
    }

    /** Drops every key's state and unbinds the current key (keyed subclasses also clear timers). */
    @Override
    public void clearState() {
        stateStore.clearAll();
        currentKey = null;
    }

    @Override
    public void clearMetrics() {
        ((StandaloneOperatorMetricGroup) runtimeContext.getMetricGroup())
                .resetCounters();
    }

    @Override
    public void resetAll() {
        clearState();
        clearMetrics();
    }

    @Override
    public Map<String, Object> metricsSnapshot() {
        return ((StandaloneOperatorMetricGroup) runtimeContext.getMetricGroup()).snapshot();
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

    /** Per-element entry point. Order matters: bind the key first so a lazy {@link #openOnce()}
     * observes the element's key when registering state handles. */
    @Override
    public final FunctionResult<?> processViaEdge(Object element, Edge edge) {
        bindKey(element, edge);
        openOnce();
        return invokeUnchecked(element);
    }

    /** Confined raw cast for KeySelector invocation; failures identify the offending edge. */
    @SuppressWarnings("unchecked")
    private void bindKey(Object element, Edge edge) {
        if (edge != null && edge.keyed()) {
            try {
                this.currentKey = ((KeySelector<Object, Object>) edge.keySelector()).getKey(element);
                stateStore.setCurrentKey(currentKey);
            } catch (Exception e) {
                throw new RuntimeException("keySelector failed on edge " + edge.src() + "\u2192" + edge.dst(), e);
            }
        }
    }

    protected abstract FunctionResult<?> invokeUnchecked(Object element);

    @Override
    public abstract RichFunction unwrap();

    @Override
    public MetricGroup unwrapMetricGroup() {
        return runtimeContext.getMetricGroup();
    }

    @Override
    public abstract boolean requiresKeyedEdge();

    /**
     * Wires global job parameters into the runtime context. Called once by
     * {@code WorkflowBuilder} at build time. Public (not API) — consumers
     * constructing harnesses directly get the default empty map.
     */
    public void setGlobalJobParameters(Map<String, String> params) {
        runtimeContext.setGlobalJobParameters(params);
    }
}