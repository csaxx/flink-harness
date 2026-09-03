package org.flink.harness.harness;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.OutputTag;
import org.flink.harness.Edge;
import org.flink.harness.FunctionResult;
import org.flink.harness.internal.InMemoryKeyedStateStore;
import org.flink.harness.internal.StandaloneOperatorMetricGroup;
import org.flink.harness.internal.StandaloneRuntimeContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Base of all function harnesses. Handles open/close lifecyle, wire runtime context,
 * key-binding and optional locking.
 */
public abstract class FunctionHarness {

    /** Singleton {@link OpenContext} (empty interface in Flink). */
    private static final OpenContext OPEN_CONTEXT = new OpenContext() {};

    private final String id;
    private final boolean useLock;
    private final StandaloneRuntimeContext runtimeContext;
    private final InMemoryKeyedStateStore stateStore;
    private final ReentrantLock lock = new ReentrantLock();
    private boolean opened;
    private Object currentKey;

    protected FunctionHarness(String id, boolean threadSafe) {
        this.id = id;
        this.useLock = threadSafe;
        this.runtimeContext = new StandaloneRuntimeContext(id);
        this.stateStore = runtimeContext.stateStore();
    }

    public final String getId() {
        return id;
    }

    /** Wire rich function: set context and call open once. */
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

    public void close() {
        if (opened) {
            try {
                unwrap().close();
            } catch (Exception e) {
                throw new RuntimeException("close() failed for " + id, e);
            }
        }
    }

    /** Reset state for all keys + unbind the current key slot. */
    public void clearState() {
        stateStore.clearAll();
        currentKey = null;
    }

    /** Reset metrics counters (primary gauges/meters untouched). */
    public void clearMetrics() {
        ((StandaloneOperatorMetricGroup) runtimeContext.getMetricGroup())
                .resetCounters();
    }

    /** Convenience: clear state + metrics together. */
    public void resetAll() {
        clearState();
        clearMetrics();
    }

    public Map<String, Object> metricsSnapshot() {
        return ((StandaloneOperatorMetricGroup) runtimeContext.getMetricGroup()).snapshot();
    }

    /** Current key (bound when invoked through a keyed edge). */
    protected final Object currentKey() {
        return currentKey;
    }

    /** Invoke the wrapped function under the optional lock. Key is bound BEFORE open(),
     * so keyed functions can initialize state handles from the bound key. */
    public final FunctionResult<?> processViaEdge(Object element, Edge edge) {
        if (useLock) {
            lock.lock();
        }
        try {
            bindKey(element, edge);
            openOnce();
            return invokeUnchecked(element);
        } finally {
            if (useLock) {
                lock.unlock();
            }
        }
    }

    // raw helper #2: KeySelector invocation — confined boundary
    @SuppressWarnings("unchecked")
    private void bindKey(Object element, Edge edge) {
        if (edge != null && edge.keyed()) {
            try {
                this.currentKey = ((KeySelector<Object, Object>) edge.keySelector()).getKey(element);
                stateStore.setCurrentKey(currentKey);
            } catch (Exception e) {
                throw new RuntimeException("keySelector failed on edge " + edge.src() + "→" + edge.dst(), e);
            }
        }
    }

    /** Invoke the wrapped function with raw cast — every subclass chose the exact method. */
    protected abstract FunctionResult<?> invokeUnchecked(Object element);

    /** Raw access to underlying function for lifecycle transitions. */
    public abstract RichFunction unwrap();

    /** true for {@code KeyedProcessFunctionHarness} — must receive keyed edges only. */
    public abstract boolean requiresKeyedEdge();

    /** Resolved input type, {@link WorkflowNode#UNKNOWN_TYPE} when unresolved. */
    public abstract String inputTypeName();

    /** Resolved output type name. */
    public abstract String outputTypeName();
}