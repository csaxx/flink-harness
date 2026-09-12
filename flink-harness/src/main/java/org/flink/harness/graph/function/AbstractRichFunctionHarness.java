package org.flink.harness.graph.function;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.flink.harness.Edge;
import org.flink.harness.graph.result.FunctionResult;
import org.flink.harness.state.InMemoryKeyedStateStore;

import java.util.Map;

/**
 * Base for harnesses wrapping an {@link AbstractRichFunction}. Adds key binding and
 * keyed-state scoping on top of {@link FunctionHarness}: on a keyed edge the element's
 * key is bound before invocation, mirroring Flink where any rich function on a keyed
 * stream may use keyed state (only non-keyed streams throw).
 *
 * @param <F> the wrapped Flink function type
 */
public abstract class AbstractRichFunctionHarness<F extends AbstractRichFunction>
        extends FunctionHarness<F> {

    private final InMemoryKeyedStateStore stateStore;
    private Object currentKey;

    protected AbstractRichFunctionHarness(
            String id, F function, Map<String, String> globalJobParameters) {
        super(id, function, globalJobParameters);
        this.stateStore = runtimeContext().stateStore();
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

    /** Order matters: bind the key first so a lazy {@link #open()} observes the element's
     * key when registering state handles. */
    @Override
    public FunctionResult<?> processElement(Object element, Edge edge) {
        bindKey(element, edge);
        return super.processElement(element, edge);
    }

    /** Confined raw cast for KeySelector invocation; failures identify the offending edge. */
    @SuppressWarnings("unchecked")
    private void bindKey(Object element, Edge edge) {
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
