package org.flink.harness.graph.function;

import org.apache.flink.api.common.functions.Function;
import org.flink.harness.graph.StreamEdge;
import org.flink.harness.graph.StreamNode;
import org.flink.harness.graph.function.rich.AbstractRichFunctionHarness;
import org.flink.harness.graph.function.single.AbstractSingleStreamFunctionHarness;
import org.flink.harness.graph.result.FunctionResult;

/**
 * Base of all Flink-function harnesses: wraps a {@link Function} and owns the node's
 * identity. Mirrors Flink's plain {@code Function} interface, which has no
 * {@code RuntimeContext} and therefore no metrics, lifecycle or keyed state.
 * Lifecycle, runtime-context wiring, key management and metrics only exist for rich
 * functions and live one level down in {@link AbstractRichFunctionHarness};
 * non-rich single-stream functions are served by {@link AbstractSingleStreamFunctionHarness}.
 * Implements {@link StreamNode} so it participates in the workflow graph.
 *
 * @param <F> the wrapped Flink function type
 */
public abstract class AbstractFunctionHarness<F extends Function> implements StreamNode {

    private final String id;
    private final F function;

    protected AbstractFunctionHarness(String id, F function) {
        this.id = id;
        this.function = function;
    }

    public final String getId() {
        return id;
    }

    public final F getFunction() {
        return function;
    }

    /** Per-element entry point: no lifecycle here — rich subtypes override to bind the
     * edge key and open lazily before delegating to the same abstract {@link #invoke}. */
    @Override
    public FunctionResult<?> processElement(Object element, StreamEdge edge) {
        return invoke(element);
    }

    /** The single subtype-specific per-element body, already surrounded by whatever
     * lifecycle the branch needs (rich) or not (non-rich). */
    protected abstract FunctionResult<?> invoke(Object element);
}
