package org.flink.harness.graph.function;

import org.apache.flink.api.common.functions.Function;
import org.flink.harness.graph.DataStreamEdge;
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
     * edge key and open lazily before delegating to the same abstract invocation. */
    @Override
    public FunctionResult<?> processElement(Object element, DataStreamEdge edge) {
        return processElement(element);
    }

    protected abstract FunctionResult<?> processElement(Object element);
}
