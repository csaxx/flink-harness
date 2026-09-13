package org.flink.harness.graph.function.single;

import org.apache.flink.api.common.functions.Function;
import org.flink.harness.graph.RecordingCollector;
import org.flink.harness.graph.function.AbstractFunctionHarness;
import org.flink.harness.graph.result.FunctionResult;

import java.util.Map;

/**
 * Base for harnesses wrapping a non-rich single-stream Flink function
 * ({@code MapFunction}/{@code FlatMapFunction}/{@code FilterFunction}). Mirrors upstream's
 * {@code AbstractUdfStreamOperator}: the three concretes share the per-invocation output
 * buffer and result assembly, differing only in how the element is invoked. There is no
 * lifecycle and no runtime context — the wrapped interfaces provide neither, so
 * {@code open()}/{@code close()} stay {@code StreamNode} no-ops and keyed state is
 * unreachable (Flink-faithful: state requires a {@code RuntimeContext}).
 *
 * @param <F> the wrapped Flink function type
 */
public abstract class AbstractSingleStreamFunctionHarness<F extends Function>
        extends AbstractFunctionHarness<F> {

    private final String operationName;
    private final RecordingCollector<Object> collector = new RecordingCollector<>();

    protected AbstractSingleStreamFunctionHarness(String id, F function, String operationName) {
        super(id, function);
        this.operationName = operationName;
    }

    protected final RecordingCollector<Object> collector() {
        return collector;
    }

    /** Reuses the collector's per-node buffer, so it must be cleared before every invocation. */
    @Override
    protected FunctionResult<?> processElement(Object element) {
        collector.clear();
        try {
            invoke(element);
        } catch (Exception exception) {
            throw new RuntimeException(operationName + " failed in " + getId(), exception);
        }
        return new FunctionResult<>(collector.recorded(), Map.of(), Map.of());
    }

    protected abstract void invoke(Object element) throws Exception;
}
