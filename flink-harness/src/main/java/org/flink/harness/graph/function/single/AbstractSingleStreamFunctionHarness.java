package org.flink.harness.graph.function.single;

import org.apache.flink.api.common.functions.Function;
import org.flink.harness.graph.RecordingCollector;
import org.flink.harness.graph.function.AbstractFunctionHarness;

/**
 * Base for harnesses wrapping a non-rich single-stream Flink function
 * ({@code MapFunction}/{@code FlatMapFunction}/{@code FilterFunction}). Each concrete owns
 * its full per-element body (collector clear → wrap invocation → assemble
 * {@code FunctionResult}) and differs only in how the element is invoked; this class
 * supplies the shared per-node output buffer. There is no lifecycle and no runtime
 * context — the wrapped interfaces provide neither.
 *
 * @param <F> the wrapped Flink function type
 */
public abstract class AbstractSingleStreamFunctionHarness<F extends Function>
        extends AbstractFunctionHarness<F> {

    private final RecordingCollector<Object> collector = new RecordingCollector<>();

    protected AbstractSingleStreamFunctionHarness(String id, F function) {
        super(id, function);
    }

    protected final RecordingCollector<Object> collector() {
        return collector;
    }
}
