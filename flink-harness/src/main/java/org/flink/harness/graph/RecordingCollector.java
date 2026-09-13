package org.flink.harness.graph;

import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * List-backed {@link Collector} owning the per-node output buffer. Flink function signatures
 * require a {@code Collector}, and {@code Collector} declares two abstract methods
 * ({@code collect}/{@code close}) so no lambda can stand in. Each harness reuses one instance
 * per node: {@link #clear()} before every invocation, {@link #recorded()} to snapshot the
 * result. The backing list is never exposed, so user functions cannot mutate it.
 */
public final class RecordingCollector<T> implements Collector<T> {

    private final List<T> records = new ArrayList<>();

    @Override
    public void collect(T record) {
        records.add(record);
    }

    @Override
    public void close() {
        // no resources
    }

    /** Clears the reused buffer; harnesses call this before every invocation. */
    public void clear() {
        records.clear();
    }

    /** Immutable snapshot of everything recorded since the last {@link #clear()}. */
    public List<T> recorded() {
        return List.copyOf(records);
    }
}
