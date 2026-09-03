package org.flink.harness.internal;

import org.apache.flink.util.Collector;

import java.util.List;

/** Collector that records each element into a backing list. */
public final class RecordingCollector<T> implements Collector<T> {

    private final List<T> sink;

    public RecordingCollector(List<T> sink) {
        this.sink = sink;
    }

    @Override
    public void collect(T record) {
        sink.add(record);
    }

    @Override
    public void close() {
        // no resources
    }
}