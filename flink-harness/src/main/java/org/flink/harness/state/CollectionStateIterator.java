package org.flink.harness.state;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Eagerly-completed {@link StateIterator} backed by an in-memory collection snapshot.
 * Both {@link #onNext} overloads iterate immediately on the caller thread.
 */
final class CollectionStateIterator<T> implements StateIterator<T> {

    private final List<T> items;

    CollectionStateIterator(Iterable<T> source) {
        this.items = new ArrayList<>();
        source.forEach(items::add);
    }

    @Override
    public <U> StateFuture<Collection<U>> onNext(
            FunctionWithException<T, StateFuture<? extends U>, Exception> iterating) {
        try {
            List<U> results = new ArrayList<>(items.size());
            for (T item : items) {
                StateFuture<? extends U> future = iterating.apply(item);
                results.add(CompletedStateFuture.resolve(future));
            }
            return CompletedStateFuture.of(Collections.unmodifiableList(results));
        } catch (Exception e) {
            throw new RuntimeException("StateIterator.onNext failed", e);
        }
    }

    @Override
    public StateFuture<Void> onNext(ThrowingConsumer<T, Exception> iterating) {
        try {
            for (T item : items) {
                iterating.accept(item);
            }
            return CompletedStateFuture.ofVoid();
        } catch (Exception e) {
            throw new RuntimeException("StateIterator.onNext failed", e);
        }
    }

    @Override
    public boolean isEmpty() {
        return items.isEmpty();
    }
}