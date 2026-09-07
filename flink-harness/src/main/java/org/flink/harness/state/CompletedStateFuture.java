package org.flink.harness.state;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

/**
 * Eagerly-completed {@link StateFuture}: the result is computed synchronously at creation time
 * and all continuations ({@code thenApply}, {@code thenCompose}, etc.) execute immediately
 * on the caller thread. This is a faithful emulation of Flink's async state API for a
 * single-threaded synchronous harness — callbacks run deterministically before the user's
 * next statement, and exceptions from continuations propagate to the caller.
 *
 * <p>Futures returned by {@code thenCompose}/{@code thenCombine} and their conditional
 * variants are assumed to be {@link CompletedStateFuture} instances created by flink-harness.
 * A foreign {@link StateFuture} implementation causes an {@link IllegalStateException}
 * at the chaining call site.
 */
final class CompletedStateFuture<T> implements StateFuture<T> {

    private final T value;

    private CompletedStateFuture(T value) {
        this.value = value;
    }

    /**
     * Package-private accessor for the eagerly-computed value. Exists so tests
     * in the same package can verify results without going through callbacks.
     */
    T value() {
        return value;
    }

    /**
     * Wrap an already-computed value into an eagerly-completed future. */
    static <T> CompletedStateFuture<T> of(T value) {
        return new CompletedStateFuture<>(value);
    }

    /** A completed {@code StateFuture<Void>}. */
    static CompletedStateFuture<Void> ofVoid() {
        return new CompletedStateFuture<>(null);
    }

    /**
     * Resolve the value from a {@link StateFuture}. Only works for futures created by
     * flink-harness ({@link CompletedStateFuture}).
     */
    @SuppressWarnings("unchecked")
    static <T> T resolve(StateFuture<T> f) {
        if (f instanceof CompletedStateFuture) {
            return ((CompletedStateFuture<T>) f).value;
        }
        throw new IllegalStateException(
                "flink-harness supports only eagerly-completed StateFutures "
                        + "created by its own v2 state handles");
    }

    @Override
    public <U> StateFuture<U> thenApply(
            FunctionWithException<? super T, ? extends U, ? extends Exception> fn) {
        try {
            return of(fn.apply(value));
        } catch (Exception e) {
            throw new RuntimeException("StateFuture.thenApply failed", e);
        }
    }

    @Override
    public StateFuture<Void> thenAccept(
            ThrowingConsumer<? super T, ? extends Exception> action) {
        try {
            action.accept(value);
            return ofVoid();
        } catch (Exception e) {
            throw new RuntimeException("StateFuture.thenAccept failed", e);
        }
    }

    @Override
    public <U> StateFuture<U> thenCompose(
            FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                    action) {
        try {
            return of(resolve(action.apply(value)));
        } catch (Exception e) {
            throw new RuntimeException("StateFuture.thenCompose failed", e);
        }
    }

    @Override
    public <U, V> StateFuture<V> thenCombine(
            StateFuture<? extends U> other,
            BiFunctionWithException<? super T, ? super U, ? extends V, ? extends Exception> fn) {
        try {
            return of(fn.apply(value, resolve(other)));
        } catch (Exception e) {
            throw new RuntimeException("StateFuture.thenCombine failed", e);
        }
    }

    // -- thenConditionallyApply (two-arg) --

    @Override
    public <U, V> StateFuture<Tuple2<Boolean, Object>> thenConditionallyApply(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends U, ? extends Exception> actionIfTrue,
            FunctionWithException<? super T, ? extends V, ? extends Exception> actionIfFalse) {
        try {
            boolean cond = condition.apply(value);
            if (cond) {
                return of(Tuple2.of(true, actionIfTrue.apply(value)));
            } else {
                return of(Tuple2.of(false, actionIfFalse.apply(value)));
            }
        } catch (Exception e) {
            throw new RuntimeException("StateFuture.thenConditionallyApply failed", e);
        }
    }

    // -- thenConditionallyApply (one-arg) --

    @Override
    @SuppressWarnings("unchecked")
    public <U> StateFuture<Tuple2<Boolean, U>> thenConditionallyApply(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends U, ? extends Exception> actionIfTrue) {
        try {
            boolean cond = condition.apply(value);
            if (cond) {
                return of(Tuple2.of(true, actionIfTrue.apply(value)));
            } else {
                return of(Tuple2.of(false, (U) null));
            }
        } catch (Exception e) {
            throw new RuntimeException("StateFuture.thenConditionallyApply failed", e);
        }
    }

    // -- thenConditionallyAccept (two-arg) --

    @Override
    public StateFuture<Boolean> thenConditionallyAccept(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            ThrowingConsumer<? super T, ? extends Exception> actionIfTrue,
            ThrowingConsumer<? super T, ? extends Exception> actionIfFalse) {
        try {
            boolean cond = condition.apply(value);
            if (cond) {
                actionIfTrue.accept(value);
            } else {
                actionIfFalse.accept(value);
            }
            return of(cond);
        } catch (Exception e) {
            throw new RuntimeException("StateFuture.thenConditionallyAccept failed", e);
        }
    }

    // -- thenConditionallyAccept (one-arg) --

    @Override
    public StateFuture<Boolean> thenConditionallyAccept(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            ThrowingConsumer<? super T, ? extends Exception> actionIfTrue) {
        try {
            boolean cond = condition.apply(value);
            if (cond) {
                actionIfTrue.accept(value);
            }
            return of(cond);
        } catch (Exception e) {
            throw new RuntimeException("StateFuture.thenConditionallyAccept failed", e);
        }
    }

    // -- thenConditionallyCompose (two-arg) --

    @Override
    public <U, V> StateFuture<Tuple2<Boolean, Object>> thenConditionallyCompose(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                    actionIfTrue,
            FunctionWithException<? super T, ? extends StateFuture<V>, ? extends Exception>
                    actionIfFalse) {
        try {
            boolean cond = condition.apply(value);
            if (cond) {
                return of(Tuple2.of(true, resolve(actionIfTrue.apply(value))));
            } else {
                return of(Tuple2.of(false, resolve(actionIfFalse.apply(value))));
            }
        } catch (Exception e) {
            throw new RuntimeException("StateFuture.thenConditionallyCompose failed", e);
        }
    }

    // -- thenConditionallyCompose (one-arg) --

    @Override
    @SuppressWarnings("unchecked")
    public <U> StateFuture<Tuple2<Boolean, U>> thenConditionallyCompose(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                    actionIfTrue) {
        try {
            boolean cond = condition.apply(value);
            if (cond) {
                return of(Tuple2.of(true, resolve(actionIfTrue.apply(value))));
            } else {
                return of(Tuple2.of(false, (U) null));
            }
        } catch (Exception e) {
            throw new RuntimeException("StateFuture.thenConditionallyCompose failed", e);
        }
    }
}