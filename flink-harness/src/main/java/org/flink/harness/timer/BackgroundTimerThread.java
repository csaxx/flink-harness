package org.flink.harness.timer;

import org.flink.harness.result.WorkflowResult;

import java.util.concurrent.atomic.AtomicReference;

public final class BackgroundTimerThread implements AutoCloseable {

    private static final long POLL_INTERVAL_MS = 100;

    public enum State { RUNNING, FAILED, SHUTDOWN }

    private final Thread thread;
    private final BackgroundTimerAction action;
    private final BackgroundTimerListener listener;
    private final Object wakeLock = new Object();
    private volatile boolean running = true;
    private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);
    private volatile Throwable failure;

    @FunctionalInterface
    public interface BackgroundTimerAction {
        WorkflowResult fireDue();
    }

    public BackgroundTimerThread(BackgroundTimerAction action, BackgroundTimerListener listener) {
        this.action = action;
        this.listener = listener;
        this.thread = new Thread(this::runLoop, "flink-harness-bg-timer");
        this.thread.setDaemon(true);
    }

    public void start() {
        thread.start();
    }

    public State state() {
        return state.get();
    }

    public Throwable failure() {
        return failure;
    }

    public void notifyWake() {
        synchronized (wakeLock) {
            wakeLock.notifyAll();
        }
    }

    @Override
    public void close() {
        running = false;
        notifyWake();
        try {
            thread.join(2000);
        } catch (InterruptedException ignored) {
            thread.interrupt();
        }
    }

    private void runLoop() {
        while (running) {
            try {
                WorkflowResult result = action.fireDue();
                if (result != null) {
                    listener.onResult(result);
                }

                long waitMs = POLL_INTERVAL_MS;
                synchronized (wakeLock) {
                    if (running) {
                        wakeLock.wait(waitMs);
                    }
                }
            } catch (InterruptedException e) {
                if (!running) break;
            } catch (Exception e) {
                failure = e;
                state.set(State.FAILED);
                listener.onError("", e);
                break;
            } catch (Throwable t) {
                failure = t;
                state.set(State.FAILED);
                listener.onError("", t);
                break;
            }
        }
        state.compareAndSet(State.RUNNING, State.SHUTDOWN);
        state.compareAndSet(State.FAILED, State.FAILED);
    }
}