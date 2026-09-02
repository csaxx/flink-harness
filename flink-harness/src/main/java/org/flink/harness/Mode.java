package org.flink.harness;

/** Workflow execution mode. See AGENTS.md. */
public enum Mode {
    /** Metrics and state accumulate across invocations, like real Flink semantics. */
    CONTINUOUS,
    /** Everything (state, metrics, stateful counters) is cleared after each process() call — even on exception. */
    TRANSIENT
}