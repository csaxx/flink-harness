package org.flink.harness.timer;

/** How often processing-time timers are fired. Exclusive and chosen at build time; TRANSIENT only allows OPPORTUNISTIC. */
public enum ProcessingTimerMode {
    /** Fire due timers after every element invocation and when the queue drains. */
    OPPORTUNISTIC,
    /** Never fire automatically; only getTimerService().fireProcessingTimers() fires. */
    MANUAL,
    /** A daemon poller (~100ms) fires due timers under the workflow lock and reports via a listener. */
    BACKGROUND
}