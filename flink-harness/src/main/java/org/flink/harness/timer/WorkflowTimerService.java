package org.flink.harness.timer;

import org.flink.harness.graph.result.WorkflowResult;

/**
 * Public handle for workflow-level timer control. Delegates to the owning StandaloneWorkflow's
 * internal firing/counting, so all firing paths share the same lock and queue.
 */
public final class WorkflowTimerService {

    private final WorkflowAction fire;
    private final PendingTimerCount count;

    @FunctionalInterface
    public interface WorkflowAction {
        WorkflowResult fire();
    }

    @FunctionalInterface
    public interface PendingTimerCount {
        long count();
    }

    public WorkflowTimerService(WorkflowAction fire, PendingTimerCount count) {
        this.fire = fire;
        this.count = count;
    }

    public WorkflowResult fireProcessingTimers() {
        return fire.fire();
    }

    public long pendingTimerCount() {
        return count.count();
    }
}