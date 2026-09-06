package org.flink.harness.timer;

import org.flink.harness.result.WorkflowResult;

public interface BackgroundTimerListener {
    void onResult(WorkflowResult result);

    void onError(String nodeId, Throwable error);
}