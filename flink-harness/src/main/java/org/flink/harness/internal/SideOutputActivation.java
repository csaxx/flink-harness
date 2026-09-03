package org.flink.harness.internal;

import org.apache.flink.util.OutputTag;

/** Internal: a terminal side output to aggregate (function id + tag). Public only for cross-package access by workflow classes. */
public record SideOutputActivation(String functionId, OutputTag<?> tag) {}