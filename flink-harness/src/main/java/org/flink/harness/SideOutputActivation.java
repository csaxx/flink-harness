package org.flink.harness;

import org.apache.flink.util.OutputTag;

/** Internal: a terminal side output to aggregate (function id + tag). */
record SideOutputActivation(String functionId, OutputTag<?> tag) {}