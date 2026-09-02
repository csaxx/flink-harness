package org.flink.harness;

import java.io.Serializable;
import java.util.List;

/**
 * One node in the workflow DAG. Used for visualization and introspection.
 */
public record WorkflowNode(
        String functionId,
        String inputType,
        String outputType,
        List<String> successors) implements Serializable {

    public static final String UNKNOWN_TYPE = "<unknown>";
}