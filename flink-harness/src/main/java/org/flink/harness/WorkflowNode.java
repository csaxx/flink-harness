package org.flink.harness;

import java.io.Serializable;
import java.util.List;

/**
 * One node in the workflow DAG. Used for visualization and introspection.
 */
public record WorkflowNode(
        String functionId,
        Kind kind,
        String inputType,
        String outputType,
        List<String> successors) implements Serializable {

    public enum Kind {
        SOURCE,
        FUNCTION,
        SINK
    }

    public static final String UNKNOWN_TYPE = "<unknown>";
}