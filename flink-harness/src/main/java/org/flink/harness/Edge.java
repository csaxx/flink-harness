package org.flink.harness;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.OutputTag;

/**
 * Edge descriptor — source, destination, optional key selector, optional side-output tag.
 * {@code sideTag == null} means main output channel.
 */
public record Edge(String src, String dst, KeySelector<?, ?> keySelector, OutputTag<?> sideTag) {

    public Edge(String src, String dst, KeySelector<?, ?> keySelector) {
        this(src, dst, keySelector, null);
    }

    public boolean keyed() {
        return keySelector != null;
    }

    public boolean sideChannel() {
        return sideTag != null;
    }
}