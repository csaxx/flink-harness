package org.flink.harness;

import org.apache.flink.api.java.functions.KeySelector;

/** Edge descriptor — source, destination, optional key selector. */
public record Edge(String src, String dst, KeySelector<?, ?> keySelector) {

    public boolean keyed() {
        return keySelector != null;
    }
}