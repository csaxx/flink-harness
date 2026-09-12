package org.flink.harness.graph.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class JsonSource<T> extends StandaloneSource<String, T> {

    private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

    private final Class<T> targetClass;
    private final ObjectMapper mapper;

    public JsonSource(Class<T> targetClass) {
        this(targetClass, DEFAULT_MAPPER);
    }

    public JsonSource(Class<T> targetClass, ObjectMapper mapper) {
        this.targetClass = targetClass;
        this.mapper = mapper;
    }

    @Override
    protected void process(String element, Collector<Object> out) throws Exception {
        try {
            T value = mapper.readValue(element, targetClass);
            out.collect(value);
        } catch (Exception e) {
            throw new RuntimeException(
                    "JsonSource: failed to parse JSON for " + targetClass.getSimpleName(), e);
        }
    }
}