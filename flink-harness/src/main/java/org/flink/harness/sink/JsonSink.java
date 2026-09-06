package org.flink.harness.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.util.Collector;

public class JsonSink<IN> extends StandaloneSink<IN> {

    private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

    private final boolean prettyPrint;
    private final ObjectMapper mapper;
    private final ObjectWriter jsonWriter;

    public JsonSink(boolean prettyPrint) {
        this(prettyPrint, DEFAULT_MAPPER);
    }

    public JsonSink(boolean prettyPrint, ObjectMapper mapper) {
        this.prettyPrint = prettyPrint;
        this.mapper = mapper;
        this.jsonWriter = prettyPrint ? mapper.writerWithDefaultPrettyPrinter() : null;
    }

    public boolean isPrettyPrint() {
        return prettyPrint;
    }

    @Override
    protected void accept(IN element, Collector<Object> collected) throws Exception {
        try {
            String json;
            if (prettyPrint) {
                json = jsonWriter.writeValueAsString(element);
            } else {
                json = mapper.writeValueAsString(element);
            }
            collected.collect(json);
        } catch (Exception e) {
            throw new RuntimeException(
                    "JsonSink: failed to serialize " + element + " to JSON", e);
        }
    }
}