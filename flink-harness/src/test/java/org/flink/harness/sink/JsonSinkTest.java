package org.flink.harness.sink;

import org.flink.harness.graph.sink.JsonSink;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JsonSinkTest {

    private record Sample(String label, int count) {}

    private record Cyclic(JsonSinkTest parent) {}

    @Test
    void serializesToCompactJson() {
        var sink = new JsonSink<>(false);
        var result = sink.processViaEdge(new Sample("x", 3), null);
        assertThat(result.outputs()).hasSize(1);
        assertThat(result.outputs().get(0)).isEqualTo("{\"label\":\"x\",\"count\":3}");
    }

    @Test
    void serializesWithPrettyPrint() {
        var sink = new JsonSink<>(true);
        var result = sink.processViaEdge(new Sample("y", 5), null);
        assertThat(result.outputs()).hasSize(1);
        String json = (String) result.outputs().get(0);
        assertThat(json).contains("\n");
        assertThat(json).contains("\"label\"");
    }

    @Test
    void wrapsSerializationException() {
        var sink = new JsonSink<>(false);
        assertThat(org.junit.jupiter.api.Assertions.assertThrows(
                RuntimeException.class,
                () -> sink.processViaEdge(new Cyclic(this), null)))
                .isInstanceOf(RuntimeException.class);
    }
}