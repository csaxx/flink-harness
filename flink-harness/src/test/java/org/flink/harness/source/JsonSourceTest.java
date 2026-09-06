package org.flink.harness.source;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonSourceTest {

    @Test
    void parsesValidJson() {
        var source = new JsonSource<>(Record.class);
        var result = source.processViaEdge("{\"name\":\"alice\",\"value\":42}", null);
        assertThat(result.outputs()).hasSize(1);
        Record r = (Record) result.outputs().get(0);
        assertThat(r.name).isEqualTo("alice");
        assertThat(r.value).isEqualTo(42);
    }

    @Test
    void wrapsParseException() {
        var source = new JsonSource<>(Record.class);
        assertThatThrownBy(() -> source.processViaEdge("not-json", null))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(RuntimeException.class);
    }

    @Test
    void usesCustomMapper() {
        var mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        var source = new JsonSource<>(Record.class, mapper);
        var result = source.processViaEdge(
                "{\"name\":\"bob\",\"value\":7,\"extra\":\"ignored\"}", null);
        Record r = (Record) result.outputs().get(0);
        assertThat(r.name).isEqualTo("bob");
        assertThat(r.value).isEqualTo(7);
    }

    public record Record(String name, int value) {}
}