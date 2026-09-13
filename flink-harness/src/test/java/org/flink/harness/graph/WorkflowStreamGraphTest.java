package org.flink.harness.graph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.flink.harness.graph.function.rich.KeyedProcessFunctionHarness;
import org.flink.harness.graph.function.rich.ProcessFunctionHarness;
import org.flink.harness.graph.sink.StandaloneSink;
import org.flink.harness.graph.source.StandaloneSource;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for the graph container itself: immutability, ordering, derived views and
 * construction-time validation — no workflow execution involved. */
class WorkflowStreamGraphTest {

    private static final TypeInformation<String> STRING = TypeInformation.of(String.class);

    private final ProcessFunction<Object, Object> passThroughFn = new Passive();
    private final KeyedProcessFunction<Object, Object, Object> keyedFn = new PassiveKeyed();
    private final StandaloneSource<Object, Object> source = new StandaloneSource<>();
    private final StandaloneSink<Object> sink = new StandaloneSink<>();

    /** in → a → k → out, plus fan-out a → out; all main-channel types String. */
    private WorkflowStreamGraph newGraph() {
        Map<String, StreamNode> nodes = new LinkedHashMap<>();
        nodes.put("a", new ProcessFunctionHarness("a", passThroughFn));
        nodes.put("k", new KeyedProcessFunctionHarness("k", keyedFn));
        nodes.put("in", source);
        nodes.put("out", sink);
        List<StreamEdge> edges = List.of(
                new StreamEdge("in", "a", null, null),
                new StreamEdge("a", "k", (KeySelector<Object, Object>) key -> key, null),
                new StreamEdge("a", "out", null, null),
                new StreamEdge("k", "out", null, null));
        Map<String, TypeInformation<?>> inputTypes = new LinkedHashMap<>();
        Map<String, TypeInformation<?>> outputTypes = new LinkedHashMap<>();
        for (String id : List.of("in", "a", "k", "out")) {
            inputTypes.put(id, STRING);
        }
        for (String id : List.of("in", "a", "k")) {
            outputTypes.put(id, STRING);
        }
        return WorkflowStreamGraph.create(
                nodes, edges, Set.of("in"), Set.of("out"), inputTypes, outputTypes, false);
    }

    @Test
    void viewsAreImmutableAndRegistrationOrdered() {
        WorkflowStreamGraph graph = newGraph();

        assertThat(graph.nodes().keySet()).containsExactly("a", "k", "in", "out");
        assertThatThrownBy(() -> graph.nodes().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> graph.edges().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> graph.inputTypes().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> graph.outputTypes().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> graph.workflowNodes().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> graph.sourceIds().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> graph.outboundEdgesOf("a").clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void outboundEdgesGroupBySourceInEdgeRegistrationOrder() {
        WorkflowStreamGraph graph = newGraph();

        assertThat(graph.outboundEdgesOf("a"))
                .extracting(StreamEdge::dst)
                .containsExactly("k", "out");
        assertThat(graph.outboundEdgesOf("in")).extracting(StreamEdge::dst).containsExactly("a");
        assertThat(graph.outboundEdgesOf("out")).isEmpty();
    }

    @Test
    void keyedHarnessesContainsOnlyKeyedNodes() {
        WorkflowStreamGraph graph = newGraph();

        assertThat(graph.keyedHarnesses())
                .extracting(KeyedProcessFunctionHarness::getId)
                .containsExactly("k");
    }

    @Test
    void nodeAndUnwrapped() {
        WorkflowStreamGraph graph = newGraph();

        assertThatThrownBy(() -> graph.node("missing"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unknown node id");
        // harness nodes unwrap to the wrapped Flink function; synthetic nodes unwrap to themselves
        assertThat(graph.unwrapped("a")).isSameAs(passThroughFn);
        assertThat(graph.unwrapped("k")).isSameAs(keyedFn);
        assertThat(graph.unwrapped("in")).isSameAs(source);
        assertThat(graph.unwrapped("out")).isSameAs(sink);
    }

    @Test
    void typeHintsAreExposedPerNode() {
        WorkflowStreamGraph graph = newGraph();

        assertThat(graph.inputTypes().get("a")).isEqualTo(STRING);
        assertThat(graph.outputTypes().get("in")).isEqualTo(STRING);
        // the sink has no output-type hint
        assertThat(graph.outputTypes()).doesNotContainKey("out");
    }

    @Test
    void workflowNodesProjectionDerivesKindsSuccessorsAndTypeNames() {
        WorkflowStreamGraph graph = newGraph();

        List<WorkflowNode> projection = graph.workflowNodes();
        assertThat(projection).extracting(WorkflowNode::functionId)
                .containsExactly("a", "k", "in", "out");

        WorkflowNode a = projection.stream().filter(node -> node.functionId().equals("a")).findFirst().orElseThrow();
        assertThat(a.kind()).isEqualTo(WorkflowNode.Kind.FUNCTION);
        assertThat(a.successors()).containsExactly("k", "out");
        assertThat(a.inputType()).isEqualTo(STRING.toString());

        WorkflowNode in = projection.stream().filter(node -> node.functionId().equals("in")).findFirst().orElseThrow();
        assertThat(in.kind()).isEqualTo(WorkflowNode.Kind.SOURCE);

        WorkflowNode out = projection.stream().filter(node -> node.functionId().equals("out")).findFirst().orElseThrow();
        assertThat(out.kind()).isEqualTo(WorkflowNode.Kind.SINK);
        assertThat(out.outputType()).isEqualTo(WorkflowNode.UNKNOWN_TYPE);
    }

    @Test
    void validationFailsAtConstruction() {
        Map<String, StreamNode> nodes = new LinkedHashMap<>();
        nodes.put("a", new ProcessFunctionHarness("a", passThroughFn));
        nodes.put("k", new KeyedProcessFunctionHarness("k", keyedFn));

        // unknown endpoint
        assertThatThrownBy(() -> WorkflowStreamGraph.create(
                nodes, List.of(new StreamEdge("a", "nope", null, null)),
                Set.of(), Set.of(), Map.of(), Map.of(), true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unknown node id");

        // keyed function receiving an unkeyed edge
        assertThatThrownBy(() -> WorkflowStreamGraph.create(
                nodes, List.of(new StreamEdge("a", "k", null, null)),
                Set.of(), Set.of(), Map.of(), Map.of(), true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unkeyed edge");

        // source receiving an inbound edge
        assertThatThrownBy(() -> WorkflowStreamGraph.create(
                nodes, List.of(new StreamEdge("k", "a", null, null)),
                Set.of("a"), Set.of(), Map.of(), Map.of(), true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must not receive inbound edges");
    }

    private static final class Passive extends ProcessFunction<Object, Object> {
        @Override
        public void processElement(Object value, Context ctx, Collector<Object> out) {
            out.collect(value);
        }
    }

    private static final class PassiveKeyed extends KeyedProcessFunction<Object, Object, Object> {
        @Override
        public void processElement(Object value, Context ctx, Collector<Object> out) {
            out.collect(value);
        }
    }
}
