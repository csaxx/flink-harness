package org.flink.harness;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.OutputTag;
import org.flink.harness.harness.FunctionHarness;
import org.flink.harness.harness.HarnessFactory;
import org.flink.harness.harness.NodeHarness;
import org.flink.harness.source.StandaloneSource;
import org.flink.harness.sink.StandaloneSink;
import org.flink.harness.WorkflowNode.Kind;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fluent builder for {@link StandaloneWorkflow}. Sources are the only entrypoints;
 * sinks are the only collection points. Side outputs route via channel-qualified edges.
 */
public final class WorkflowBuilder {

    private final Mode mode;
    private final Map<String, Object> functions = new LinkedHashMap<>();
    private final Map<String, StandaloneSource<?, ?>> sources = new LinkedHashMap<>();
    private final Map<String, StandaloneSink<?>> sinks = new LinkedHashMap<>();
    private final Map<String, TypeInformation<?>> inputTypes = new LinkedHashMap<>();
    private final Map<String, TypeInformation<?>> outputTypes = new LinkedHashMap<>();
    private final List<Edge> edges = new ArrayList<>();
    private boolean eagerInit;

    // node kind tracking for validation
    private final Map<String, Kind> nodeKinds = new LinkedHashMap<>();

    public WorkflowBuilder(Mode mode) {
        this.mode = mode;
    }

    public WorkflowBuilder initializeAtBuild() {
        this.eagerInit = true;
        return this;
    }

    // --------------------------------------------------------------------------------------------
    // function registration
    // --------------------------------------------------------------------------------------------

    public WorkflowBuilder registerFunction(String id, Function function) {
        registerAny(id, function, Kind.FUNCTION);
        return this;
    }

    public WorkflowBuilder registerFunction(String id, Function function,
            TypeInformation<?> inputType, TypeInformation<?> outputType) {
        registerAny(id, function, Kind.FUNCTION);
        inputTypes.put(id, inputType);
        outputTypes.put(id, outputType);
        return this;
    }

    public WorkflowBuilder registerKeyedFunction(String id,
            org.apache.flink.streaming.api.functions.KeyedProcessFunction<?, ?, ?> function) {
        registerAny(id, function, Kind.FUNCTION);
        return this;
    }

    public WorkflowBuilder registerKeyedFunction(String id,
            org.apache.flink.streaming.api.functions.KeyedProcessFunction<?, ?, ?> function,
            TypeInformation<?> inputType, TypeInformation<?> outputType) {
        registerAny(id, function, Kind.FUNCTION);
        inputTypes.put(id, keyedNotNull(inputType, "input"));
        outputTypes.put(id, keyedNotNull(outputType, "output"));
        return this;
    }

    // --------------------------------------------------------------------------------------------
    // source registration
    // --------------------------------------------------------------------------------------------

    /** Passthrough source (untyped). */
    public WorkflowBuilder addSource(String id) {
        sources.put(requireUnique(id, "source"), new StandaloneSource<>());
        nodeKinds.put(id, Kind.SOURCE);
        return this;
    }

    /** Passthrough source with single type (same in/out). */
    public WorkflowBuilder addSource(String id, TypeInformation<?> ioType) {
        addSource(id);
        inputTypes.put(id, ioType);
        outputTypes.put(id, ioType);
        return this;
    }

    /** Passthrough source with explicit input/output types. */
    public WorkflowBuilder addSource(String id, TypeInformation<?> inType, TypeInformation<?> outType) {
        addSource(id);
        inputTypes.put(id, inType);
        outputTypes.put(id, outType);
        return this;
    }

    /** Custom source. */
    public WorkflowBuilder addSource(String id, StandaloneSource<?, ?> source) {
        sources.put(requireUnique(id, "source"), source);
        nodeKinds.put(id, Kind.SOURCE);
        return this;
    }

    /** Custom source with type hints. */
    public WorkflowBuilder addSource(String id, StandaloneSource<?, ?> source,
            TypeInformation<?> inType, TypeInformation<?> outType) {
        addSource(id, source);
        inputTypes.put(id, inType);
        outputTypes.put(id, outType);
        return this;
    }

    // --------------------------------------------------------------------------------------------
    // sink registration
    // --------------------------------------------------------------------------------------------

    public WorkflowBuilder addSink(String id) {
        sinks.put(requireUnique(id, "sink"), new StandaloneSink<>());
        nodeKinds.put(id, Kind.SINK);
        return this;
    }

    public WorkflowBuilder addSink(String id, TypeInformation<?> inType) {
        addSink(id);
        inputTypes.put(id, inType);
        return this;
    }

    public WorkflowBuilder addSink(String id, StandaloneSink<?> sink) {
        sinks.put(requireUnique(id, "sink"), sink);
        nodeKinds.put(id, Kind.SINK);
        return this;
    }

    public WorkflowBuilder addSink(String id, StandaloneSink<?> sink, TypeInformation<?> inType) {
        addSink(id, sink);
        inputTypes.put(id, inType);
        return this;
    }

    // --------------------------------------------------------------------------------------------
    // edges
    // --------------------------------------------------------------------------------------------

    /** Untyped edge routing main outputs of {@code src} into {@code dst}. */
    public WorkflowBuilder addEdge(String src, String dst) {
        edges.add(new Edge(src, dst, null, null));
        return this;
    }

    /** Keyed edge: compute key per element — dst must be keyed-able (KeyedProcess, Rich*). */
    public <IN, K> WorkflowBuilder addKeyedEdge(String src, String dst, KeySelector<IN, K> keySelector) {
        if (keySelector == null) {
            throw new IllegalArgumentException("keySelector must not be null for keyed edge");
        }
        edges.add(new Edge(src, dst, keySelector, null));
        return this;
    }

    /** Edge routing a side output (tag) from src into dst (function or sink). */
    public WorkflowBuilder addSideOutputEdge(String src, String dst, OutputTag<?> tag) {
        if (tag == null) {
            throw new IllegalArgumentException("tag must not be null for side-output edge");
        }
        edges.add(new Edge(src, dst, null, tag));
        return this;
    }

    /** Keyed side-output edge. */
    public <IN, K> WorkflowBuilder addKeyedSideOutputEdge(String src, String dst,
            OutputTag<?> tag, KeySelector<IN, K> keySelector) {
        if (tag == null) {
            throw new IllegalArgumentException("tag must not be null for side-output edge");
        }
        if (keySelector == null) {
            throw new IllegalArgumentException("keySelector must not be null for keyed edge");
        }
        edges.add(new Edge(src, dst, keySelector, tag));
        return this;
    }

    /** Sugar: edge from a source to a function/sink. */
    public WorkflowBuilder addSourceEdge(String src, String dst) {
        return addEdge(src, dst);
    }

    /** Sugar: keyed edge from a source to a keyed function. */
    public <IN, K> WorkflowBuilder addSourceEdge(String src, String dst, KeySelector<IN, K> keySelector) {
        return addKeyedEdge(src, dst, keySelector);
    }

    /** Sugar: edge from a function/source to a sink (main channel). */
    public WorkflowBuilder addSinkEdge(String src, String dst) {
        return addEdge(src, dst);
    }

    /** Sugar: edge routing a side output into a sink. */
    public WorkflowBuilder addSinkEdge(String src, String dst, OutputTag<?> tag) {
        return addSideOutputEdge(src, dst, tag);
    }

    // --------------------------------------------------------------------------------------------
    // build
    // --------------------------------------------------------------------------------------------

    public StandaloneWorkflow build() {
        return build(false);
    }

    public StandaloneWorkflow build(boolean optOutTypeValidation) {
        // 1. build harnesses for Flink functions
        Map<String, NodeHarness> nodes = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : functions.entrySet()) {
            nodes.put(entry.getKey(), HarnessFactory.create(entry.getKey(), entry.getValue()));
        }

        // 2. add sources and sinks directly (they implement NodeHarness already)
        for (Map.Entry<String, StandaloneSource<?, ?>> entry : sources.entrySet()) {
            nodes.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, StandaloneSink<?>> entry : sinks.entrySet()) {
            nodes.put(entry.getKey(), entry.getValue());
        }

        // 3. topology validation
        validateTopology(nodes);

        // 4. edge type validation
        for (Edge edge : edges) {
            NodeHarness src = requireNode(nodes, edge.src());
            NodeHarness dst = requireNode(nodes, edge.dst());

            TypeInformation<?> srcOut = outputTypes.get(edge.src());
            TypeInformation<?> dstIn = inputTypes.get(edge.dst());

            if (edge.sideChannel()) {
                // side output: source type from tag
                TypeInformation<?> tagType = edge.sideTag().getTypeInfo();
                if (tagType != null) {
                    if (dstIn != null && !tagType.equals(dstIn)) {
                        throw new IllegalStateException(
                                "side-output edge type mismatch: " + edge.src() + "#" + edge.sideTag()
                                        + " outputs " + tagType + " but " + edge.dst() + " expects " + dstIn);
                    }
                } else if (!optOutTypeValidation) {
                    throw new IllegalStateException(
                            "unresolved side-output tag type for " + edge.src() + "\u2192" + edge.dst()
                                    + " — provide TypeInformation hints or opt out explicitly");
                }
            } else {
                // main channel
                boolean known = srcOut != null && dstIn != null;
                if (known && !srcOut.equals(dstIn)) {
                    throw new IllegalStateException(
                            "edge type mismatch: " + edge.src() + " outputs " + srcOut
                                    + " but " + edge.dst() + " expects " + dstIn);
                }
                if (!known && !optOutTypeValidation) {
                    throw new IllegalStateException(
                            "unresolved edge type for " + edge.src() + "\u2192" + edge.dst()
                                    + " — provide TypeInformation hints at registration or opt out explicitly");
                }
            }

            // keyed function sanity
            if (dst.requiresKeyedEdge() && !edge.keyed()) {
                throw new IllegalStateException(
                        "KeyedProcessFunction " + edge.dst() + " received unkeyed edge from " + edge.src());
            }
        }

        if (eagerInit) {
            for (NodeHarness node : nodes.values()) {
                node.openOnceEager();
            }
        }

        List<WorkflowNode> graph = buildGraph(nodes);
        Set<String> sourceIds = Set.copyOf(sources.keySet());
        Set<String> sinkIds = Set.copyOf(sinks.keySet());
        return new StandaloneWorkflow(nodes, edges, sourceIds, sinkIds, mode, graph);
    }

    private void validateTopology(Map<String, NodeHarness> nodes) {
        for (Edge edge : edges) {
            // a source must not have inbound edges
            if (nodeKinds.get(edge.dst()) == Kind.SOURCE) {
                throw new IllegalStateException(
                        "source node " + edge.dst() + " must not receive inbound edges (edge from " + edge.src() + ")");
            }
            // a sink must not have outbound edges
            if (nodeKinds.get(edge.src()) == Kind.SINK) {
                throw new IllegalStateException(
                        "sink node " + edge.src() + " must not have outbound edges (edge to " + edge.dst() + ")");
            }
        }
    }

    private String requireUnique(String id, String kind) {
        if (functions.containsKey(id) || sources.containsKey(id) || sinks.containsKey(id)) {
            throw new IllegalArgumentException("duplicate " + kind + " id: " + id);
        }
        return id;
    }

    private void registerAny(String id, Object fn, Kind kind) {
        if (fn == null) {
            throw new IllegalArgumentException("function for " + id + " is null");
        }
        requireUnique(id, "function");
        functions.put(id, fn);
        nodeKinds.put(id, kind);
    }

    private static <T> T keyedNotNull(T value, String what) {
        if (value == null) {
            throw new IllegalArgumentException(what + " type for keyed function is null");
        }
        return value;
    }

    private static NodeHarness requireNode(Map<String, NodeHarness> nodes, String id) {
        NodeHarness n = nodes.get(id);
        if (n == null) {
            throw new IllegalStateException("unknown node id: " + id);
        }
        return n;
    }

    private List<WorkflowNode> buildGraph(Map<String, NodeHarness> nodes) {
        Map<String, List<String>> successors = new LinkedHashMap<>();
        for (Edge edge : edges) {
            successors.computeIfAbsent(edge.src(), k -> new ArrayList<>()).add(edge.dst());
        }
        List<WorkflowNode> result = new ArrayList<>();
        for (Map.Entry<String, NodeHarness> entry : nodes.entrySet()) {
            String id = entry.getKey();
            Kind kind = nodeKinds.getOrDefault(id, Kind.FUNCTION);
            String inType = typeName(inputTypes.get(id));
            String outType = typeName(outputTypes.get(id));
            result.add(new WorkflowNode(id, kind, inType, outType,
                    List.copyOf(successors.getOrDefault(id, List.of()))));
        }
        return result;
    }

    private static String typeName(TypeInformation<?> t) {
        return t == null ? WorkflowNode.UNKNOWN_TYPE : t.toString();
    }
}