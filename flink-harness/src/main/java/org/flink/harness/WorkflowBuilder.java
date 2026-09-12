package org.flink.harness;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.flink.harness.graph.function.FunctionHarness;
import org.flink.harness.graph.function.HarnessFactory;
import org.flink.harness.graph.function.KeyedProcessFunctionHarness;
import org.flink.harness.graph.function.NodeHarness;
import org.flink.harness.graph.source.StandaloneSource;
import org.flink.harness.graph.sink.StandaloneSink;
import org.flink.harness.WorkflowNode.Kind;
import org.flink.harness.timer.BackgroundTimerListener;
import org.flink.harness.timer.ProcessingTimerMode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    private Clock clock = SystemClock.getInstance();
    private ProcessingTimerMode timerMode = ProcessingTimerMode.OPPORTUNISTIC;
    private BackgroundTimerListener bgListener;
    private Map<String, String> globalJobParameters = Map.of();

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
    // clock & timer mode
    // --------------------------------------------------------------------------------------------

    public WorkflowBuilder clock(Clock clock) {
        if (clock == null) {
            throw new IllegalArgumentException("clock must not be null");
        }
        this.clock = clock;
        return this;
    }

    public WorkflowBuilder setProcessingTimerMode(ProcessingTimerMode mode) {
        if (mode == null) {
            throw new IllegalArgumentException("timer mode must not be null");
        }
        this.timerMode = mode;
        return this;
    }

    /** Background mode is the only async firing path, so it is the only mode that may take a listener. */
    public WorkflowBuilder setProcessingTimerMode(
            ProcessingTimerMode mode, BackgroundTimerListener listener) {
        if (mode == null) {
            throw new IllegalArgumentException("timer mode must not be null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("BackgroundTimerListener must not be null");
        }
        if (mode != ProcessingTimerMode.BACKGROUND) {
            throw new IllegalArgumentException(
                    "BackgroundTimerListener is only valid with BACKGROUND mode");
        }
        this.timerMode = mode;
        this.bgListener = listener;
        return this;
    }

    // --------------------------------------------------------------------------------------------
    // global job parameters
    // --------------------------------------------------------------------------------------------

    public WorkflowBuilder globalJobParameters(Map<String, String> params) {
        if (params == null) {
            throw new IllegalArgumentException("globalJobParameters must not be null");
        }
        this.globalJobParameters = Map.copyOf(params);
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

    public WorkflowBuilder addSource(String id) {
        sources.put(requireUnique(id, "source"), new StandaloneSource<>());
        nodeKinds.put(id, Kind.SOURCE);
        return this;
    }

    public WorkflowBuilder addSource(String id, TypeInformation<?> ioType) {
        addSource(id);
        inputTypes.put(id, ioType);
        outputTypes.put(id, ioType);
        return this;
    }

    public WorkflowBuilder addSource(String id, TypeInformation<?> inType, TypeInformation<?> outType) {
        addSource(id);
        inputTypes.put(id, inType);
        outputTypes.put(id, outType);
        return this;
    }

    public WorkflowBuilder addSource(String id, StandaloneSource<?, ?> source) {
        sources.put(requireUnique(id, "source"), source);
        nodeKinds.put(id, Kind.SOURCE);
        return this;
    }

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

    public WorkflowBuilder addEdge(String src, String dst) {
        edges.add(new Edge(src, dst, null, null));
        return this;
    }

    /** Keyed main channel: the destination binds {@code keySelector(element)} as its current key
     * before invoking, which is what makes keyed state and timers work. */
    public <IN, K> WorkflowBuilder addKeyedEdge(String src, String dst, KeySelector<IN, K> keySelector) {
        if (keySelector == null) {
            throw new IllegalArgumentException("keySelector must not be null for keyed edge");
        }
        edges.add(new Edge(src, dst, keySelector, null));
        return this;
    }

    /** Side channel: transports only the source's {@code ctx.output(tag)} values. An untyped tag
     * makes the edge unresolvable, so {@code build()} fails unless validation is opted out. */
    public WorkflowBuilder addSideOutputEdge(String src, String dst, OutputTag<?> tag) {
        if (tag == null) {
            throw new IllegalArgumentException("tag must not be null for side-output edge");
        }
        edges.add(new Edge(src, dst, null, tag));
        return this;
    }

    /** Side channel whose values are additionally keyed at the destination (see addKeyedEdge). */
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

    public WorkflowBuilder addSourceEdge(String src, String dst) {
        return addEdge(src, dst);
    }

    public <IN, K> WorkflowBuilder addSourceEdge(String src, String dst, KeySelector<IN, K> keySelector) {
        return addKeyedEdge(src, dst, keySelector);
    }

    public WorkflowBuilder addSinkEdge(String src, String dst) {
        return addEdge(src, dst);
    }

    public WorkflowBuilder addSinkEdge(String src, String dst, OutputTag<?> tag) {
        return addSideOutputEdge(src, dst, tag);
    }

    // --------------------------------------------------------------------------------------------
    // build
    // --------------------------------------------------------------------------------------------

    public StandaloneWorkflow build() {
        return build(false);
    }

    /**
     * Assembles and validates the graph. Anything checkable statically fails here rather than at
     * run time: mode/timer-mode guards, topology, edge types, and keyed-edge requirements. The one
     * deferral is opt-out type validation, which turns type errors into per-element
     * {@code ClassCastException}s instead. Also eagerly opens nodes when requested and collects the
     * keyed harnesses the workflow uses to fire timers.
     */
    public StandaloneWorkflow build(boolean optOutTypeValidation) {
        if (mode == Mode.TRANSIENT && timerMode != ProcessingTimerMode.OPPORTUNISTIC) {
            throw new IllegalStateException(
                    "Processing timer mode " + timerMode + " is not supported in TRANSIENT mode");
        }
        if (timerMode == ProcessingTimerMode.BACKGROUND && bgListener == null) {
            throw new IllegalStateException(
                    "BACKGROUND mode requires a BackgroundTimerListener — "
                            + "use setProcessingTimerMode(BACKGROUND, listener)");
        }

        // 1. build harnesses for Flink functions
        Map<String, NodeHarness> nodes = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : functions.entrySet()) {
            FunctionHarness h = HarnessFactory.create(entry.getKey(), entry.getValue(), clock, mode);
            h.setGlobalJobParameters(globalJobParameters);
            nodes.put(entry.getKey(), h);
        }

        // 2. add sources and sinks directly
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

            // side channel: the tag's declared type must match the destination input type
            if (edge.sideChannel()) {
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
                // main channel: known endpoint types must be equal; either side unknown = unresolved
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

            // a keyed function is meaningless without a key selector on every inbound edge
            if (dst.requiresKeyedEdge() && !edge.keyed()) {
                throw new IllegalStateException(
                        "KeyedProcessFunction " + edge.dst() + " received unkeyed edge from " + edge.src());
            }
        }

        // eager open turns open() failures into build-time failures
        if (eagerInit) {
            for (NodeHarness node : nodes.values()) {
                node.openOnceEager();
            }
        }

        List<WorkflowNode> graph = buildGraph(nodes);
        Set<String> sourceIds = Set.copyOf(sources.keySet());
        Set<String> sinkIds = Set.copyOf(sinks.keySet());

        // collect keyed harnesses for timer management
        List<KeyedProcessFunctionHarness> keyedHarnesses = nodes.values().stream()
                .filter(n -> n instanceof KeyedProcessFunctionHarness)
                .map(n -> (KeyedProcessFunctionHarness) n)
                .collect(Collectors.toList());

        return new StandaloneWorkflow(
                nodes, edges, sourceIds, sinkIds, mode, graph,
                clock, timerMode, keyedHarnesses, bgListener);
    }

    // --------------------------------------------------------------------------------------------
    // private helpers (unchanged)
    // --------------------------------------------------------------------------------------------

    /** Sources are roots and sinks are leaves; only functions may sit in between. */
    private void validateTopology(Map<String, NodeHarness> nodes) {
        for (Edge edge : edges) {
            if (nodeKinds.get(edge.dst()) == Kind.SOURCE) {
                throw new IllegalStateException(
                        "source node " + edge.dst() + " must not receive inbound edges (edge from " + edge.src() + ")");
            }
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

    /** Builds the introspection-only DAG exposed by {@code getWorkflow()}; successors come from edges. */
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