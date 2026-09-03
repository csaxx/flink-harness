package org.flink.harness;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.OutputTag;
import org.flink.harness.harness.FunctionHarness;
import org.flink.harness.harness.HarnessFactory;
import org.flink.harness.internal.SideOutputActivation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fluent builder for {@link StandaloneWorkflow}. Edges route main outputs only; side
 * outputs terminate via {@link #activateSideOutput(String, OutputTag)} (sink-equivalent).
 */
public final class WorkflowBuilder {

    private final Mode mode;
    private final boolean threadSafe;
    private final Map<String, Object> functions = new LinkedHashMap<>();
    private final Map<String, TypeInformation<?>> inputTypes = new LinkedHashMap<>();
    private final Map<String, TypeInformation<?>> outputTypes = new LinkedHashMap<>();
    private final List<Edge> edges = new ArrayList<>();
    private final Map<String, KeySelector<?, ?>> entrySelectors = new LinkedHashMap<>();
    private final Set<String> activatedOutputs = new LinkedHashSet<>();
    private final Set<SideOutputActivation> activatedSideOutputs = new LinkedHashSet<>();

    public WorkflowBuilder(Mode mode, boolean threadSafe) {
        this.mode = mode;
        this.threadSafe = threadSafe;
    }

    // --------------------------------------------------------------------------------------------
    // function registration
    // --------------------------------------------------------------------------------------------

    /** Register function, type-unresolved (edges to/from it will be untyped). */
    public WorkflowBuilder registerFunction(String id, Function function) {
        register(id, function);
        return this;
    }

    /** Register function with explicit input/output type hints. */
    public WorkflowBuilder registerFunction(String id, Function function,
            TypeInformation<?> inputType, TypeInformation<?> outputType) {
        register(id, function);
        inputTypes.put(id, inputType);
        outputTypes.put(id, outputType);
        return this;
    }

    /** Register a {@code KeyedProcessFunction}; must receive keyed edges only. */
    public WorkflowBuilder registerKeyedFunction(String id, org.apache.flink.streaming.api.functions.KeyedProcessFunction<?, ?, ?> function) {
        register(id, function);
        return this;
    }

    /** Same as above with explicit type hints. */
    public WorkflowBuilder registerKeyedFunction(String id, org.apache.flink.streaming.api.functions.KeyedProcessFunction<?, ?, ?> function,
            TypeInformation<?> inputType, TypeInformation<?> outputType) {
        register(id, function);
        inputTypes.put(id, keyedNotNull(inputType, "input"));
        outputTypes.put(id, keyedNotNull(outputType, "output"));
        return this;
    }

    /** Register a keyed function that is directly used as a workflow entrypoint —
     * the selector computes keys for elements fed via process(inputs, thisId). */
    public <IN, K> WorkflowBuilder registerKeyedFunction(String id,
            org.apache.flink.streaming.api.functions.KeyedProcessFunction<?, ?, ?> function,
            KeySelector<IN, K> entryKeySelector) {
        register(id, function);
        entrySelectors.put(id, entryKeySelector);
        return this;
    }

    /** Same with type hints as well. */
    public <IN, K> WorkflowBuilder registerKeyedFunction(String id,
            org.apache.flink.streaming.api.functions.KeyedProcessFunction<?, ?, ?> function,
            TypeInformation<?> inputType, TypeInformation<?> outputType,
            KeySelector<IN, K> entryKeySelector) {
        register(id, function);
        inputTypes.put(id, keyedNotNull(inputType, "input"));
        outputTypes.put(id, keyedNotNull(outputType, "output"));
        entrySelectors.put(id, entryKeySelector);
        return this;
    }

    private Object register(String id, Object function) {
        if (functions.putIfAbsent(id, requireNonNullFunction(id, function)) != null) {
            throw new IllegalArgumentException("duplicate function id: " + id);
        }
        return function;
    }

    private static Object requireNonNullFunction(String id, Object fn) {
        if (fn == null) {
            throw new IllegalArgumentException("function for " + id + " is null");
        }
        return fn;
    }

    private static <T> T keyedNotNull(T value, String what) {
        if (value == null) {
            throw new IllegalArgumentException(what + " type for keyed function is null");
        }
        return value;
    }

    // --------------------------------------------------------------------------------------------
    // edges
    // --------------------------------------------------------------------------------------------

    /** Untyped edge routing main outputs of {@code src} into {@code dst}. */
    public WorkflowBuilder addEdge(String src, String dst) {
        edges.add(new Edge(src, dst, null));
        return this;
    }

    /** Keyed edge: compute key per element — dst must be keyed-able (KeyProcess, Rich*). */
    public <IN, K> WorkflowBuilder addKeyedEdge(String src, String dst, KeySelector<IN, K> keySelector) {
        if (keySelector == null) {
            throw new IllegalArgumentException("keySelector must not be null for keyed edge");
        }
        edges.add(new Edge(src, dst, keySelector));
        return this;
    }

    // --------------------------------------------------------------------------------------------
    // sink equivalents
    // --------------------------------------------------------------------------------------------

    /** Aggregate the main outputs of this function into the workflow result. */
    public WorkflowBuilder activateOutput(String functionId) {
        activatedOutputs.add(functionId);
        return this;
    }

    /** Aggregate a specific side output of this function into the workflow result. */
    public WorkflowBuilder activateSideOutput(String functionId, OutputTag<?> sideOutputTag) {
        activatedSideOutputs.add(new SideOutputActivation(functionId, sideOutputTag));
        return this;
    }

    // --------------------------------------------------------------------------------------------
    // build
    // --------------------------------------------------------------------------------------------

    /** Build with strict type validation (fail on unresolved generics unless an edge opts out). */
    public StandaloneWorkflow build() {
        return build(false);
    }

    /** Build — opt-out of type validation with {@code optOutTypeValidation=true}. */
    public StandaloneWorkflow build(boolean optOutTypeValidation) {
        Map<String, FunctionHarness> harnesses = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : functions.entrySet()) {
            String id = entry.getKey();
            String inName = nameOrUnknown(inputTypes.get(id));
            String outName = nameOrUnknown(outputTypes.get(id));
            harnesses.put(id, HarnessFactory.create(id, entry.getValue(), threadSafe, inName, outName));
        }

        // edge type validation
        for (Edge edge : edges) {
            FunctionHarness src = requireHarness(harnesses, edge.src());
            FunctionHarness dst = requireHarness(harnesses, edge.dst());

            TypeInformation<?> srcOut = outputTypes.get(edge.src());
            TypeInformation<?> dstIn = inputTypes.get(edge.dst());
            boolean known = srcOut != null && dstIn != null;
            if (known && !srcOut.equals(dstIn)) {
                throw new IllegalStateException(
                        "edge type mismatch: " + edge.src() + " outputs " + srcOut
                                + " but " + edge.dst() + " expects " + dstIn);
            }
            if (!known && !optOutTypeValidation) {
                throw new IllegalStateException(
                        "unresolved edge type for " + edge.src() + "→" + edge.dst()
                                + " — provide TypeInformation hints at registration or opt out explicitly");
            }

            // keyed function sanity: all inbound edges to a KeyedProcessFunction must be keyed
            if (dst.requiresKeyedEdge() && !edge.keyed()) {
                throw new IllegalStateException(
                        "KeyedProcessFunction " + edge.dst() + " received unkeyed edge from " + edge.src());
            }
        }

        validateActivations(harnesses);
        List<WorkflowNode> graph = buildGraph(harnesses, edges);
        return new StandaloneWorkflow(harnesses, edges, entrySelectors, activatedOutputs, activatedSideOutputs, mode, graph);
    }

    private void validateActivations(Map<String, FunctionHarness> harnesses) {
        for (String id : activatedOutputs) {
            requireHarness(harnesses, id);
        }
        for (SideOutputActivation s : activatedSideOutputs) {
            requireHarness(harnesses, s.functionId());
        }
    }

    private static String nameOrUnknown(TypeInformation<?> type) {
        return type == null ? WorkflowNode.UNKNOWN_TYPE : type.toString();
    }

    private static FunctionHarness requireHarness(Map<String, FunctionHarness> harnesses, String id) {
        FunctionHarness h = harnesses.get(id);
        if (h == null) {
            throw new IllegalStateException("unknown function id: " + id);
        }
        return h;
    }

    private static List<WorkflowNode> buildGraph(Map<String, FunctionHarness> harnesses, List<Edge> edges) {
        Map<String, List<String>> successors = new LinkedHashMap<>();
        for (Edge edge : edges) {
            successors.computeIfAbsent(edge.src(), k -> new ArrayList<>()).add(edge.dst());
        }
        List<WorkflowNode> nodes = new ArrayList<>();
        for (Map.Entry<String, FunctionHarness> entry : harnesses.entrySet()) {
            FunctionHarness h = entry.getValue();
            nodes.add(new WorkflowNode(
                    entry.getKey(),
                    h.inputTypeName(),
                    h.outputTypeName(),
                    List.copyOf(successors.getOrDefault(entry.getKey(), List.of()))));
        }
        return nodes;
    }

    // --------------------------------------------------------------------------------------------
}