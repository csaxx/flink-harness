package org.flink.harness;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Executable workflow graph built by {@link WorkflowBuilder}.
 * {@code process(inputs, entryId)} runs elements through the graph; see AGENTS.md for semantics.
 */
public final class StandaloneWorkflow {

    private final Map<String, FunctionHarness> harnesses;
    private final List<Edge> edges;
    private final Map<String, KeySelector<?, ?>> entrySelectors;
    private final Map<String, List<Edge>> outboundEdges;
    private final Set<String> activatedOutputs;
    private final Set<SideOutputActivation> activatedSideOutputs;
    private final Mode mode;
    private final List<WorkflowNode> graph;
    private boolean closed;

    StandaloneWorkflow(Map<String, FunctionHarness> harnesses, List<Edge> edges,
            Map<String, KeySelector<?, ?>> entrySelectors,
            Set<String> activatedOutputs, Set<SideOutputActivation> activatedSideOutputs,
            Mode mode, List<WorkflowNode> graph) {
        this.harnesses = harnesses;
        this.edges = edges;
        this.entrySelectors = entrySelectors;
        this.outboundEdges = new LinkedHashMap<>();
        for (Edge edge : edges) {
            outboundEdges.computeIfAbsent(edge.src(), k -> new java.util.ArrayList<>()).add(edge);
        }
        this.activatedOutputs = activatedOutputs;
        this.activatedSideOutputs = activatedSideOutputs;
        this.mode = mode;
        this.graph = graph;
    }

    // --------------------------------------------------------------------------------------------
    // execute
    // --------------------------------------------------------------------------------------------

    /** Feed all elements through the graph starting at {@code entryFunctionId}. */
    public WorkflowResult process(List<?> inputs, String entryFunctionId) {
        try {
            return doProcess(inputs, entryFunctionId);
        } finally {
            if (mode == Mode.TRANSIENT) {
                resetTransient();
            }
        }
    }

    private WorkflowResult doProcess(List<?> inputs, String entryFunctionId) {
        FunctionHarness entryHarness = harnesses.get(entryFunctionId);
        if (entryHarness == null) {
            throw new IllegalStateException("unknown function id: " + entryFunctionId);
        }
        Edge entryEdge = null;
        if (entryHarness.requiresKeyedEdge()) {
            KeySelector<?, ?> selector = entrySelectors.get(entryFunctionId);
            if (selector == null) {
                throw new IllegalStateException(
                        "KeyedProcessFunction " + entryFunctionId
                                + " used as workflow entrypoint but no entry key selector was registered — "
                                + "use registerKeyedFunction(id, fn, keySelector)");
            }
            entryEdge = new Edge(null, entryFunctionId, selector);
        }

        Deque<Invocation> queue = new ArrayDeque<>();
        for (Object input : inputs) {
            queue.add(new Invocation(entryFunctionId, input, entryEdge));
        }

        Map<String, List<Object>> outputsAgg = new LinkedHashMap<>();
        Map<WorkflowResult.SideOutputKey<?>, List<Object>> sideAgg = new LinkedHashMap<>();

        while (!queue.isEmpty()) {
            Invocation inv = queue.poll();
            FunctionHarness harness = harnesses.get(inv.functionId());
            if (harness == null) {
                throw new IllegalStateException("unknown function id: " + inv.functionId());
            }

            FunctionResult<?> result = harness.processViaEdge(inv.element(), inv.inboundEdge());

            if (activatedOutputs.contains(inv.functionId())) {
                outputsAgg.computeIfAbsent(inv.functionId(), k -> new java.util.ArrayList<>())
                        .addAll(result.outputs());
            }
            aggregateSideOutputs(inv.functionId(), result, sideAgg);

            List<Edge> outbound = outboundEdges.getOrDefault(inv.functionId(), List.of());
            for (Edge edge : outbound) {
                for (Object out : result.outputs()) {
                    queue.add(new Invocation(edge.dst(), out, edge));
                }
            }
        }

        Map<String, Map<String, Object>> metrics = new LinkedHashMap<>();
        for (Map.Entry<String, FunctionHarness> entry : harnesses.entrySet()) {
            metrics.put(entry.getKey(), entry.getValue().metricsSnapshot());
        }

        return new WorkflowResult(outputsAgg, sideAgg, metrics);
    }

    private void aggregateSideOutputs(String functionId, FunctionResult<?> result,
            Map<WorkflowResult.SideOutputKey<?>, List<Object>> sideAgg) {
        for (SideOutputActivation activation : activatedSideOutputs) {
            if (!activation.functionId().equals(functionId)) {
                continue;
            }
            OutputTag<?> tag = activation.tag();
            List<?> values = result.sideOutputs().get(tag);
            if (values != null && !values.isEmpty()) {
                sideAgg.computeIfAbsent(
                                new WorkflowResult.SideOutputKey<>(functionId, tag),
                                k -> new java.util.ArrayList<>())
                        .addAll(values);
            }
        }
    }

    private void resetTransient() {
        for (FunctionHarness harness : harnesses.values()) {
            harness.resetAll();
        }
    }

    // --------------------------------------------------------------------------------------------
    // introspection
    // --------------------------------------------------------------------------------------------

    public Set<String> getFunctionIds() {
        return harnesses.keySet();
    }

    public Object getFunction(String id) {
        FunctionHarness h = harnesses.get(id);
        if (h == null) {
            throw new IllegalStateException("unknown function id: " + id);
        }
        return h.unwrap();
    }

    /** DAG for visualization — immutable copy of the graph. */
    public List<WorkflowNode> getWorkflow() {
        return List.copyOf(graph);
    }

    // --------------------------------------------------------------------------------------------
    // global state / metrics management
    // --------------------------------------------------------------------------------------------

    public void clearState(String functionId) {
        require(functionId).clearState();
    }

    public void clearStateAll() {
        harnesses.values().forEach(FunctionHarness::clearState);
    }

    public void clearMetrics(String functionId) {
        require(functionId).clearMetrics();
    }

    public void clearMetricsAll() {
        harnesses.values().forEach(FunctionHarness::clearMetrics);
    }

    private FunctionHarness require(String id) {
        FunctionHarness h = harnesses.get(id);
        if (h == null) {
            throw new IllegalStateException("unknown function id: " + id);
        }
        return h;
    }

    // --------------------------------------------------------------------------------------------
    // lifecycle
    // --------------------------------------------------------------------------------------------

    public void close() {
        if (!closed) {
            closed = true;
            harnesses.values().forEach(FunctionHarness::close);
        }
    }

    // --------------------------------------------------------------------------------------------

    private record Invocation(String functionId, Object element, Edge inboundEdge) {}
}