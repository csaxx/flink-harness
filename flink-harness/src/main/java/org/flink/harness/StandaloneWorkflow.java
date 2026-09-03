package org.flink.harness;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.util.OutputTag;
import org.flink.harness.harness.FunctionHarness;
import org.flink.harness.internal.StandaloneMetricGroup;
import org.flink.harness.result.FunctionResult;
import org.flink.harness.result.WorkflowResult;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Executable workflow graph built by {@link WorkflowBuilder}.
 * {@code process(inputs, entryId)} runs elements through the graph; see AGENTS.md for semantics.
 *
 * <p>Thread-safe by design: the entire {@link #process} call (and all clear/close operations)
 * is guarded by a single lock. Concurrent {@code process} invocations serialize on the same
 * workflow; separate workflow instances run without contention.
 */
public final class StandaloneWorkflow {

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, FunctionHarness> harnesses;
    private final List<Edge> edges;
    private final Map<String, KeySelector<?, ?>> entrySelectors;
    private final Map<String, List<Edge>> outboundEdges;
    private final Set<String> activatedOutputs;
    private final Map<String, List<OutputTag<?>>> activatedSideOutputs;
    private final Mode mode;
    private final List<WorkflowNode> graph;
    private boolean closed;

    StandaloneWorkflow(Map<String, FunctionHarness> harnesses, List<Edge> edges,
            Map<String, KeySelector<?, ?>> entrySelectors,
            Set<String> activatedOutputs, Map<String, List<OutputTag<?>>> activatedSideOutputs,
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

    /** Feed all elements through the graph starting at {@code entryFunctionId}.
     * Guarded by the workflow lock; in TRANSIENT mode the reset also happens within the lock. */
    public WorkflowResult process(List<?> inputs, String entryFunctionId) {
        lock.lock();
        try {
            return doProcess(inputs, entryFunctionId);
        } finally {
            if (mode == Mode.TRANSIENT) {
                resetTransient();
            }
            lock.unlock();
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

        // per-function accumulation
        Map<String, List<Object>> outputsAgg = new LinkedHashMap<>();
        Map<String, Map<OutputTag<?>, List<Object>>> sideAgg = new LinkedHashMap<>();

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

        // assemble per-function results (only functions with ≥1 of outputs/sideOutputs/metrics)
        Map<String, FunctionResult<Object>> results = new LinkedHashMap<>();
        for (Map.Entry<String, FunctionHarness> entry : harnesses.entrySet()) {
            String id = entry.getKey();
            List<Object> outs = List.copyOf(outputsAgg.getOrDefault(id, List.of()));
            Map<OutputTag<?>, List<Object>> sides = sideAgg.getOrDefault(id, Map.of());
            Map<String, Object> metricSnap = entry.getValue().metricsSnapshot();

            boolean hasOutputs = !outs.isEmpty();
            boolean hasSides = !sides.isEmpty();
            boolean hasMetrics = !metricSnap.isEmpty();
            if (hasOutputs || hasSides || hasMetrics) {
                @SuppressWarnings("unchecked")
                Map<OutputTag<?>, List<?>> sidesCasted = (Map<OutputTag<?>, List<?>>) (Map<?, ?>) sides;
                results.put(id, new FunctionResult<>(outs, sidesCasted, metricSnap));
            }
        }

        Map<String, Object> aggregated = aggregateAllMetrics();
        return new WorkflowResult(results, aggregated);
    }

    @SuppressWarnings("unchecked")
    private void aggregateSideOutputs(String functionId, FunctionResult<?> result,
            Map<String, Map<OutputTag<?>, List<Object>>> sideAgg) {
        List<OutputTag<?>> tags = activatedSideOutputs.get(functionId);
        if (tags == null) {
            return;
        }
        for (OutputTag<?> tag : tags) {
            List<?> values = result.sideOutputs().get(tag);
            if (values != null && !values.isEmpty()) {
                sideAgg.computeIfAbsent(functionId, k -> new LinkedHashMap<>())
                        .computeIfAbsent(tag, k -> new java.util.ArrayList<>())
                        .addAll((List<Object>) values);
            }
        }
    }

    /** Aggregate metrics across all harnesses: counters/meters/histograms summed, gauges last-wins. */
    private Map<String, Object> aggregateAllMetrics() {
        Map<String, Object> aggregated = new LinkedHashMap<>();
        for (FunctionHarness harness : harnesses.values()) {
            for (Map.Entry<String, Metric> entry : metricInstances(harness).entrySet()) {
                String name = entry.getKey();
                Metric metric = entry.getValue();
                if (metric instanceof Gauge<?> gauge) {
                    aggregated.put(name, gauge.getValue()); // last-wins; registration order is deterministic
                } else if (metric instanceof Counter counter) {
                    aggregated.merge(name, counter.getCount(), (a, b) -> ((Number) a).longValue() + ((Number) b).longValue());
                } else if (metric instanceof Meter meter) {
                    aggregated.merge(name, meter.getCount(), (a, b) -> ((Number) a).longValue() + ((Number) b).longValue());
                } else if (metric instanceof Histogram histogram) {
                    aggregated.merge(name, histogram.getCount(), (a, b) -> ((Number) a).longValue() + ((Number) b).longValue());
                }
            }
        }
        return aggregated;
    }

    private Map<String, Metric> metricInstances(FunctionHarness harness) {
        return ((StandaloneMetricGroup) harness.unwrapMetricGroup()).metricInstances();
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
        lock.lock();
        try {
            require(functionId).clearState();
        } finally {
            lock.unlock();
        }
    }

    public void clearStateAll() {
        lock.lock();
        try {
            harnesses.values().forEach(FunctionHarness::clearState);
        } finally {
            lock.unlock();
        }
    }

    public void clearMetrics(String functionId) {
        lock.lock();
        try {
            require(functionId).clearMetrics();
        } finally {
            lock.unlock();
        }
    }

    public void clearMetricsAll() {
        lock.lock();
        try {
            harnesses.values().forEach(FunctionHarness::clearMetrics);
        } finally {
            lock.unlock();
        }
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
        lock.lock();
        try {
            if (!closed) {
                closed = true;
                harnesses.values().forEach(FunctionHarness::close);
            }
        } finally {
            lock.unlock();
        }
    }

    // --------------------------------------------------------------------------------------------

    private record Invocation(String functionId, Object element, Edge inboundEdge) {}
}