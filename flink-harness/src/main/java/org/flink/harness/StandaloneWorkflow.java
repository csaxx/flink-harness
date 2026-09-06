package org.flink.harness;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.flink.harness.harness.NodeHarness;
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
 * {@code process(inputs, sourceId)} runs elements through the graph starting at a source node.
 * See AGENTS.md for semantics.
 *
 * <p>Thread-safe by design: the entire {@link #process} call (and all clear/close operations)
 * is guarded by a single lock. Concurrent {@code process} invocations serialize on the same
 * workflow; separate workflow instances run without contention.
 */
public final class StandaloneWorkflow {

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, NodeHarness> nodes;
    private final List<Edge> edges;
    private final Map<String, List<Edge>> outboundEdges;
    private final Set<String> sourceIds;
    private final Set<String> sinkIds;
    private final Mode mode;
    private final List<WorkflowNode> graph;
    private boolean closed;

    StandaloneWorkflow(Map<String, NodeHarness> nodes, List<Edge> edges,
            Set<String> sourceIds, Set<String> sinkIds,
            Mode mode, List<WorkflowNode> graph) {
        this.nodes = nodes;
        this.edges = edges;
        this.sourceIds = sourceIds;
        this.sinkIds = sinkIds;
        this.outboundEdges = new LinkedHashMap<>();
        for (Edge edge : edges) {
            outboundEdges.computeIfAbsent(edge.src(), k -> new java.util.ArrayList<>()).add(edge);
        }
        this.mode = mode;
        this.graph = graph;
    }

    // --------------------------------------------------------------------------------------------
    // execute
    // --------------------------------------------------------------------------------------------

    /** Feed all elements through the graph starting at {@code sourceId}.
     * Guarded by the workflow lock; in TRANSIENT mode the reset also happens within the lock. */
    public WorkflowResult process(List<?> inputs, String sourceId) {
        lock.lock();
        try {
            return doProcess(inputs, sourceId);
        } finally {
            if (mode == Mode.TRANSIENT) {
                resetTransient();
            }
            lock.unlock();
        }
    }

    private WorkflowResult doProcess(List<?> inputs, String sourceId) {
        if (!sourceIds.contains(sourceId)) {
            throw new IllegalArgumentException("unknown source id: " + sourceId
                    + "; registered sources: " + sourceIds);
        }

        Deque<Invocation> queue = new ArrayDeque<>();
        for (Object input : inputs) {
            queue.add(new Invocation(sourceId, input, null));
        }

        Map<String, List<Object>> outputsAgg = new LinkedHashMap<>();

        while (!queue.isEmpty()) {
            Invocation inv = queue.poll();
            NodeHarness node = nodes.get(inv.functionId());
            if (node == null) {
                throw new IllegalStateException("unknown node id: " + inv.functionId());
            }

            FunctionResult<?> result = node.processViaEdge(inv.element(), inv.inboundEdge());

            // sinks collect outputs unconditionally
            if (sinkIds.contains(inv.functionId())) {
                outputsAgg.computeIfAbsent(inv.functionId(), k -> new java.util.ArrayList<>())
                        .addAll(result.outputs());
            }

            List<Edge> outbound = outboundEdges.getOrDefault(inv.functionId(), List.of());
            for (Edge edge : outbound) {
                List<?> transported;
                if (edge.sideChannel()) {
                    transported = result.sideOutputs().get(edge.sideTag());
                    if (transported == null) {
                        continue; // no elements for this side output
                    }
                } else {
                    transported = result.outputs();
                }
                for (Object out : transported) {
                    queue.add(new Invocation(edge.dst(), out, edge));
                }
            }
        }

        // per-function results (only functions with >=1 of outputs/sideOutputs/metrics)
        Map<String, FunctionResult<Object>> results = new LinkedHashMap<>();
        for (Map.Entry<String, NodeHarness> entry : nodes.entrySet()) {
            String id = entry.getKey();
            List<Object> outs = List.copyOf(outputsAgg.getOrDefault(id, List.of()));
            Map<String, Object> metricSnap = entry.getValue().metricsSnapshot();

            boolean hasOutputs = !outs.isEmpty();
            boolean hasMetrics = !metricSnap.isEmpty();
            // side outputs of non-sink nodes are routed via edges, not collected directly;
            // they still show up if the node happened to be a sink with side-channel edges
            if (hasOutputs || hasMetrics) {
                results.put(id, new FunctionResult<>(outs, Map.of(), metricSnap));
            }
        }

        Map<String, Object> aggregated = aggregateAllMetrics();
        return new WorkflowResult(results, aggregated);
    }

    /** Aggregate metrics across all nodes: counters/meters/histograms summed, gauges last-wins. */
    private Map<String, Object> aggregateAllMetrics() {
        Map<String, Object> aggregated = new LinkedHashMap<>();
        for (NodeHarness node : nodes.values()) {
            for (Map.Entry<String, Metric> entry : metricInstances(node).entrySet()) {
                String name = entry.getKey();
                Metric metric = entry.getValue();
                if (metric instanceof Gauge<?> gauge) {
                    aggregated.put(name, gauge.getValue());
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

    private Map<String, Metric> metricInstances(NodeHarness node) {
        return ((StandaloneMetricGroup) node.unwrapMetricGroup()).metricInstances();
    }

    private void resetTransient() {
        for (NodeHarness node : nodes.values()) {
            node.resetAll();
        }
    }

    // --------------------------------------------------------------------------------------------
    // introspection
    // --------------------------------------------------------------------------------------------

    public Set<String> getNodeIds() {
        return nodes.keySet();
    }

    public Set<String> getSourceIds() {
        return sourceIds;
    }

    public Set<String> getSinkIds() {
        return sinkIds;
    }

    public Object getNode(String id) {
        NodeHarness n = nodes.get(id);
        if (n == null) {
            throw new IllegalStateException("unknown node id: " + id);
        }
        return n.unwrap();
    }

    public List<WorkflowNode> getWorkflow() {
        return List.copyOf(graph);
    }

    // --------------------------------------------------------------------------------------------
    // global state / metrics management
    // --------------------------------------------------------------------------------------------

    public void clearState(String nodeId) {
        lock.lock();
        try {
            require(nodeId).clearState();
        } finally {
            lock.unlock();
        }
    }

    public void clearStateAll() {
        lock.lock();
        try {
            nodes.values().forEach(NodeHarness::clearState);
        } finally {
            lock.unlock();
        }
    }

    public void clearMetrics(String nodeId) {
        lock.lock();
        try {
            require(nodeId).clearMetrics();
        } finally {
            lock.unlock();
        }
    }

    public void clearMetricsAll() {
        lock.lock();
        try {
            nodes.values().forEach(NodeHarness::clearMetrics);
        } finally {
            lock.unlock();
        }
    }

    private NodeHarness require(String id) {
        NodeHarness n = nodes.get(id);
        if (n == null) {
            throw new IllegalStateException("unknown node id: " + id);
        }
        return n;
    }

    // --------------------------------------------------------------------------------------------
    // lifecycle
    // --------------------------------------------------------------------------------------------

    public void close() {
        lock.lock();
        try {
            if (!closed) {
                closed = true;
                nodes.values().forEach(NodeHarness::close);
            }
        } finally {
            lock.unlock();
        }
    }

    // --------------------------------------------------------------------------------------------

    private record Invocation(String functionId, Object element, Edge inboundEdge) {}
}