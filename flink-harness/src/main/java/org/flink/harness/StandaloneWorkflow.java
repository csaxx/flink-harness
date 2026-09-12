package org.flink.harness;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.util.clock.Clock;
import org.flink.harness.graph.function.KeyedProcessFunctionHarness;
import org.flink.harness.graph.function.NodeHarness;
import org.flink.harness.metrics.StandaloneMetricGroup;
import org.flink.harness.graph.result.FunctionResult;
import org.flink.harness.graph.result.WorkflowResult;
import org.flink.harness.timer.BackgroundTimerListener;
import org.flink.harness.timer.BackgroundTimerThread;
import org.flink.harness.timer.ProcessingTimerMode;
import org.flink.harness.timer.TimerHeap;
import org.flink.harness.timer.WorkflowTimerService;

import java.util.ArrayDeque;
import java.util.ArrayList;
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
 *
 * <p>Timer support: processing-time only in this version. Timers are keyed-only
 * (Flink-faithful). See {@link ProcessingTimerMode} for firing semantics.
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
    private final Clock clock;
    private final ProcessingTimerMode timerMode;
    private final List<KeyedProcessFunctionHarness> keyedHarnesses;
    private final WorkflowTimerService timerService;
    private final BackgroundTimerThread backgroundThread;
    private volatile boolean closed;

    private volatile Throwable bgFailure;

    StandaloneWorkflow(
            Map<String, NodeHarness> nodes,
            List<Edge> edges,
            Set<String> sourceIds,
            Set<String> sinkIds,
            Mode mode,
            List<WorkflowNode> graph,
            Clock clock,
            ProcessingTimerMode timerMode,
            List<KeyedProcessFunctionHarness> keyedHarnesses,
            BackgroundTimerListener bgListener) {
        this.nodes = nodes;
        this.edges = edges;
        this.sourceIds = sourceIds;
        this.sinkIds = sinkIds;
        this.outboundEdges = new LinkedHashMap<>();
        for (Edge edge : edges) {
            outboundEdges.computeIfAbsent(edge.src(), k -> new ArrayList<>()).add(edge);
        }
        this.mode = mode;
        this.graph = graph;
        this.clock = clock;
        this.timerMode = timerMode;
        this.keyedHarnesses = keyedHarnesses;
        this.timerService = new WorkflowTimerService(
                this::fireProcessingTimersInternal, this::pendingTimerCountInternal);

        // BACKGROUND mode gets one daemon poller for the whole workflow; other modes never start it
        if (timerMode == ProcessingTimerMode.BACKGROUND) {
            this.backgroundThread = new BackgroundTimerThread(
                    () -> backgroundFireAndRoute(),
                    bgListener);
            this.backgroundThread.start();
        } else {
            this.backgroundThread = null;
        }
    }

    /** Background-thread entry point: fires due timers under the workflow lock. Returns null when
     * nothing was due so the listener is not invoked with empty results. */
    private WorkflowResult backgroundFireAndRoute() {
        lock.lock();
        try {
            checkFailed();
            Deque<Invocation> queue = new ArrayDeque<>();
            fireDueTimers(clock.absoluteTimeMillis(), queue);
            if (queue.isEmpty()) {
                return null;
            }
            return drainBfsQueue(queue);
        } finally {
            lock.unlock();
        }
    }

    // --------------------------------------------------------------------------------------------
    // execute
    // --------------------------------------------------------------------------------------------

    /** Runs {@code inputs} through the graph starting at {@code sourceId}. The whole call, including
     * the TRANSIENT reset, happens under the workflow lock; the result is built before that reset. */
    public WorkflowResult process(List<?> inputs, String sourceId) {
        lock.lock();
        try {
            checkFailed();
            return doProcess(inputs, sourceId);
        } finally {
            if (mode == Mode.TRANSIENT) {
                resetTransient();
            }
            lock.unlock();
        }
    }

    public WorkflowTimerService getTimerService() {
        return timerService;
    }

    private WorkflowResult doProcess(List<?> inputs, String sourceId) {
        // every element starts as an invocation of the source node; the source is just a node
        if (!sourceIds.contains(sourceId)) {
            throw new IllegalArgumentException("unknown source id: " + sourceId
                    + "; registered sources: " + sourceIds);
        }

        Deque<Invocation> queue = new ArrayDeque<>();
        for (Object input : inputs) {
            queue.add(new Invocation(sourceId, input, null));
        }

        if (timerMode == ProcessingTimerMode.OPPORTUNISTIC) {
            return drainBfsQueueOpportunistic(queue);
        }
        return drainBfsQueue(queue);
    }

    // --------------------------------------------------------------------------------------------
    // BFS queue drainage
    // --------------------------------------------------------------------------------------------

    /** FIFO breadth-first drain of the invocation queue. Only sink outputs are collected; every
     * node's outputs are routed onward via {@link #routeResult}. */
    private WorkflowResult drainBfsQueue(Deque<Invocation> queue) {
        Map<String, List<Object>> outputsAgg = new LinkedHashMap<>();

        while (!queue.isEmpty()) {
            Invocation inv = queue.poll();
            NodeHarness node = nodes.get(inv.functionId());
            if (node == null) {
                throw new IllegalStateException("unknown node id: " + inv.functionId());
            }

            FunctionResult<?> result = node.processViaEdge(inv.element(), inv.inboundEdge());

            if (sinkIds.contains(inv.functionId())) {
                outputsAgg.computeIfAbsent(inv.functionId(), k -> new ArrayList<>())
                        .addAll(result.outputs());
            }

            routeResult(inv.functionId(), result, queue);
        }

        return buildWorkflowResult(outputsAgg);
    }

    /** OPPORTUNISTIC drain: fires due timers after every element so timers scheduled in the past
     * (or by the element just processed) run immediately, without waiting for the queue to empty. */
    private WorkflowResult drainBfsQueueOpportunistic(Deque<Invocation> queue) {
        Map<String, List<Object>> outputsAgg = new LinkedHashMap<>();

        while (!queue.isEmpty()) {
            Invocation inv = queue.poll();
            NodeHarness node = nodes.get(inv.functionId());
            if (node == null) {
                throw new IllegalStateException("unknown node id: " + inv.functionId());
            }

            FunctionResult<?> result = node.processViaEdge(inv.element(), inv.inboundEdge());

            if (sinkIds.contains(inv.functionId())) {
                outputsAgg.computeIfAbsent(inv.functionId(), k -> new ArrayList<>())
                        .addAll(result.outputs());
            }

            routeResult(inv.functionId(), result, queue);

            fireDueTimers(clock.absoluteTimeMillis(), queue);

            // keep draining anything the timers just produced (including further timer firings)
            while (!queue.isEmpty()) {
                Invocation nxt = queue.poll();
                NodeHarness n = nodes.get(nxt.functionId());
                if (n == null) throw new IllegalStateException("unknown node id: " + nxt.functionId());

                FunctionResult<?> nr = n.processViaEdge(nxt.element(), nxt.inboundEdge());

                if (sinkIds.contains(nxt.functionId())) {
                    outputsAgg.computeIfAbsent(nxt.functionId(), k -> new ArrayList<>())
                            .addAll(nr.outputs());
                }

                routeResult(nxt.functionId(), nr, queue);

                fireDueTimers(clock.absoluteTimeMillis(), queue);
            }

            fireDueTimers(clock.absoluteTimeMillis(), queue);
        }

        return buildWorkflowResult(outputsAgg);
    }

    /** Sends a node's result along its outbound edges. A side output with no matching edge is
     * silently dropped; main-channel outputs from non-sink nodes are only used for routing. */
    private void routeResult(String srcId, FunctionResult<?> result, Deque<Invocation> queue) {
        List<Edge> outbound = outboundEdges.getOrDefault(srcId, List.of());
        for (Edge edge : outbound) {
            List<?> transported;
            if (edge.sideChannel()) {
                // side channel: only values the source explicitly emitted for this tag
                transported = result.sideOutputs().get(edge.sideTag());
                if (transported == null) {
                    continue;
                }
            } else {
                transported = result.outputs();
            }
            for (Object out : transported) {
                queue.add(new Invocation(edge.dst(), out, edge));
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Timer management
    // --------------------------------------------------------------------------------------------

    /** Explicit firing path behind {@code getTimerService().fireProcessingTimers()}; used by MANUAL
     * mode and as a nudge in the other modes. Fires all due timers, then drains their outputs. */
    private WorkflowResult fireProcessingTimersInternal() {
        lock.lock();
        try {
            checkFailed();
            Deque<Invocation> queue = new ArrayDeque<>();
            fireDueTimers(clock.absoluteTimeMillis(), queue);
            Map<String, List<Object>> outputsAgg = new LinkedHashMap<>();
            while (!queue.isEmpty()) {
                Invocation inv = queue.poll();
                NodeHarness node = nodes.get(inv.functionId());
                if (node == null) {
                    throw new IllegalStateException("unknown node id: " + inv.functionId());
                }
                FunctionResult<?> result = node.processViaEdge(inv.element(), inv.inboundEdge());

                if (sinkIds.contains(inv.functionId())) {
                    outputsAgg.computeIfAbsent(inv.functionId(), k -> new ArrayList<>())
                            .addAll(result.outputs());
                }

                routeResult(inv.functionId(), result, queue);
            }
            return buildWorkflowResult(outputsAgg);
        } finally {
            lock.unlock();
        }
    }

    /** Polls every keyed harness for timers due at {@code now} and routes their outputs into the
     * same queue as element outputs, so timers can trigger downstream functions and sinks. */
    private void fireDueTimers(long now, Deque<Invocation> queue) {
        for (KeyedProcessFunctionHarness harness : keyedHarnesses) {
            TimerHeap.TimerEntry entry;
            while ((entry = harness.timerHeap().pollDue(now)) != null) {
                FunctionResult<?> result = harness.fireTimer(entry);
                routeResult(harness.getId(), result, queue);
            }
        }
    }

    void wakeBackground() {
        if (backgroundThread != null) {
            backgroundThread.notifyWake();
        }
    }

    private long pendingTimerCountInternal() {
        long total = 0;
        for (KeyedProcessFunctionHarness h : keyedHarnesses) {
            total += h.timerHeapSize();
        }
        return total;
    }

    // --------------------------------------------------------------------------------------------
    // Result aggregation
    // --------------------------------------------------------------------------------------------

    /** Assembles the per-node results and the aggregate metrics. Only sinks contribute outputs
     * ({@code outputsAgg} is filled only for sink ids); a node appears iff it has sink outputs or a
     * non-empty metrics snapshot. Side outputs are intentionally empty here — they were already
     * routed to downstream sinks by {@link #routeResult}, so read them from the sink's outputs.
     * Called before the TRANSIENT reset, which is why TRANSIENT results still carry their metrics. */
    private WorkflowResult buildWorkflowResult(Map<String, List<Object>> outputsAgg) {
        Map<String, FunctionResult<Object>> results = new LinkedHashMap<>();
        for (Map.Entry<String, NodeHarness> entry : nodes.entrySet()) {
            String id = entry.getKey();
            List<Object> outs = List.copyOf(outputsAgg.getOrDefault(id, List.of()));
            Map<String, Object> metricSnap = entry.getValue().metricsSnapshot();

            boolean hasOutputs = !outs.isEmpty();
            boolean hasMetrics = !metricSnap.isEmpty();
            if (hasOutputs || hasMetrics) {
                results.put(id, new FunctionResult<>(outs, Map.of(), metricSnap));
            }
        }

        Map<String, Object> aggregated = aggregateAllMetrics();
        return new WorkflowResult(results, aggregated);
    }

    /** Cross-node flat metrics. Counters/meters/histograms are summed and gauges are last-wins in
     * node registration order; metric names collide across nodes by design (no node namespacing). */
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

    // --------------------------------------------------------------------------------------------
    // lifecycle
    // --------------------------------------------------------------------------------------------

    /** Fails every subsequent locked operation once the background timer thread has died. This is
     * deliberate: a half-dead poller would silently stop firing timers, so the workflow is poisoned
     * and callers must build a new one. */
    private void checkFailed() {
        if (bgFailure != null) {
            throw new RuntimeException(
                    "Background timer thread failed with: " + bgFailure.getMessage(), bgFailure);
        }
        if (backgroundThread != null && backgroundThread.state() == BackgroundTimerThread.State.FAILED) {
            bgFailure = backgroundThread.failure();
            throw new RuntimeException(
                    "Background timer thread failed with: " + bgFailure.getMessage(), bgFailure);
        }
    }

    /** TRANSIENT teardown, invoked from {@code process()} finally so it also runs when a run throws. */
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

    /** Idempotent teardown: stops the background poller first, then closes every node (nodes that
     * were never opened close as no-ops). */
    public void close() {
        lock.lock();
        try {
            if (!closed) {
                closed = true;
                if (backgroundThread != null) {
                    backgroundThread.close();
                }
                nodes.values().forEach(NodeHarness::close);
            }
        } finally {
            lock.unlock();
        }
    }

    // --------------------------------------------------------------------------------------------

    private record Invocation(String functionId, Object element, Edge inboundEdge) {}
}