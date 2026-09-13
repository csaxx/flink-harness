package org.flink.harness;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.util.clock.Clock;
import org.flink.harness.graph.StreamEdge;
import org.flink.harness.graph.StreamNode;
import org.flink.harness.graph.WorkflowStreamGraph;
import org.flink.harness.graph.function.rich.AbstractRichFunctionHarness;
import org.flink.harness.graph.function.rich.KeyedProcessFunctionHarness;
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
import java.util.concurrent.locks.ReentrantLock;

/**
 * Executable workflow built by {@link WorkflowBuilder}.
 * {@code process(inputs, sourceId)} runs elements through the {@link WorkflowStreamGraph}
 * starting at a source node. See AGENTS.md for semantics.
 *
 * <p>Purely an execution engine: the graph layout (nodes, edges, types, validation) lives in
 * {@link WorkflowStreamGraph}; this class owns routing, result aggregation, timers, locking
 * and the CONTINUOUS/TRANSIENT lifecycle.
 *
 * <p>Thread-safe by design: the entire {@link #process} call (and all reset/close operations)
 * is guarded by a single lock. Concurrent {@code process} invocations serialize on the same
 * workflow; separate workflow instances run without contention.
 *
 * <p>Timer support: processing-time only in this version. Timers are keyed-only
 * (Flink-faithful). See {@link ProcessingTimerMode} for firing semantics.
 */
public final class StandaloneWorkflow {

    private final ReentrantLock lock = new ReentrantLock();
    private final WorkflowStreamGraph graph;
    private final Mode mode;
    private final Clock clock;
    private final ProcessingTimerMode timerMode;
    private final WorkflowTimerService timerService;
    private final BackgroundTimerThread backgroundThread;
    private volatile boolean closed;

    private volatile Throwable bgFailure;

    StandaloneWorkflow(
            WorkflowStreamGraph graph,
            Mode mode,
            Clock clock,
            ProcessingTimerMode timerMode,
            BackgroundTimerListener bgListener) {
        this.graph = graph;
        this.mode = mode;
        this.clock = clock;
        this.timerMode = timerMode;
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
            return drain(queue, false);
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

    public WorkflowStreamGraph graph() {
        return graph;
    }

    private WorkflowResult doProcess(List<?> inputs, String sourceId) {
        // every element starts as an invocation of the source node; the source is just a node
        if (!graph.sourceIds().contains(sourceId)) {
            throw new IllegalArgumentException("unknown source id: " + sourceId
                    + "; registered sources: " + graph.sourceIds());
        }

        Deque<Invocation> queue = new ArrayDeque<>();
        for (Object input : inputs) {
            queue.add(new Invocation(sourceId, input, null));
        }

        return drain(queue, timerMode == ProcessingTimerMode.OPPORTUNISTIC);
    }

    // --------------------------------------------------------------------------------------------
    // BFS queue drainage
    // --------------------------------------------------------------------------------------------

    /** FIFO breadth-first drain of the invocation queue; the single drain path behind
     * {@code process()}, explicit timer firing and background firing. When
     * {@code opportunistic}, due timers fire after every element (and thus also after the
     * queue drains), so timers scheduled in the past or by the element just processed run
     * immediately. Only sink outputs are collected; every node's outputs are routed onward. */
    private WorkflowResult drain(Deque<Invocation> queue, boolean opportunistic) {
        Map<String, List<Object>> outputsAgg = new LinkedHashMap<>();
        while (!queue.isEmpty()) {
            step(queue.poll(), queue, outputsAgg);
            if (opportunistic) {
                fireDueTimers(clock.absoluteTimeMillis(), queue);
            }
        }
        return buildWorkflowResult(outputsAgg);
    }

    /** One invocation: invoke the node, collect its outputs when it is a sink, route onward. */
    private void step(Invocation inv, Deque<Invocation> queue, Map<String, List<Object>> outputsAgg) {
        StreamNode node = graph.node(inv.functionId());
        FunctionResult<?> result = node.processElement(inv.element(), inv.inboundEdge());

        if (graph.sinkIds().contains(inv.functionId())) {
            outputsAgg.computeIfAbsent(inv.functionId(), sinkId -> new ArrayList<>())
                    .addAll(result.outputs());
        }

        routeResult(inv.functionId(), result, queue);
    }

    /** Sends a node's result along its outbound edges. A side output with no matching edge is
     * silently dropped; main-channel outputs from non-sink nodes are only used for routing. */
    private void routeResult(String srcId, FunctionResult<?> result, Deque<Invocation> queue) {
        for (StreamEdge edge : graph.outboundEdgesOf(srcId)) {
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
            return drain(queue, false);
        } finally {
            lock.unlock();
        }
    }

    /** Polls every keyed harness for timers due at {@code now} and routes their outputs into the
     * same queue as element outputs, so timers can trigger downstream functions and sinks. */
    private void fireDueTimers(long now, Deque<Invocation> queue) {
        for (KeyedProcessFunctionHarness harness : graph.keyedHarnesses()) {
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
        for (KeyedProcessFunctionHarness keyedHarness : graph.keyedHarnesses()) {
            total += keyedHarness.timerHeapSize();
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
        for (Map.Entry<String, StreamNode> entry : graph.nodes().entrySet()) {
            String id = entry.getKey();
            List<Object> outs = List.copyOf(outputsAgg.getOrDefault(id, List.of()));
            Map<String, Object> metricSnap = metricsOf(entry.getValue());

            boolean hasOutputs = !outs.isEmpty();
            boolean hasMetrics = !metricSnap.isEmpty();
            if (hasOutputs || hasMetrics) {
                results.put(id, new FunctionResult<>(outs, Map.of(), metricSnap));
            }
        }

        Map<String, Object> aggregated = aggregateAllMetrics();
        return new WorkflowResult(results, aggregated);
    }

    /** Only rich functions carry metrics; every other node contributes an empty snapshot. */
    private static Map<String, Object> metricsOf(StreamNode node) {
        if (node instanceof AbstractRichFunctionHarness<?> richHarness) {
            return richHarness.metricsSnapshot();
        }
        return Map.of();
    }

    /** Cross-node flat metrics. Counters/meters/histograms are summed and gauges are last-wins in
     * node registration order; metric names collide across nodes by design (no node namespacing). */
    private Map<String, Object> aggregateAllMetrics() {
        Map<String, Object> aggregated = new LinkedHashMap<>();
        for (StreamNode node : graph.nodes().values()) {
            if (!(node instanceof AbstractRichFunctionHarness<?> richHarness)) {
                continue;
            }
            for (Map.Entry<String, Metric> entry : metricInstances(richHarness).entrySet()) {
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

    private Map<String, Metric> metricInstances(AbstractRichFunctionHarness<?> richHarness) {
        return richHarness.metricGroup().metricInstances();
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
        for (StreamNode node : graph.nodes().values()) {
            node.resetAll();
        }
    }

    // --------------------------------------------------------------------------------------------
    // global state / metrics management
    // --------------------------------------------------------------------------------------------

    public void resetState(String nodeId) {
        lock.lock();
        try {
            graph.node(nodeId).resetState();
        } finally {
            lock.unlock();
        }
    }

    public void resetStateAll() {
        lock.lock();
        try {
            graph.nodes().values().forEach(StreamNode::resetState);
        } finally {
            lock.unlock();
        }
    }

    public void resetMetrics(String nodeId) {
        lock.lock();
        try {
            graph.node(nodeId).resetMetrics();
        } finally {
            lock.unlock();
        }
    }

    public void resetMetricsAll() {
        lock.lock();
        try {
            graph.nodes().values().forEach(StreamNode::resetMetrics);
        } finally {
            lock.unlock();
        }
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
                graph.nodes().values().forEach(StreamNode::close);
            }
        } finally {
            lock.unlock();
        }
    }

    // --------------------------------------------------------------------------------------------

    private record Invocation(String functionId, Object element, StreamEdge inboundEdge) {}
}
