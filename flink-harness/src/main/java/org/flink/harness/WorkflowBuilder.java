package org.flink.harness;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.flink.harness.graph.StreamEdge;
import org.flink.harness.graph.StreamNode;
import org.flink.harness.graph.WorkflowStreamGraph;
import org.flink.harness.graph.function.HarnessFactory;
import org.flink.harness.graph.source.StandaloneSource;
import org.flink.harness.graph.sink.StandaloneSink;
import org.flink.harness.timer.BackgroundTimerListener;
import org.flink.harness.timer.ProcessingTimerMode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Fluent builder for {@link StandaloneWorkflow}. Sources are the only entrypoints;
 * sinks are the only collection points. Side outputs route via channel-qualified edges.
 * Owns registration and execution config; structural validation lives in
 * {@link WorkflowStreamGraph}.
 */
public final class WorkflowBuilder {

    private final Mode mode;
    private final Map<String, Object> functions = new LinkedHashMap<>();
    private final Map<String, StandaloneSource<?, ?>> sources = new LinkedHashMap<>();
    private final Map<String, StandaloneSink<?>> sinks = new LinkedHashMap<>();
    private final Map<String, TypeInformation<?>> inputTypes = new LinkedHashMap<>();
    private final Map<String, TypeInformation<?>> outputTypes = new LinkedHashMap<>();
    private final List<StreamEdge> edges = new ArrayList<>();
    private boolean eagerInit;

    private Clock clock = SystemClock.getInstance();
    private ProcessingTimerMode timerMode = ProcessingTimerMode.OPPORTUNISTIC;
    private BackgroundTimerListener bgListener;
    private Map<String, String> globalJobParameters = Map.of();

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
        registerAny(id, function);
        return this;
    }

    public WorkflowBuilder registerFunction(String id, Function function,
            TypeInformation<?> inputType, TypeInformation<?> outputType) {
        registerAny(id, function);
        inputTypes.put(id, inputType);
        outputTypes.put(id, outputType);
        return this;
    }

    /** Keyed overloads exist only so callers never cast; overload resolution picks them whenever
     * the static type is {@link KeyedProcessFunction}. Type hints are mandatory here because a
     * keyed function without them is almost always a mistake. */
    public WorkflowBuilder registerFunction(String id, KeyedProcessFunction<?, ?, ?> function) {
        registerAny(id, function);
        return this;
    }

    public WorkflowBuilder registerFunction(String id, KeyedProcessFunction<?, ?, ?> function,
            TypeInformation<?> inputType, TypeInformation<?> outputType) {
        registerAny(id, function);
        inputTypes.put(id, keyedNotNull(inputType, "input"));
        outputTypes.put(id, keyedNotNull(outputType, "output"));
        return this;
    }

    // --------------------------------------------------------------------------------------------
    // source registration
    // --------------------------------------------------------------------------------------------

    public WorkflowBuilder addSource(String id) {
        sources.put(requireUnique(id, "source"), new StandaloneSource<>());
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
        return this;
    }

    public WorkflowBuilder addSink(String id, TypeInformation<?> inType) {
        addSink(id);
        inputTypes.put(id, inType);
        return this;
    }

    public WorkflowBuilder addSink(String id, StandaloneSink<?> sink) {
        sinks.put(requireUnique(id, "sink"), sink);
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
        edges.add(new StreamEdge(src, dst, null, null));
        return this;
    }

    /** Keyed main channel: the destination binds {@code keySelector(element)} as its current key
     * before invoking, which is what makes keyed state and timers work. */
    public <IN, K> WorkflowBuilder addKeyedEdge(String src, String dst, KeySelector<IN, K> keySelector) {
        if (keySelector == null) {
            throw new IllegalArgumentException("keySelector must not be null for keyed edge");
        }
        edges.add(new StreamEdge(src, dst, keySelector, null));
        return this;
    }

    /** Side channel: transports only the source's {@code ctx.output(tag)} values. An untyped tag
     * makes the edge unresolvable, so {@code build()} fails unless validation is opted out. */
    public WorkflowBuilder addSideOutputEdge(String src, String dst, OutputTag<?> tag) {
        if (tag == null) {
            throw new IllegalArgumentException("tag must not be null for side-output edge");
        }
        edges.add(new StreamEdge(src, dst, null, tag));
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
        edges.add(new StreamEdge(src, dst, keySelector, tag));
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
     * Assembles the {@link WorkflowStreamGraph} and wraps it in a {@link StandaloneWorkflow}.
     * Execution-config guards (mode/timer-mode compatibility) run here; all structural
     * validation (topology, edge types, keyed-edge requirements) happens inside
     * {@link WorkflowStreamGraph#create}. The one deferral is opt-out type validation, which
     * turns type errors into per-element {@code ClassCastException}s instead. Also eagerly opens
     * nodes when requested.
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

        // harnesses for Flink functions; sources and sinks are already StreamNodes
        Map<String, StreamNode> nodes = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : functions.entrySet()) {
            nodes.put(entry.getKey(), HarnessFactory.create(
                    entry.getKey(), entry.getValue(), clock, mode, globalJobParameters));
        }
        nodes.putAll(sources);
        nodes.putAll(sinks);

        WorkflowStreamGraph graph = WorkflowStreamGraph.create(
                nodes, edges, sources.keySet(), sinks.keySet(),
                inputTypes, outputTypes, optOutTypeValidation);

        // eager open turns open() failures into build-time failures
        if (eagerInit) {
            graph.openAll();
        }

        return new StandaloneWorkflow(graph, mode, clock, timerMode, bgListener);
    }

    // --------------------------------------------------------------------------------------------
    // private helpers
    // --------------------------------------------------------------------------------------------

    private String requireUnique(String id, String kind) {
        if (functions.containsKey(id) || sources.containsKey(id) || sinks.containsKey(id)) {
            throw new IllegalArgumentException("duplicate " + kind + " id: " + id);
        }
        return id;
    }

    private void registerAny(String id, Object fn) {
        if (fn == null) {
            throw new IllegalArgumentException("function for " + id + " is null");
        }
        requireUnique(id, "function");
        functions.put(id, fn);
    }

    private static <T> T keyedNotNull(T value, String what) {
        if (value == null) {
            throw new IllegalArgumentException(what + " type for keyed function is null");
        }
        return value;
    }
}
