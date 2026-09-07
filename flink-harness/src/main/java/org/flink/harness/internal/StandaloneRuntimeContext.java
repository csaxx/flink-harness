package org.flink.harness.internal;

import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.flink.harness.metrics.StandaloneOperatorMetricGroup;
import org.flink.harness.state.InMemoryKeyedStateStore;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Standalone {@link RuntimeContext}: metrics via {@link StandaloneOperatorMetricGroup},
 * keyed state v1 via {@link InMemoryKeyedStateStore}, global job parameters via
 * {@link #setGlobalJobParameters(Map)}, serializers via
 * {@link #createSerializer(TypeInformation)}. Everything else (v2 state, broadcast
 * vars, distributed cache, accumulators, ...) throws
 * {@link UnsupportedOperationException}. Accumulators are permanently out of scope
 * — use metrics ({@link #getMetricGroup()}) instead.
 */
public class StandaloneRuntimeContext implements RuntimeContext {

    private final StandaloneOperatorMetricGroup metricGroup;
    private final InMemoryKeyedStateStore stateStore;
    private final JobInfo jobInfo;
    private final TaskInfo taskInfo;
    private volatile Map<String, String> globalJobParameters = Map.of();

    public StandaloneRuntimeContext(String functionName) {
        this.metricGroup = new StandaloneOperatorMetricGroup(functionName);
        this.stateStore = new InMemoryKeyedStateStore();
        this.jobInfo = new JobInfo() {
            @Override
            public org.apache.flink.api.common.JobID getJobId() {
                return null;
            }

            @Override
            public String getJobName() {
                return functionName;
            }
        };
        this.taskInfo = new TaskInfo() {
            @Override
            public String getTaskName() {
                return functionName;
            }

            @Override
            public int getMaxNumberOfParallelSubtasks() {
                return 1;
            }

            @Override
            public int getIndexOfThisSubtask() {
                return 0;
            }

            @Override
            public int getNumberOfParallelSubtasks() {
                return 1;
            }

            @Override
            public int getAttemptNumber() {
                return 0;
            }

            @Override
            public String getTaskNameWithSubtasks() {
                return functionName + "(1/1)";
            }

            @Override
            public String getAllocationIDAsString() {
                return "standalone";
            }
        };
    }

    /** Access to the chained state store (used by harnesses to bind keys). */
    public InMemoryKeyedStateStore stateStore() {
        return stateStore;
    }

    /** Wires global job parameters (immutable). Called once at build time. */
    public void setGlobalJobParameters(Map<String, String> params) {
        this.globalJobParameters = Map.copyOf(params);
    }

    @Override
    public Map<String, String> getGlobalJobParameters() {
        return globalJobParameters;
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return metricGroup;
    }

    // --------------------------------------------------------------------------------------------
    // keyed state v1
    // --------------------------------------------------------------------------------------------

    @Override
    public <T> ValueState<T> getState(ValueStateDescriptor<T> descriptor) {
        return stateStore.getState(descriptor);
    }

    @Override
    public <T> ListState<T> getListState(ListStateDescriptor<T> descriptor) {
        return stateStore.getListState(descriptor);
    }

    @Override
    public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> descriptor) {
        return stateStore.getReducingState(descriptor);
    }

    @Override
    public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
            AggregatingStateDescriptor<IN, ACC, OUT> descriptor) {
        return stateStore.getAggregatingState(descriptor);
    }

    @Override
    public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> descriptor) {
        return stateStore.getMapState(descriptor);
    }

    // --------------------------------------------------------------------------------------------
    // state v2 (deferred)
    // --------------------------------------------------------------------------------------------

    @Override
    public <T> org.apache.flink.api.common.state.v2.ValueState<T> getState(
            org.apache.flink.api.common.state.v2.ValueStateDescriptor<T> descriptor) {
        throw unsupported("v2 state");
    }

    @Override
    public <T> org.apache.flink.api.common.state.v2.ListState<T> getListState(
            org.apache.flink.api.common.state.v2.ListStateDescriptor<T> descriptor) {
        throw unsupported("v2 state");
    }

    @Override
    public <T> org.apache.flink.api.common.state.v2.ReducingState<T> getReducingState(
            org.apache.flink.api.common.state.v2.ReducingStateDescriptor<T> descriptor) {
        throw unsupported("v2 state");
    }

    @Override
    public <IN, ACC, OUT> org.apache.flink.api.common.state.v2.AggregatingState<IN, OUT>
            getAggregatingState(
                    org.apache.flink.api.common.state.v2.AggregatingStateDescriptor<IN, ACC, OUT> descriptor) {
        throw unsupported("v2 state");
    }

    @Override
    public <UK, UV> org.apache.flink.api.common.state.v2.MapState<UK, UV> getMapState(
            org.apache.flink.api.common.state.v2.MapStateDescriptor<UK, UV> descriptor) {
        throw unsupported("v2 state");
    }

    // --------------------------------------------------------------------------------------------
    // supported operations (formerly unsupported — now implemented with defaults)
    // --------------------------------------------------------------------------------------------

    @Override
    public <T> TypeSerializer<T> createSerializer(TypeInformation<T> typeInformation) {
        return typeInformation.createSerializer(new SerializerConfigImpl());
    }

    // --------------------------------------------------------------------------------------------
    // unsupported / permanently-out-of-scope operations
    // --------------------------------------------------------------------------------------------

    @Override
    public boolean isObjectReuseEnabled() {
        return false;
    }

    @Override
    public ClassLoader getUserCodeClassLoader() {
        return getClass().getClassLoader();
    }

    @Override
    public void registerUserCodeClassLoaderReleaseHookIfAbsent(String releaseHookName, Runnable releaseHook) {
        // no-op for standalone — class loader is never released
    }

    @Override
    public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {
        throw permanentlyOutOfScope("accumulators");
    }

    @Override
    public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
        throw permanentlyOutOfScope("accumulators");
    }

    @Override
    public IntCounter getIntCounter(String name) {
        throw permanentlyOutOfScope("accumulators");
    }

    @Override
    public LongCounter getLongCounter(String name) {
        throw permanentlyOutOfScope("accumulators");
    }

    @Override
    public DoubleCounter getDoubleCounter(String name) {
        throw permanentlyOutOfScope("accumulators");
    }

    @Override
    public Histogram getHistogram(String name) {
        throw permanentlyOutOfScope("accumulators");
    }

    @Override
    public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
        throw unsupported("externalResourceInfos");
    }

    @Override
    public boolean hasBroadcastVariable(String name) {
        return false;
    }

    @Override
    public <RT> List<RT> getBroadcastVariable(String name) {
        throw unsupported("broadcast variables");
    }

    @Override
    public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
        throw unsupported("broadcast variables");
    }

    @Override
    public DistributedCache getDistributedCache() {
        throw unsupported("distributed cache");
    }

    @Override
    public JobInfo getJobInfo() {
        return jobInfo;
    }

    @Override
    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    private static UnsupportedOperationException unsupported(String what) {
        return new UnsupportedOperationException(what + " not supported by flink-harness standalone runtime");
    }

    private static UnsupportedOperationException permanentlyOutOfScope(String what) {
        return new UnsupportedOperationException(
                what + " is permanently out of scope in flink-harness — use metrics (getMetricGroup()) instead");
    }
}