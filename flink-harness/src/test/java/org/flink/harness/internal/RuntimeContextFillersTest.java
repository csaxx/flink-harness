package org.flink.harness.internal;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.flink.harness.graph.StandaloneRuntimeContext;
import org.junit.jupiter.api.Test;
import org.flink.harness.WorkflowBuilder;
import org.flink.harness.Mode;
import org.flink.harness.StandaloneWorkflow;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RuntimeContextFillersTest {

    // ------------------------------------------------------------------------
    // globalJobParameters — defaults
    // ------------------------------------------------------------------------

    @Test
    void defaultGlobalJobParametersIsEmpty() {
        StandaloneRuntimeContext ctx = new StandaloneRuntimeContext("test");
        assertThat(ctx.getGlobalJobParameters()).isEmpty();
    }

    @Test
    void defaultGlobalJobParametersIsImmutable() {
        StandaloneRuntimeContext ctx = new StandaloneRuntimeContext("test");
        assertThatThrownBy(() -> ctx.getGlobalJobParameters().put("k", "v"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // ------------------------------------------------------------------------
    // globalJobParameters — setter
    // ------------------------------------------------------------------------

    @Test
    void setGlobalJobParametersMakesCopy() {
        StandaloneRuntimeContext ctx = new StandaloneRuntimeContext("test");
        Map<String, String> mutable = new HashMap<>();
        mutable.put("key1", "val1");
        ctx.setGlobalJobParameters(mutable);
        assertThat(ctx.getGlobalJobParameters()).containsEntry("key1", "val1");
        // mutation after set does not affect stored copy
        mutable.put("key1", "mutated");
        assertThat(ctx.getGlobalJobParameters()).containsEntry("key1", "val1");
    }

    @Test
    void setGlobalJobParametersRejectsNull() {
        StandaloneRuntimeContext ctx = new StandaloneRuntimeContext("test");
        assertThatThrownBy(() -> ctx.setGlobalJobParameters(null))
                .isInstanceOf(NullPointerException.class);
    }

    // ------------------------------------------------------------------------
    // globalJobParameters — wired via WorkflowBuilder
    // ------------------------------------------------------------------------

    @Test
    void builderWiresGlobalJobParameters_toRichMap() {
        Map<String, String> params = Map.of("env", "test", "retries", "3");
        WorkflowBuilder builder = new WorkflowBuilder(Mode.TRANSIENT)
                .globalJobParameters(params)
                .addSource("src", BasicTypeInfo.STRING_TYPE_INFO)
                .registerFunction("map", new ParamsReaderMap(),
                        BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
                .addEdge("src", "map");

        standaloneWorkflow(builder, "src", "input");
        assertThat(ParamsReaderMap.LAST_PARAMS).containsEntry("env", "test");
        assertThat(ParamsReaderMap.LAST_PARAMS).containsEntry("retries", "3");
    }

    @Test
    void builderDefaultParamsIsEmpty() {
        WorkflowBuilder builder = new WorkflowBuilder(Mode.TRANSIENT)
                .addSource("src", BasicTypeInfo.STRING_TYPE_INFO)
                .registerFunction("map", new ParamsReaderMap(),
                        BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
                .addEdge("src", "map");

        standaloneWorkflow(builder, "src", "x");
        assertThat(ParamsReaderMap.LAST_PARAMS).isEmpty();
    }

    @Test
    void builderParamsAreImmutable() {
        Map<String, String> mutable = new HashMap<>(Map.of("k", "v"));
        new WorkflowBuilder(Mode.TRANSIENT).globalJobParameters(mutable);
        mutable.put("k", "changed");
    }

    @Test
    void builderRejectsNullGlobalJobParameters() {
        assertThatThrownBy(() -> new WorkflowBuilder(Mode.TRANSIENT).globalJobParameters(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("globalJobParameters must not be null");
    }

    // ------------------------------------------------------------------------
    // globalJobParameters — TRANSIENT survival
    // ------------------------------------------------------------------------

    @Test
    void paramsSurviveTransientReset() {
        Map<String, String> params = Map.of("mode", "transient-test");
        ParamsReaderMap fn = new ParamsReaderMap();
        WorkflowBuilder builder = new WorkflowBuilder(Mode.TRANSIENT)
                .globalJobParameters(params)
                .addSource("src", BasicTypeInfo.STRING_TYPE_INFO)
                .registerFunction("map", fn,
                        BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
                .addEdge("src", "map");

        standaloneWorkflow(builder, "src", "first");
        assertThat(ParamsReaderMap.LAST_PARAMS).containsEntry("mode", "transient-test");

        standaloneWorkflow(builder, "src", "second");
        assertThat(ParamsReaderMap.LAST_PARAMS).containsEntry("mode", "transient-test");
    }

    // ------------------------------------------------------------------------
    // createSerializer
    // ------------------------------------------------------------------------

    @Test
    void createSerializer_stringRoundTrip() throws Exception {
        StandaloneRuntimeContext ctx = new StandaloneRuntimeContext("test");
        TypeSerializer<String> ser = ctx.createSerializer(BasicTypeInfo.STRING_TYPE_INFO);

        DataOutputSerializer out = new DataOutputSerializer(32);
        ser.serialize("hello harness", out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        assertThat(ser.deserialize(in)).isEqualTo("hello harness");
    }

    @Test
    void createSerializer_integerRoundTrip() throws Exception {
        StandaloneRuntimeContext ctx = new StandaloneRuntimeContext("test");
        TypeSerializer<Integer> ser = ctx.createSerializer(BasicTypeInfo.INT_TYPE_INFO);

        DataOutputSerializer out = new DataOutputSerializer(8);
        ser.serialize(42, out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        assertThat(ser.deserialize(in)).isEqualTo(42);
    }

    @Test
    void createSerializer_tupleRoundTrip() throws Exception {
        StandaloneRuntimeContext ctx = new StandaloneRuntimeContext("test");
        TypeInformation<Tuple2<String, Integer>> tupleType =
                new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
        TypeSerializer<Tuple2<String, Integer>> ser = ctx.createSerializer(tupleType);

        DataOutputSerializer out = new DataOutputSerializer(64);
        ser.serialize(new Tuple2<>("key", 7), out);
        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        assertThat(ser.deserialize(in)).isEqualTo(new Tuple2<>("key", 7));
    }

    // ------------------------------------------------------------------------
    // accumulators — permanently out of scope, still UOE
    // ------------------------------------------------------------------------

    @Test
    void addAccumulatorThrows() {
        StandaloneRuntimeContext ctx = new StandaloneRuntimeContext("test");
        assertThatThrownBy(() -> ctx.addAccumulator("a", new DummyAccumulator()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("permanently out of scope");
    }

    @Test
    void getAccumulatorThrows() {
        StandaloneRuntimeContext ctx = new StandaloneRuntimeContext("test");
        assertThatThrownBy(() -> ctx.getAccumulator("a"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("permanently out of scope");
    }

    @Test
    void getIntCounterThrows() {
        StandaloneRuntimeContext ctx = new StandaloneRuntimeContext("test");
        assertThatThrownBy(() -> ctx.getIntCounter("c"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("permanently out of scope");
    }

    @Test
    void getLongCounterThrows() {
        StandaloneRuntimeContext ctx = new StandaloneRuntimeContext("test");
        assertThatThrownBy(() -> ctx.getLongCounter("c"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("permanently out of scope");
    }

    @Test
    void getDoubleCounterThrows() {
        StandaloneRuntimeContext ctx = new StandaloneRuntimeContext("test");
        assertThatThrownBy(() -> ctx.getDoubleCounter("c"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("permanently out of scope");
    }

    @Test
    void getHistogramThrows() {
        StandaloneRuntimeContext ctx = new StandaloneRuntimeContext("test");
        assertThatThrownBy(() -> ctx.getHistogram("h"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("permanently out of scope");
    }

    // ------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------

    /** RichMapFunction that captures globalJobParameters on first map call. */
    public static class ParamsReaderMap
            extends org.apache.flink.api.common.functions.RichMapFunction<String, String> {
        static volatile Map<String, String> LAST_PARAMS = Map.of();

        @Override
        public String map(String value) {
            LAST_PARAMS = getRuntimeContext().getGlobalJobParameters();
            return value;
        }
    }

    /** Minimal accumulable for testing addAccumulator rejection. */
    private static class DummyAccumulator implements Accumulator<String, Serializable> {
        @Override
        public void add(String value) {}

        @Override
        public Serializable getLocalValue() {
            return null;
        }

        @Override
        public void resetLocal() {}

        @Override
        public void merge(Accumulator<String, Serializable> other) {}

        @Override
        public Accumulator<String, Serializable> clone() {
            return new DummyAccumulator();
        }
    }

    /** Run a single-element workflow and ignore the result. */
    private static void standaloneWorkflow(WorkflowBuilder builder, String sourceId, Object input) {
        StandaloneWorkflow wf = builder.build();
        try {
            wf.process(java.util.List.of(input), sourceId);
        } finally {
            wf.close();
        }
    }
}