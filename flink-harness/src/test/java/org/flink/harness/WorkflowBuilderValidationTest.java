package org.flink.harness;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowBuilderValidationTest {

    @Test
    void duplicateIdRejected() {
        WorkflowBuilder b = new WorkflowBuilder(Mode.CONTINUOUS);
        b.registerFunction("a", new Passive());
        assertThatThrownBy(() -> b.registerFunction("a", new Passive()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unknownEdgeRejected() {
        WorkflowBuilder b = new WorkflowBuilder(Mode.CONTINUOUS);
        b.registerFunction("a", new Passive());
        b.addEdge("a", "nope");
        assertThatThrownBy(b::build).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void typeMismatchRejected() {
        WorkflowBuilder b = new WorkflowBuilder(Mode.CONTINUOUS);
        b.registerFunction("a", new Passive(), TypeInformation.of(String.class), TypeInformation.of(String.class));
        b.registerFunction("b", new KeyedFn(), TypeInformation.of(Integer.class), TypeInformation.of(Integer.class));
        b.addKeyedEdge("a", "b", v -> v);
        assertThatThrownBy(b::build).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void unkeyedInboundEdgeToKeyedFunctionRejected() {
        WorkflowBuilder b = new WorkflowBuilder(Mode.CONTINUOUS);
        b.registerFunction("a", new Passive());
        b.registerKeyedFunction("b", new KeyedFn());
        b.addEdge("a", "b");
        // unresolved types → would also fail; but keyed-check should trip with optOut=true
        assertThatThrownBy(() -> b.build(true)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unkeyed edge");
    }

    @Test
    void unresolvedGenericsFailAtBuildAndOptOutPasses() {
        WorkflowBuilder strict = new WorkflowBuilder(Mode.CONTINUOUS);
        strict.registerFunction("a", new Passive());
        strict.registerFunction("b", new Passive());
        strict.addEdge("a", "b");
        assertThatThrownBy(strict::build).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unresolved edge type");

        // with opt-out, build succeeds
        WorkflowBuilder lax = new WorkflowBuilder(Mode.CONTINUOUS);
        lax.registerFunction("a", new Passive());
        lax.registerFunction("b", new Passive());
        lax.addEdge("a", "b");
        StandaloneWorkflow wf = lax.build(true);
        assertThat(wf.getWorkflow()).hasSize(2);
        assertThat(wf.getWorkflow().get(0).inputType()).isEqualTo(WorkflowNode.UNKNOWN_TYPE);
    }

    @Test
    void initializeAtBuildOpensFunctionsEagerly() {
        WorkflowBuilder b = new WorkflowBuilder(Mode.CONTINUOUS);
        b.initializeAtBuild()
         .registerKeyedFunction("k", new KeyedFn());
        // eager init sets a dummy key for the keyed function at build() time
        StandaloneWorkflow wf = b.build(true);
        assertThat(wf.getWorkflow()).hasSize(1);
        wf.close(); // no exception → open succeeded
    }

    @Test
    void initializeAtBuildSurfacesOpenFailuresAtBuildTime() {
        FailingOpen fn = new FailingOpen();
        WorkflowBuilder b = new WorkflowBuilder(Mode.CONTINUOUS);
        b.initializeAtBuild().registerFunction("bad", fn);
        assertThatThrownBy(b::build).isInstanceOf(RuntimeException.class)
                .hasMessageContaining("open() failed for bad");
    }

    private static final class FailingOpen extends ProcessFunction<Object, Object> {
        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext context) {
            throw new RuntimeException("expected failure");
        }

        @Override
        public void processElement(Object value, Context ctx, Collector<Object> out) {
            out.collect(value);
        }
    }

    private static final class Passive extends ProcessFunction<Object, Object> {
        @Override
        public void processElement(Object value, Context ctx, Collector<Object> out) {
            out.collect(value);
        }
    }

    private static final class KeyedFn extends KeyedProcessFunction<Object, Object, Object> {
        @Override
        public void processElement(Object value, Context ctx, Collector<Object> out) {
            out.collect(value);
        }
    }
}