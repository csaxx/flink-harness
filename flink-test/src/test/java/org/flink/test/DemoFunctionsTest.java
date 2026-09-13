package org.flink.test;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.flink.harness.graph.StreamEdge;
import org.flink.harness.Mode;
import org.flink.harness.StandaloneWorkflow;
import org.flink.harness.WorkflowBuilder;
import org.flink.harness.graph.result.WorkflowResult;
import org.flink.harness.graph.function.rich.KeyedProcessFunctionHarness;
import org.flink.harness.graph.function.single.MapFunctionHarness;
import org.flink.harness.graph.function.rich.ProcessFunctionHarness;
import org.flink.harness.graph.function.rich.RichMapFunctionHarness;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DemoFunctionsTest {

    // --------------------------------------------------------------------------------------------
    // single-function tests (harness constructors no longer take type hint strings)
    // --------------------------------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void parseFnParsesCsv() {
        RichMapFunctionHarness harness = new RichMapFunctionHarness("parse", new DemoFunctions.ParseFn());
        harness.open();
        DemoFunctions.ParsedOrder order = (DemoFunctions.ParsedOrder)
                ((List<Object>) harness.processElement("cust1,item,3,10.0", null).outputs()).get(0);
        assertThat(order.customer()).isEqualTo("cust1");
        assertThat(order.product()).isEqualTo("item");
        assertThat(order.quantity()).isEqualTo(3);
        assertThat(order.price()).isEqualTo(10.0);
        assertThat(order.total()).isEqualTo(30.0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void routeFnRoutesGoodOrders() {
        ProcessFunctionHarness harness = new ProcessFunctionHarness("route", new DemoFunctions.RouteFn());
        DemoFunctions.ParsedOrder good = new DemoFunctions.ParsedOrder("c1", "p1", 2, 5.0);
        var result = harness.processElement(good, null);
        assertThat((List<Object>) result.outputs()).containsExactly(good);
        assertThat(result.sideOutputs()).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void routeFnRejectsBadOrders() {
        ProcessFunctionHarness harness = new ProcessFunctionHarness("route", new DemoFunctions.RouteFn());
        DemoFunctions.ParsedOrder bad = new DemoFunctions.ParsedOrder("c1", "p1", 0, 5.0);
        var result = harness.processElement(bad, null);
        assertThat((List<Object>) result.outputs()).isEmpty();
        assertThat((List<Object>) result.sideOutputs().get(DemoFunctions.REJECTED_TAG)).containsExactly(bad);
    }

    @Test
    @SuppressWarnings("unchecked")
    void accumulateFnAccumulatesPerCustomer() {
        KeyedProcessFunctionHarness harness = new KeyedProcessFunctionHarness("accum", new DemoFunctions.AccumulateFn());
        KeySelector<DemoFunctions.ParsedOrder, String> byCustomer = DemoFunctions.ParsedOrder::customer;
        StreamEdge edge = new StreamEdge("src", "accum", byCustomer);

        DemoFunctions.ParsedOrder a = new DemoFunctions.ParsedOrder("c1", "p1", 2, 10.0);
        DemoFunctions.ParsedOrder b = new DemoFunctions.ParsedOrder("c1", "p2", 1, 20.0);
        DemoFunctions.ParsedOrder c = new DemoFunctions.ParsedOrder("c2", "p3", 3, 5.0);

        var r1 = harness.processElement(a, edge);
        var r2 = harness.processElement(b, edge);
        var r3 = harness.processElement(c, edge);

        assertThat((List<Object>) r1.outputs()).containsExactly("customer=c1 orders=1 total=20.00");
        assertThat((List<Object>) r2.outputs()).containsExactly("customer=c1 orders=2 total=40.00");
        assertThat((List<Object>) r3.outputs()).containsExactly("customer=c2 orders=1 total=15.00");
    }

    @Test
    @SuppressWarnings("unchecked")
    void reportFnFormatsLine() {
        ProcessFunctionHarness harness = new ProcessFunctionHarness("report", new DemoFunctions.ReportFn());
        DemoFunctions.ParsedOrder order = new DemoFunctions.ParsedOrder("c1", "gadget", 3, 4.5);
        var result = harness.processElement(order, null);
        assertThat((List<Object>) result.outputs()).containsExactly("product=gadget qty=3 total=13.50");
    }

    /** The demo's non-rich function runs through the plain MapFunctionHarness. */
    @Test
    @SuppressWarnings("unchecked")
    void shoutFnAnnouncesOrders() {
        MapFunctionHarness harness = new MapFunctionHarness("shout", new DemoFunctions.ShoutFn());
        DemoFunctions.ParsedOrder order = new DemoFunctions.ParsedOrder("c1", "gadget", 3, 4.5);
        var result = harness.processElement(order, null);
        assertThat((List<Object>) result.outputs()).containsExactly("ORDER GADGET for c1");
    }

    // --------------------------------------------------------------------------------------------
    // integration test: full workflow with sources and sinks
    // --------------------------------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void fullWorkflowWithFanOut() {
        StandaloneWorkflow wf = new WorkflowBuilder(Mode.CONTINUOUS)
                .addSource("csv", TypeInformation.of(String.class))
                .registerFunction("parse", new DemoFunctions.ParseFn())
                .registerFunction("route", new DemoFunctions.RouteFn())
                .registerFunction("accum", new DemoFunctions.AccumulateFn())
                .registerFunction("report", new DemoFunctions.ReportFn())
                .registerFunction("shout", new DemoFunctions.ShoutFn())
                .addSink("accumOut")
                .addSink("reportOut")
                .addSink("shoutOut")
                .addSink("rejectedOut")
                .addSourceEdge("csv", "parse")
                .addEdge("parse", "route")
                .addKeyedEdge("route", "accum", DemoFunctions.ParsedOrder::customer)
                .addEdge("route", "report")
                .addEdge("route", "shout")
                .addEdge("accum", "accumOut")
                .addEdge("report", "reportOut")
                .addEdge("shout", "shoutOut")
                .addSinkEdge("route", "rejectedOut", DemoFunctions.REJECTED_TAG)
                .build(true);

        WorkflowResult result = wf.process(
                List.of("Alice,book,2,12.50", "Bob,pen,0,2.00", "Alice,notebook,1,8.00"),
                "csv");

        // accumulated orders appear in the sink for accum
        List<?> accumOut = result.outputsOf("accumOut");
        assertThat((List<Object>) accumOut).containsExactly(
                "customer=Alice orders=1 total=25.00",
                "customer=Alice orders=2 total=33.00");

        // report output appears in the report sink
        List<?> reportOut = result.outputsOf("reportOut");
        assertThat((List<Object>) reportOut).containsExactly(
                "product=book qty=2 total=25.00",
                "product=notebook qty=1 total=8.00");

        // non-rich map branch announces the good orders
        List<?> shoutOut = result.outputsOf("shoutOut");
        assertThat((List<Object>) shoutOut).containsExactly(
                "ORDER BOOK for Alice",
                "ORDER NOTEBOOK for Alice");

        // rejected side output routed to its own sink
        List<?> rejected = result.outputsOf("rejectedOut");
        assertThat((List<Object>) rejected).hasSize(1);
        DemoFunctions.ParsedOrder o = (DemoFunctions.ParsedOrder) ((List<Object>) rejected).get(0);
        assertThat(o.customer()).isEqualTo("Bob");
        assertThat(o.product()).isEqualTo("pen");
        assertThat(o.quantity()).isEqualTo(0);
    }
}