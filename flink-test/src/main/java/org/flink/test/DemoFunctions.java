package org.flink.test;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Locale;
import java.util.Objects;

/**
 * Data model + function collection for the demo order-processing pipeline.
 *
 * <pre>
 *   parse (RichMapFunction) → route (ProcessFunction)  → [fan-out]
 *                                                         ├── aggregate (KeyedProcessFunction)
 *                                                         └── report (ProcessFunction)
 * </pre>
 *
 * Activated outputs:
 *   - aggregate (main)
 *   - report   (main)
 *   - route    (side "rejected")
 * </pre>
 */
public final class DemoFunctions {

    public static final OutputTag<ParsedOrder> REJECTED_TAG = new OutputTag<>("rejected") {};

    // --------------------------------------------------------------------------------------------
    // data model
    // --------------------------------------------------------------------------------------------

    /** A parsed order line. */
    public record ParsedOrder(String customer, String product, int quantity, double price) {

        /** Build from a CSV line {@code customer,product,quantity,price}. */
        public static ParsedOrder parse(String line) {
            String[] parts = line.split(",");
            if (parts.length < 4) {
                throw new IllegalArgumentException("invalid CSV: " + line);
            }
            return new ParsedOrder(
                    parts[0].trim(),
                    parts[1].trim(),
                    Integer.parseInt(parts[2].trim()),
                    Double.parseDouble(parts[3].trim()));
        }

        public double total() {
            return quantity * price;
        }
    }

    // --------------------------------------------------------------------------------------------
    // functions
    // --------------------------------------------------------------------------------------------

    /** Stage 1: parse raw lines into {@link ParsedOrder}. */
    public static final class ParseFn extends RichMapFunction<String, ParsedOrder> {

        private transient Counter parsed;

        @Override
        public void open(OpenContext ctx) {
            parsed = getRuntimeContext().getMetricGroup().counter("parsedCount");
        }

        @Override
        public ParsedOrder map(String value) {
            parsed.inc();
            return ParsedOrder.parse(value);
        }
    }

    /** Stage 2: validate orders; good orders pass through, bad ones go to side output {@code REJECTED}. */
    public static final class RouteFn extends ProcessFunction<ParsedOrder, ParsedOrder> {

        private transient Counter good;

        @Override
        public void open(OpenContext ctx) {
            good = getRuntimeContext().getMetricGroup().counter("good");
        }

        @Override
        public void processElement(ParsedOrder value, Context ctx, Collector<ParsedOrder> out) {
            if (value.quantity() <= 0) {
                ctx.output(REJECTED_TAG, value);
                return;
            }
            good.inc();
            out.collect(value);
        }
    }

    /** Stage 3a (keyed by customer): accumulate total spent and order count per customer. */
    public static final class AccumulateFn
            extends KeyedProcessFunction<String, ParsedOrder, String> {

        private transient ValueState<Long> orderCount;
        private transient ValueState<Double> totalSpent;
        private transient Counter aggregated;

        @Override
        public void open(OpenContext ctx) {
            orderCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("orderCount", Long.class, 0L));
            totalSpent = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("totalSpent", Double.class, 0.0));
            aggregated = getRuntimeContext().getMetricGroup().counter("aggregated");
        }

        @Override
        public void processElement(ParsedOrder value, Context ctx, Collector<String> out) throws Exception {
            double total = totalSpent.value();
            long count = orderCount.value();
            totalSpent.update(total + value.total());
            orderCount.update(count + 1);
            aggregated.inc();
            out.collect(String.format(Locale.US, "customer=%s orders=%d total=%.2f",
                    ctx.getCurrentKey(), count + 1, total + value.total()));
        }
    }

    /** Stage 3b: produce a simple human-readable report line per order. */
    public static final class ReportFn extends ProcessFunction<ParsedOrder, String> {

        private transient Counter reported;

        @Override
        public void open(OpenContext ctx) {
            reported = getRuntimeContext().getMetricGroup().counter("reported");
        }

        @Override
        public void processElement(ParsedOrder value, Context ctx, Collector<String> out) {
            reported.inc();
            out.collect(String.format(Locale.US, "product=%s qty=%d total=%.2f",
                    value.product(), value.quantity(), value.total()));
        }
    }
}