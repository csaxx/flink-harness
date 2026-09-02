package org.flink.harness;

import org.apache.flink.metrics.Counter;
import org.flink.harness.runtime.StandaloneMetricGroup;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MetricGroupTest {

    @Test
    void countersAccumulateAndSnapshot() {
        StandaloneMetricGroup group = new StandaloneMetricGroup("fn");
        Counter count = group.counter("elements");
        count.inc();
        count.inc();
        Map<String, Object> snapshot = group.snapshot();
        assertThat(snapshot).containsEntry("elements", 2L);
    }

    @Test
    void nestedGroupsFlatten() {
        StandaloneMetricGroup group = new StandaloneMetricGroup("fn");
        Counter count = group.addGroup("errors").counter("count");
        count.inc();
        Map<String, Object> snapshot = group.snapshot();
        assertThat(snapshot).containsEntry("errors.count", 1L);
    }

    @Test
    void resetCountersZeroesCountersRecursively() {
        StandaloneMetricGroup group = new StandaloneMetricGroup("fn");
        Counter a = group.counter("a");
        Counter b = group.addGroup("g").counter("b");
        a.inc();
        b.inc();
        group.resetCounters();
        assertThat(a.getCount()).isZero();
        assertThat(b.getCount()).isZero();
        assertThat(group.snapshot()).containsEntry("a", 0L).containsEntry("g.b", 0L);
    }

    @Test
    void gaugeValueExposedInSnapshot() {
        StandaloneMetricGroup group = new StandaloneMetricGroup("fn");
        group.gauge("current", new org.apache.flink.metrics.Gauge<String>() {
            @Override
            public String getValue() {
                return "hello";
            }
        });
        assertThat(group.snapshot()).containsEntry("current", "hello");
    }
}