package org.flink.harness.metrics;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Minimal metric registry. Metrics registered by functions are kept in a flat map;
 * child groups are supported recursively. Snapshots flatten nested groups with
 * dotted names (e.g. "errors.count").
 */
public class StandaloneMetricGroup implements MetricGroup {

    private final String path;
    private final Map<String, Metric> metrics = new LinkedHashMap<>();
    private final Map<String, StandaloneMetricGroup> children = new LinkedHashMap<>();

    public StandaloneMetricGroup(String path) {
        this.path = path;
    }

    /** Returns the existing metric under this name as a Counter, creating a StandaloneCounter on
     * first use. The cast is a trap: registering a gauge under the same name throws here. */
    @Override
    public Counter counter(String name) {
        Counter existing = (Counter) metrics.get(name);
        if (existing != null) {
            return existing;
        }
        Counter created = new StandaloneCounter();
        metrics.put(name, created);
        return created;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <C extends Counter> C counter(String name, C counter) {
        metrics.put(name, counter);
        return counter;
    }

    @Override
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
        metrics.put(name, gauge);
        return gauge;
    }

    @Override
    public <H extends Histogram> H histogram(String name, H histogram) {
        metrics.put(name, histogram);
        return histogram;
    }

    @Override
    public <M extends Meter> M meter(String name, M meter) {
        metrics.put(name, meter);
        return meter;
    }

    @Override
    public MetricGroup addGroup(String name) {
        return children.computeIfAbsent(name, n -> new StandaloneMetricGroup(path + "." + n));
    }

    @Override
    public MetricGroup addGroup(String key, String value) {
        StandaloneMetricGroup first = children.computeIfAbsent(key, n -> new StandaloneMetricGroup(path + "." + n));
        return first.children.computeIfAbsent(value, n -> new StandaloneMetricGroup(path + "." + key + "." + n));
    }

    @Override
    public String[] getScopeComponents() {
        return path.split("\\.");
    }

    @Override
    public Map<String, String> getAllVariables() {
        return Map.of();
    }

    @Override
    public String getMetricIdentifier(String metricName) {
        return path + "." + metricName;
    }

    @Override
    public String getMetricIdentifier(String metricName, CharacterFilter filter) {
        String scope = filter == null ? path : filter.filterCharacters(path);
        return scope + "." + metricName;
    }

    /** Snapshot all metrics (recursively) with flattened dotted keys. */
    public Map<String, Object> snapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        collect(out, "");
        return out;
    }

    /** Accessor returning the registered {@link Metric} instances with dotted names
     * (kind preserved — needed for correct cross-function aggregation). */
    public Map<String, Metric> metricInstances() {
        Map<String, Metric> out = new LinkedHashMap<>();
        collectInstances(out, "");
        return out;
    }

    private void collectInstances(Map<String, Metric> out, String prefix) {
        for (Map.Entry<String, Metric> entry : metrics.entrySet()) {
            out.put(prefix + entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, StandaloneMetricGroup> child : children.entrySet()) {
            child.getValue().collectInstances(out, prefix + child.getKey() + ".");
        }
    }

    private void collect(Map<String, Object> out, String prefix) {
        for (Map.Entry<String, Metric> entry : metrics.entrySet()) {
            out.put(prefix + entry.getKey(), extract(entry.getValue()));
        }
        for (Map.Entry<String, StandaloneMetricGroup> child : children.entrySet()) {
            child.getValue().collect(out, prefix + child.getKey() + ".");
        }
    }

    /** Recursively zeroes only StandaloneCounter instances: custom counters registered via
     * {@code counter(name, custom)} as well as gauges/meters/histograms are intentionally untouched. */
    public void resetCounters() {
        for (Metric metric : metrics.values()) {
            if (metric instanceof StandaloneCounter counter) {
                counter.set(0);
            }
        }
        for (StandaloneMetricGroup child : children.values()) {
            child.resetCounters();
        }
    }

    // snapshot values: meters and histograms collapse to their counts (no percentiles)
    private static Object extract(Metric metric) {
        if (metric instanceof Counter counter) {
            return counter.getCount();
        }
        if (metric instanceof Gauge<?> gauge) {
            return gauge.getValue();
        }
        if (metric instanceof Meter meter) {
            return meter.getCount();
        }
        if (metric instanceof Histogram histogram) {
            return histogram.getCount();
        }
        return metric.toString();
    }

    /** Simple {@link Counter} implementation, package-private. */
    static final class StandaloneCounter implements Counter {
        private long count;

        @Override
        public void inc() {
            count++;
        }

        @Override
        public void inc(long n) {
            count += n;
        }

        @Override
        public void dec() {
            count--;
        }

        @Override
        public void dec(long n) {
            count -= n;
        }

        @Override
        public long getCount() {
            return count;
        }

        void set(long value) {
            count = value;
        }
    }
}