package org.flink.harness.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;

/**
 * {@link OperatorMetricGroup} backed by {@link StandaloneMetricGroup} plus a stubbed IO group,
 * satisfying {@code RuntimeContext.getMetricGroup()}.
 */
public class StandaloneOperatorMetricGroup extends StandaloneMetricGroup implements OperatorMetricGroup {

    private final OperatorIOMetricGroup ioMetricGroup = new StandaloneIOMetricGroup();

    public StandaloneOperatorMetricGroup(String path) {
        super(path);
    }

    /** Placeholder IO group required by OperatorMetricGroup: never updated and kept out of the
     * parent group's metrics/children, so its counters never appear in snapshots. */
    @Override
    public OperatorIOMetricGroup getIOMetricGroup() {
        return ioMetricGroup;
    }

    // --------------------------------------------------------------------------------------------

    private static final class StandaloneIOMetricGroup extends StandaloneMetricGroup implements OperatorIOMetricGroup {
        private final Counter recordsIn = new IoCounter();
        private final Counter recordsOut = new IoCounter();
        private final Counter bytesIn = new IoCounter();
        private final Counter bytesOut = new IoCounter();

        StandaloneIOMetricGroup() {
            super("io");
        }

        @Override
        public Counter getNumRecordsInCounter() {
            return recordsIn;
        }

        @Override
        public Counter getNumRecordsOutCounter() {
            return recordsOut;
        }

        @Override
        public Counter getNumBytesInCounter() {
            return bytesIn;
        }

        @Override
        public Counter getNumBytesOutCounter() {
            return bytesOut;
        }

        private static final class IoCounter implements Counter {
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
        }
    }
}