package org.flink.harness.timer;

import java.util.HashMap;
import java.util.Objects;
import java.util.PriorityQueue;

public final class TimerHeap {

    public static final class TimerEntry {
        final long timestamp;
        final long seq;
        final Object key;
        boolean cancelled;

        TimerEntry(long timestamp, long seq, Object key) {
            this.timestamp = timestamp;
            this.seq = seq;
            this.key = key;
        }

        public long timestamp() { return timestamp; }

        public Object key() { return key; }
    }

    private static final class TimerKey {
        final Object key;
        final long timestamp;

        TimerKey(Object key, long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TimerKey other)) return false;
            return timestamp == other.timestamp && Objects.equals(key, other.key);
        }

        @Override
        public int hashCode() {
            return 31 * Long.hashCode(timestamp) + (key != null ? key.hashCode() : 0);
        }
    }

    private final PriorityQueue<TimerEntry> queue = new PriorityQueue<>((a, b) -> {
        int cmp = Long.compare(a.timestamp, b.timestamp);
        return cmp != 0 ? cmp : Long.compare(a.seq, b.seq);
    });

    private final HashMap<TimerKey, TimerEntry> active = new HashMap<>();
    private long counter;

    public boolean tryRegister(Object key, long timestamp) {
        TimerKey k = new TimerKey(key, timestamp);
        if (active.containsKey(k)) {
            return false;
        }
        TimerEntry e = new TimerEntry(timestamp, counter++, key);
        queue.add(e);
        active.put(k, e);
        return true;
    }

    public boolean tryDelete(Object key, long timestamp) {
        TimerKey k = new TimerKey(key, timestamp);
        TimerEntry e = active.remove(k);
        if (e != null) {
            e.cancelled = true;
            return true;
        }
        return false;
    }

    public TimerEntry peekDue(long now) {
        clean();
        TimerEntry e = queue.peek();
        return (e != null && e.timestamp <= now) ? e : null;
    }

    public TimerEntry pollDue(long now) {
        clean();
        TimerEntry e = queue.peek();
        if (e != null && e.timestamp <= now) {
            queue.poll();
            active.remove(new TimerKey(e.key, e.timestamp));
            return e;
        }
        return null;
    }

    public void clear() {
        queue.clear();
        active.clear();
    }

    public int size() {
        return active.size();
    }

    public boolean isEmpty() {
        return active.isEmpty();
    }

    public long earliestTimestamp() {
        clean();
        TimerEntry e = queue.peek();
        return e != null ? e.timestamp : Long.MAX_VALUE;
    }

    private void clean() {
        while (!queue.isEmpty() && queue.peek().cancelled) {
            TimerEntry e = queue.poll();
            active.remove(new TimerKey(e.key, e.timestamp));
        }
    }
}