package org.flink.harness.timer;

import java.util.HashMap;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * Per-keyed-node processing-time timer store. Enforces one active timer per {@code (key, timestamp)}:
 * duplicate registrations are rejected, and deletes are lazy (the entry is flagged cancelled and
 * discarded from the queue head on the next peek/poll). Polling is therefore always cheap.
 */
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

    // timestamp first, insertion sequence as tie-breaker: deterministic firing order for equal timestamps
    private final PriorityQueue<TimerEntry> queue = new PriorityQueue<>((a, b) -> {
        int cmp = Long.compare(a.timestamp, b.timestamp);
        return cmp != 0 ? cmp : Long.compare(a.seq, b.seq);
    });

    private final HashMap<TimerKey, TimerEntry> active = new HashMap<>();
    private long counter;

    /** False if a timer for this exact (key, timestamp) is already active (dedup rule). */
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

    /** Flags the entry cancelled for lazy removal; returns false (silent no-op) if not active. */
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

    /** Removes and returns the earliest timer due at or before {@code now}, or null. */
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

    // drop cancelled entries sitting at the head so they cannot block due timers behind them
    private void clean() {
        while (!queue.isEmpty() && queue.peek().cancelled) {
            TimerEntry e = queue.poll();
            active.remove(new TimerKey(e.key, e.timestamp));
        }
    }
}