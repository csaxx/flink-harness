package org.flink.harness.timer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TimerHeapTest {

    private final TimerHeap heap = new TimerHeap();

    @Test
    void registerAndPollReturnsDueEntries() {
        heap.tryRegister("keyA", 100);
        heap.tryRegister("keyA", 200);
        heap.tryRegister("keyB", 150);

        assertThat(heap.size()).isEqualTo(3);

        TimerHeap.TimerEntry e1 = heap.pollDue(150);
        assertThat(e1).isNotNull();
        assertThat(e1.timestamp()).isEqualTo(100);
        assertThat(e1.key()).isEqualTo("keyA");

        TimerHeap.TimerEntry e2 = heap.pollDue(150);
        assertThat(e2).isNotNull();
        assertThat(e2.timestamp()).isEqualTo(150);
        assertThat(e2.key()).isEqualTo("keyB");

        TimerHeap.TimerEntry e3 = heap.pollDue(150);
        assertThat(e3).isNull();

        TimerHeap.TimerEntry e4 = heap.pollDue(200);
        assertThat(e4).isNotNull();
        assertThat(e4.timestamp()).isEqualTo(200);
        assertThat(e4.key()).isEqualTo("keyA");

        assertThat(heap.isEmpty()).isTrue();
    }

    @Test
    void duplicateRegistrationIsNoOp() {
        assertThat(heap.tryRegister("k", 100)).isTrue();
        assertThat(heap.tryRegister("k", 100)).isFalse();
        assertThat(heap.size()).isEqualTo(1);
    }

    @Test
    void deleteRemovesTimer() {
        heap.tryRegister("k", 100);
        heap.tryRegister("k", 200);

        assertThat(heap.tryDelete("k", 100)).isTrue();
        assertThat(heap.size()).isEqualTo(1);

        TimerHeap.TimerEntry e = heap.pollDue(200);
        assertThat(e).isNotNull();
        assertThat(e.timestamp()).isEqualTo(200);
    }

    @Test
    void deleteNonExistentIsSilentNoOp() {
        heap.tryRegister("k", 100);
        assertThat(heap.tryDelete("k", 999)).isFalse();
        assertThat(heap.size()).isEqualTo(1);
    }

    @Test
    void deleteAndReRegisterWorks() {
        heap.tryRegister("k", 100);
        heap.tryDelete("k", 100);
        assertThat(heap.tryRegister("k", 100)).isTrue();
        assertThat(heap.size()).isEqualTo(1);
    }

    @Test
    void pollDueSkipsCancelledEntries() {
        heap.tryRegister("k1", 100);
        heap.tryRegister("k2", 100);
        heap.tryDelete("k1", 100);

        TimerHeap.TimerEntry e = heap.pollDue(100);
        assertThat(e).isNotNull();
        assertThat(e.key()).isEqualTo("k2");
        assertThat(heap.pollDue(100)).isNull();
    }

    @Test
    void clearRemovesAllTimers() {
        heap.tryRegister("a", 10);
        heap.tryRegister("b", 20);
        heap.clear();
        assertThat(heap.isEmpty()).isTrue();
        assertThat(heap.pollDue(100)).isNull();
    }

    @Test
    void peekDueReturnsEarliestWithoutRemoving() {
        heap.tryRegister("k", 100);
        heap.tryRegister("k", 200);

        TimerHeap.TimerEntry e = heap.peekDue(150);
        assertThat(e).isNotNull();
        assertThat(e.timestamp()).isEqualTo(100);
        assertThat(heap.size()).isEqualTo(2);
    }

    @Test
    void peekDueReturnsNullWhenNothingDue() {
        heap.tryRegister("k", 200);
        assertThat(heap.peekDue(100)).isNull();
    }

    @Test
    void earliestTimestampReturnsMinOrMax() {
        assertThat(heap.earliestTimestamp()).isEqualTo(Long.MAX_VALUE);
        heap.tryRegister("k", 300);
        assertThat(heap.earliestTimestamp()).isEqualTo(300);
        heap.tryRegister("k", 100);
        assertThat(heap.earliestTimestamp()).isEqualTo(100);
    }

    @Test
    void timersOrderedByTimestampThenInsertionOrder() {
        heap.tryRegister("k", 100);
        heap.tryRegister("k", 100);
        heap.tryDelete("k", 100);
        heap.tryRegister("k", 100);

        assertThat(heap.size()).isEqualTo(1);
        TimerHeap.TimerEntry e = heap.pollDue(100);
        assertThat(e).isNotNull();
        assertThat(e.timestamp()).isEqualTo(100);
        assertThat(e.key()).isEqualTo("k");
    }
}