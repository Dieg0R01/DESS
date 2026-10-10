package dev.dess.broker;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Subscribers still expected to acknowledge one accepted offset.
 * {@link #expected}, {@link #received} and the {@link #done} signal are guarded by {@link #lock}.
 */
final class PendingConfirmation {

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition done = lock.newCondition();

    /** Connection ids that have not acknowledged this offset. */
    private final Set<Long> expected;

    private final int expectedCount;

    private final long deadlineNanos;

    private int received;

    PendingConfirmation(Set<Long> subscribers, long deadlineNanos) {
        this.expected = new HashSet<>(subscribers);
        this.expectedCount = expected.size();
        this.deadlineNanos = deadlineNanos;
    }

    void acknowledge(long clientId) {
        drop(clientId, true);
    }

    void remove(long clientId) {
        drop(clientId, false);
    }

    Outcome await() throws InterruptedException {
        lock.lock();
        try {
            while (!expected.isEmpty()) {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    return new Outcome(false, expectedCount, received);
                }
                done.awaitNanos(remaining);
            }
            return new Outcome(true, expectedCount, received);
        } finally {
            lock.unlock();
        }
    }

    private void drop(long clientId, boolean count) {
        lock.lock();
        try {
            if (!expected.remove(clientId)) {
                return;
            }
            if (count) {
                received++;
            }
            if (expected.isEmpty()) {
                done.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    record Outcome(boolean complete, int expectedCount, int received) {
    }
}
