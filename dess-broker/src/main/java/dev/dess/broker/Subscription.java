package dev.dess.broker;

import java.util.concurrent.atomic.AtomicBoolean;

final class Subscription {

    private final ClientConnection connection;

    private final Topic topic;

    private final long subscribedAt;

    /** Offsets at or after this value were counted in the ring when they were accepted. */
    private final long countedFrom;

    /** Guarded by the topic lock. */
    private long cursor;

    /** Guarded by the topic lock. Offsets below this have been handed to the writer. */
    private long handedOff;

    /** Guarded by the topic lock. */
    private boolean active = true;

    /** Guarded by the topic lock. Zero while the outbound window is below capacity. */
    private long fullSince;

    /** True while this subscription is queued for its writer or being written. */
    private final AtomicBoolean scheduled = new AtomicBoolean();

    Subscription(ClientConnection connection, Topic topic, long subscribedAt, long countedFrom) {
        this.connection = connection;
        this.topic = topic;
        this.subscribedAt = subscribedAt;
        this.countedFrom = countedFrom;
        this.cursor = subscribedAt;
        this.handedOff = subscribedAt;
    }

    ClientConnection connection() {
        return connection;
    }

    Topic topic() {
        return topic;
    }

    long subscribedAt() {
        return subscribedAt;
    }

    long countedFrom() {
        return countedFrom;
    }

    long cursor() {
        return cursor;
    }

    void advanceCursor() {
        cursor++;
    }

    long handedOff() {
        return handedOff;
    }

    void handOffNext() {
        handedOff = cursor + 1;
    }

    boolean isActive() {
        return active;
    }

    void deactivate() {
        active = false;
    }

    boolean markFull(long now) {
        if (fullSince != 0) {
            return false;
        }
        fullSince = now == 0L ? 1L : now;
        return true;
    }

    void clearFull() {
        fullSince = 0L;
    }

    long fullSince() {
        return fullSince;
    }

    boolean trySchedule() {
        return scheduled.compareAndSet(false, true);
    }

    void clearScheduled() {
        scheduled.set(false);
    }
}
