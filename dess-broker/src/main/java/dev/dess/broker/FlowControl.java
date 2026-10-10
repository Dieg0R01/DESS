package dev.dess.broker;

import dev.dess.common.logging.Logging;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** One permit per accepted message that still has a subscriber to be written. */
final class FlowControl {

    private final Semaphore permits;

    private final int limit;

    private final AtomicInteger peak = new AtomicInteger();

    FlowControl(int limit) {
        this.limit = limit;
        this.permits = new Semaphore(limit);
    }

    boolean tryAcquireNow() {
        if (!permits.tryAcquire()) {
            return false;
        }
        notePeak();
        return true;
    }

    boolean acquireUntil(long deadlineNanos) throws InterruptedException {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0 || !permits.tryAcquire(remaining, TimeUnit.NANOSECONDS)) {
            return false;
        }
        notePeak();
        return true;
    }

    void release() {
        permits.release();
    }

    int inFlight() {
        return Math.max(0, limit - permits.availablePermits());
    }

    int inFlightPeak() {
        return peak.get();
    }

    void throttled(long waitedNanos, boolean accepted) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("inFlight", Integer.toString(inFlight()));
        properties.put("limit", Integer.toString(limit));
        properties.put("waitedMs", Long.toString(TimeUnit.NANOSECONDS.toMillis(waitedNanos)));
        properties.put("outcome", accepted ? "ACCEPTED" : "BUSY");
        Logging.event("dev.dess.broker", "flow.throttled", properties);
    }

    private void notePeak() {
        peak.accumulateAndGet(inFlight(), Math::max);
    }
}
