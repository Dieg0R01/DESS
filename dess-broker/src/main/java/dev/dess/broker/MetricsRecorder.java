package dev.dess.broker;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final class MetricsRecorder {

    static final int WINDOW_SECONDS = 10;

    private final LongAdder messagesIn = new LongAdder();

    private final LongAdder messagesOut = new LongAdder();

    private final long[] inboundWindow = new long[WINDOW_SECONDS];

    private final long[] outboundWindow = new long[WINDOW_SECONDS];

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition ticked = lock.newCondition();

    private final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();

    private final ThreadMXBean threads = ManagementFactory.getThreadMXBean();

    private int slot;

    private int ticks;

    void incrementIn() {
        messagesIn.increment();
    }

    void incrementOut() {
        messagesOut.increment();
    }

    void tick() {
        lock.lock();
        try {
            inboundWindow[slot] = messagesIn.sumThenReset();
            outboundWindow[slot] = messagesOut.sumThenReset();
            slot = (slot + 1) % WINDOW_SECONDS;
            ticks++;
            ticked.signalAll();
        } finally {
            lock.unlock();
        }
    }

    int ticks() {
        lock.lock();
        try {
            return ticks;
        } finally {
            lock.unlock();
        }
    }

    boolean awaitTicks(int count, Duration timeout) throws InterruptedException {
        lock.lock();
        try {
            long remaining = timeout.toNanos();
            while (ticks < count) {
                if (remaining <= 0) {
                    return false;
                }
                remaining = ticked.awaitNanos(remaining);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    Map<String, Object> snapshot(int activeConnections, int inFlight, long uptimeSeconds, List<TopicSummary> topics) {
        long inbound;
        long outbound;
        lock.lock();
        try {
            inbound = 0;
            outbound = 0;
            for (int index = 0; index < WINDOW_SECONDS; index++) {
                inbound += inboundWindow[index];
                outbound += outboundWindow[index];
            }
        } finally {
            lock.unlock();
        }
        MemoryUsage heap = memory.getHeapMemoryUsage();
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("takenAt", Instant.now().truncatedTo(ChronoUnit.SECONDS).toString());
        body.put("messagesInPerSecond", inbound / (double) WINDOW_SECONDS);
        body.put("messagesOutPerSecond", outbound / (double) WINDOW_SECONDS);
        body.put("activeConnections", (long) activeConnections);
        body.put("activeThreads", (long) threads.getThreadCount());
        body.put("heapUsedBytes", heap.getUsed());
        long heapMax = heap.getMax();
        body.put("heapMaxBytes", heapMax < 0 ? null : heapMax);
        body.put("inFlight", (long) inFlight);
        body.put("uptimeSeconds", uptimeSeconds);
        ArrayList<Map<String, Object>> topicItems = new ArrayList<>(topics.size());
        for (TopicSummary summary : topics) {
            LinkedHashMap<String, Object> item = new LinkedHashMap<>();
            item.put("name", summary.name());
            item.put("queueDepth", (long) summary.queueDepth());
            topicItems.add(item);
        }
        body.put("topics", topicItems);
        return body;
    }
}
