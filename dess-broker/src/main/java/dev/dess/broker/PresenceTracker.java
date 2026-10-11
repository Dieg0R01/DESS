package dev.dess.broker;

import dev.dess.common.logging.Logging;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

final class PresenceTracker implements AutoCloseable {

    private final ConcurrentHashMap<Long, PresenceEntry> entries = new ConcurrentHashMap<>();
    private final ScheduledExecutorService timer;
    private final int bound;

    private PresenceTracker(int bound, ScheduledExecutorService timer) {
        this.bound = bound;
        this.timer = timer;
    }

    static PresenceTracker open(int maxConnections) {
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "dess-presence");
            thread.setDaemon(false);
            return thread;
        });
        return new PresenceTracker(maxConnections, timer);
    }

    void announce(long instanceId, String name, String remoteAddress, int intervalMillis) {
        if (intervalMillis < 1 || name == null || name.isBlank()) {
            return;
        }
        synchronized (this) {
            PresenceEntry existing = entries.get(instanceId);
            if (existing == null) {
                if (entries.size() >= bound) {
                    return;
                }
                PresenceEntry created = new PresenceEntry(instanceId, name, remoteAddress, intervalMillis);
                entries.put(instanceId, created);
                created.schedule();
                Map<String, String> joined = new LinkedHashMap<>();
                joined.put("name", name);
                joined.put("remoteAddress", remoteAddress);
                joined.put("intervalMillis", Integer.toString(intervalMillis));
                Logging.event("dev.dess.broker", "presence.joined", joined);
                return;
            }
            existing.refresh(name, remoteAddress, intervalMillis);
        }
    }

    int size() {
        return entries.size();
    }

    boolean contains(long instanceId) {
        return entries.containsKey(instanceId);
    }

    void close(long timeoutMillis) {
        timer.shutdownNow();
        try {
            if (!timer.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Presence thread did not stop");
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping presence", ex);
        }
        entries.clear();
    }

    @Override
    public void close() {
        close(2_000);
    }

    private void leave(long instanceId) {
        PresenceEntry removed;
        synchronized (this) {
            removed = entries.remove(instanceId);
        }
        if (removed == null) {
            return;
        }
        Map<String, String> left = new LinkedHashMap<>();
        left.put("name", removed.name);
        left.put("remoteAddress", removed.remoteAddress);
        Logging.event("dev.dess.broker", "presence.left", left);
    }

    private final class PresenceEntry {

        private final long instanceId;
        private String name;
        private String remoteAddress;
        private int intervalMillis;
        private long lastSeen;
        private ScheduledFuture<?> removal;

        private PresenceEntry(long instanceId, String name, String remoteAddress, int intervalMillis) {
            this.instanceId = instanceId;
            this.name = name;
            this.remoteAddress = remoteAddress;
            this.intervalMillis = intervalMillis;
            this.lastSeen = System.nanoTime();
        }

        private void refresh(String name, String remoteAddress, int intervalMillis) {
            cancel();
            this.name = name;
            this.remoteAddress = remoteAddress;
            this.intervalMillis = intervalMillis;
            this.lastSeen = System.nanoTime();
            schedule();
        }

        private void schedule() {
            removal = timer.schedule(() -> leave(instanceId), 3L * intervalMillis, TimeUnit.MILLISECONDS);
        }

        private void cancel() {
            if (removal != null) {
                removal.cancel(false);
            }
        }
    }
}
