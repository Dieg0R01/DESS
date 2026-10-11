package dev.dess.broker;

import dev.dess.common.protocol.ConfirmationMode;
import dev.dess.common.protocol.StartPosition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

final class Topic {

    private final String name;

    private final int capacity;

    private final int outboundCapacity;

    private final Consumer<Subscription> onFull;

    private final FlowControl flow;

    private final TopicRegistry registry;

    private final TopicLog log;

    private final ReentrantLock lock = new ReentrantLock();

    /** Signalled with {@link #lock} held when the oldest ring slot is freed. */
    private final Condition notFull = lock.newCondition();

    /** Signalled with {@link #lock} held when {@link #nextOffset} advances. */
    private final Condition committed = lock.newCondition();

    /** Guarded by {@link #lock}. Allocated on the first accepted message. */
    private Entry[] ring;

    /** Guarded by {@link #lock}. */
    private long baseOffset;

    /** Guarded by {@link #lock}. Visible records. Advances only after the log append returns. */
    private long nextOffset;

    /** Guarded by {@link #lock}. Appends that have reserved ring space and are not visible yet. */
    private int inflight;

    /** Guarded by {@link #lock}. */
    private final Map<Long, Subscription> subscriptions = new HashMap<>();

    Topic(String name, int capacity, int outboundCapacity, FlowControl flow, TopicRegistry registry, Consumer<Subscription> onFull, TopicLog log) {
        this.name = name;
        this.capacity = capacity;
        this.outboundCapacity = outboundCapacity;
        this.flow = flow;
        this.registry = registry;
        this.onFull = onFull;
        this.log = log;
        long recovered = log.nextOffset();
        this.nextOffset = recovered;
        this.baseOffset = recovered;
    }

    String name() {
        return name;
    }

    Set<Long> subscribers() {
        lock.lock();
        try {
            return Set.copyOf(subscriptions.keySet());
        } finally {
            lock.unlock();
        }
    }

    Membership add(ClientConnection connection, StartPosition start) {
        lock.lock();
        try {
            if (connection.isClosing() || subscriptions.containsKey(connection.id())) {
                return Membership.UNCHANGED;
            }
            Long cursor = cursor(start);
            if (cursor == null) {
                return Membership.INVALID_OFFSET;
            }
            Subscription subscription = new Subscription(connection, this, cursor, nextOffset);
            subscriptions.put(connection.id(), subscription);
            connection.remember(subscription);
            if (!connection.isClosing()) {
                markReady(subscription);
                return Membership.ADDED;
            }
            subscriptions.remove(connection.id());
            connection.forget(name);
            subscription.deactivate();
            releaseOwed(subscription);
            reclaim();
            return Membership.UNCHANGED;
        } finally {
            lock.unlock();
        }
    }

    boolean remove(ClientConnection connection) {
        lock.lock();
        try {
            Subscription subscription = subscriptions.remove(connection.id());
            if (subscription == null) {
                return false;
            }
            connection.forget(name);
            subscription.deactivate();
            releaseOwed(subscription);
            reclaim();
            return true;
        } finally {
            lock.unlock();
        }
    }

    AppendResult append(long publisherId, byte[] payload, byte[] digest, long deadlineNanos, ConfirmationMode mode, int ackTimeoutMillis) throws InterruptedException {
        boolean waited = false;
        lock.lock();
        try {
            while (ring != null && nextOffset + inflight - baseOffset == capacity) {
                waited = true;
                long waiting = deadlineNanos - System.nanoTime();
                if (waiting <= 0) {
                    return AppendResult.rejected();
                }
                notFull.awaitNanos(waiting);
            }
            if (ring == null) {
                ring = new Entry[capacity];
            }
            inflight++;
        } finally {
            lock.unlock();
        }
        TopicLog.Written written;
        try {
            written = log.append(payload, digest);
        } catch (IOException ex) {
            lock.lock();
            try {
                inflight--;
                notFull.signalAll();
            } finally {
                lock.unlock();
            }
            return AppendResult.storage(waited);
        }
        List<Subscription> becameFull = new ArrayList<>();
        boolean interrupted = false;
        lock.lock();
        try {
            while (nextOffset != written.offset()) {
                try {
                    committed.await();
                } catch (InterruptedException ex) {
                    interrupted = true;
                }
            }
            accept(written.offset(), publisherId, payload, digest, becameFull, mode, ackTimeoutMillis);
            inflight--;
            committed.signalAll();
        } finally {
            lock.unlock();
        }
        for (Subscription subscription : becameFull) {
            onFull.accept(subscription);
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return new AppendResult(written.offset(), waited, written.timestamp());
    }

    boolean graceElapsed(Subscription subscription, long graceNanos) {
        lock.lock();
        try {
            long since = subscription.fullSince();
            return subscription.isActive() && since != 0L && System.nanoTime() - since >= graceNanos;
        } finally {
            lock.unlock();
        }
    }

    boolean isFull(Subscription subscription) {
        lock.lock();
        try {
            return subscription.fullSince() != 0L;
        } finally {
            lock.unlock();
        }
    }

    Delivery claim(Subscription subscription) {
        lock.lock();
        try {
            if (!subscription.isActive() || subscription.cursor() >= nextOffset) {
                return null;
            }
            long offset = subscription.cursor();
            Entry entry = ring == null ? null : ring[index(offset)];
            if (entry != null && entry.offset == offset && entry.payload != null) {
                return new Delivery(offset, entry.payload, entry.digest, -1L);
            }
            return new Delivery(offset, null, null, log.position(offset));
        } finally {
            lock.unlock();
        }
    }

    TopicLog.Stored retained(long filePosition) throws IOException {
        return log.read(filePosition);
    }

    void handOff(Subscription subscription) {
        lock.lock();
        try {
            if (subscription.isActive()) {
                subscription.handOffNext();
            }
        } finally {
            lock.unlock();
        }
    }

    boolean delivered(Subscription subscription, long offset) {
        lock.lock();
        try {
            return subscription.isActive()
                    && offset >= subscription.subscribedAt()
                    && offset < subscription.handedOff();
        } finally {
            lock.unlock();
        }
    }

    void settle(Subscription subscription) {
        lock.lock();
        try {
            subscription.clearScheduled();
            markReady(subscription);
        } finally {
            lock.unlock();
        }
    }

    void advance(Subscription subscription) {
        lock.lock();
        try {
            if (!subscription.isActive()) {
                subscription.clearScheduled();
                return;
            }
            long offset = subscription.cursor();
            Entry entry = ring == null ? null : ring[index(offset)];
            subscription.advanceCursor();
            if (offset >= subscription.countedFrom() && entry != null && entry.offset == offset) {
                release(entry);
            }
            if (nextOffset - subscription.cursor() < outboundCapacity) {
                subscription.clearFull();
            }
            reclaim();
            if (subscription.cursor() < nextOffset) {
                subscription.connection().enqueue(subscription);
                return;
            }
            subscription.clearScheduled();
            markReady(subscription);
        } finally {
            lock.unlock();
        }
    }

    private void accept(long offset, long publisherId, byte[] payload, byte[] digest, List<Subscription> becameFull, ConfirmationMode mode, int ackTimeoutMillis) {
        if (ring == null) {
            ring = new Entry[capacity];
        }
        nextOffset = offset + 1;
        Entry entry = new Entry(offset, payload.clone(), digest.clone(), publisherId, subscriptions.size());
        ring[index(offset)] = entry;
        if (entry.remaining == 0) {
            entry.payload = null;
            entry.digest = null;
            reclaim();
            flow.release();
            return;
        }
        switch (mode) {
            case BROKER -> {
            }
            case ALL -> {
                Set<Long> expected = new HashSet<>();
                for (Subscription subscription : subscriptions.values()) {
                    long subscriber = subscription.connection().id();
                    if (subscriber != publisherId && !subscription.connection().isClosing()) {
                        expected.add(subscriber);
                    }
                }
                if (!expected.isEmpty()) {
                    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ackTimeoutMillis);
                    registry.register(name, offset, new PendingConfirmation(expected, deadline));
                }
            }
            default -> {
                ConfirmationMode unexpected = mode;
                throw new IllegalStateException("Unhandled mode " + unexpected);
            }
        }
        long now = System.nanoTime();
        for (Subscription subscription : subscriptions.values()) {
            markReady(subscription);
            if (nextOffset - subscription.cursor() >= outboundCapacity && subscription.markFull(now)) {
                becameFull.add(subscription);
            }
        }
    }

    private void releaseOwed(Subscription subscription) {
        if (ring == null) {
            return;
        }
        long start = Math.max(subscription.cursor(), subscription.countedFrom());
        for (long offset = start; offset < nextOffset; offset++) {
            Entry entry = ring[index(offset)];
            if (entry != null && entry.offset == offset) {
                release(entry);
            }
        }
    }

    private Long cursor(StartPosition start) {
        switch (start.kind()) {
            case LATEST -> {
                return nextOffset;
            }
            case EARLIEST -> {
                return 0L;
            }
            case OFFSET -> {
                long offset = start.offset();
                if (offset < 0 || offset > nextOffset) {
                    return null;
                }
                return offset;
            }
            default -> {
                StartPosition.Kind unexpected = start.kind();
                throw new IllegalStateException("Unhandled start position " + unexpected);
            }
        }
    }

    private void release(Entry entry) {
        if (entry.remaining == 0) {
            return;
        }
        entry.remaining--;
        if (entry.remaining == 0) {
            entry.payload = null;
            entry.digest = null;
            flow.release();
        }
    }

    private void reclaim() {
        boolean freed = false;
        while (baseOffset < nextOffset) {
            Entry oldest = ring[index(baseOffset)];
            if (oldest == null || oldest.remaining > 0) {
                break;
            }
            ring[index(baseOffset)] = null;
            baseOffset++;
            freed = true;
        }
        if (freed) {
            notFull.signalAll();
        }
    }

    private void markReady(Subscription subscription) {
        if (!subscription.isActive() || subscription.cursor() >= nextOffset) {
            return;
        }
        if (subscription.trySchedule()) {
            subscription.connection().enqueue(subscription);
        }
    }

    private int index(long offset) {
        return (int) (offset % capacity);
    }

    static final class AppendResult {

        private final long offset;

        private final boolean accepted;

        private final boolean waited;

        private final boolean storageFailed;

        private final long timestamp;

        private AppendResult(long offset, boolean waited, long timestamp) {
            this.offset = offset;
            this.accepted = true;
            this.waited = waited;
            this.storageFailed = false;
            this.timestamp = timestamp;
        }

        private AppendResult(boolean waited, boolean storageFailed) {
            this.offset = -1;
            this.accepted = false;
            this.waited = waited;
            this.storageFailed = storageFailed;
            this.timestamp = 0;
        }

        static AppendResult rejected() {
            return new AppendResult(true, false);
        }

        static AppendResult storage(boolean waited) {
            return new AppendResult(waited, true);
        }

        boolean accepted() {
            return accepted;
        }

        boolean storageFailed() {
            return storageFailed;
        }

        long offset() {
            return offset;
        }

        boolean waited() {
            return waited;
        }

        long timestamp() {
            return timestamp;
        }
    }

    static final class Delivery {

        private final long offset;

        private final byte[] payload;

        private final byte[] digest;

        private final long filePosition;

        private Delivery(long offset, byte[] payload, byte[] digest, long filePosition) {
            this.offset = offset;
            this.payload = payload;
            this.digest = digest;
            this.filePosition = filePosition;
        }

        long offset() {
            return offset;
        }

        byte[] payload() {
            return payload;
        }

        byte[] digest() {
            return digest;
        }

        long filePosition() {
            return filePosition;
        }
    }

    enum Membership {
        ADDED,
        UNCHANGED,
        INVALID_OFFSET
    }

    private static final class Entry {

        private final long offset;

        /** Accepting connection. */
        private final long publisherId;

        /** Guarded by the topic lock. */
        private byte[] payload;

        /** Guarded by the topic lock. Released with {@link #payload}. */
        private byte[] digest;

        /** Guarded by the topic lock. */
        private int remaining;

        private Entry(long offset, byte[] payload, byte[] digest, long publisherId, int remaining) {
            this.offset = offset;
            this.payload = payload;
            this.digest = digest;
            this.publisherId = publisherId;
            this.remaining = remaining;
        }
    }
}
