package dev.dess.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

final class Topic {

    private final String name;

    private final int capacity;

    private final int outboundCapacity;

    private final Consumer<Subscription> onFull;

    private final ReentrantLock lock = new ReentrantLock();

    /** Signalled with {@link #lock} held when the oldest ring slot is freed. */
    private final Condition notFull = lock.newCondition();

    /** Guarded by {@link #lock}. Allocated on the first accepted message. */
    private Entry[] ring;

    /** Guarded by {@link #lock}. */
    private long baseOffset;

    /** Guarded by {@link #lock}. */
    private long nextOffset;

    /** Guarded by {@link #lock}. */
    private final Map<Long, Subscription> subscriptions = new HashMap<>();

    Topic(String name, int capacity, int outboundCapacity, Consumer<Subscription> onFull) {
        this.name = name;
        this.capacity = capacity;
        this.outboundCapacity = outboundCapacity;
        this.onFull = onFull;
    }

    String name() {
        return name;
    }

    boolean add(ClientConnection connection) {
        lock.lock();
        try {
            if (connection.isClosing() || subscriptions.containsKey(connection.id())) {
                return false;
            }
            Subscription subscription = new Subscription(connection, this, nextOffset);
            subscriptions.put(connection.id(), subscription);
            connection.remember(subscription);
            if (!connection.isClosing()) {
                return true;
            }
            subscriptions.remove(connection.id());
            connection.forget(name);
            subscription.deactivate();
            releaseOwed(subscription);
            reclaim();
            return false;
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

    OptionalLong append(long publisherId, byte[] payload, long deadlineNanos) throws InterruptedException {
        List<Subscription> becameFull = new ArrayList<>();
        OptionalLong accepted;
        lock.lock();
        try {
            while (ring != null && nextOffset - baseOffset == capacity) {
                long waiting = deadlineNanos - System.nanoTime();
                if (waiting <= 0) {
                    return OptionalLong.empty();
                }
                notFull.awaitNanos(waiting);
            }
            accepted = OptionalLong.of(accept(publisherId, payload, becameFull));
        } finally {
            lock.unlock();
        }
        for (Subscription subscription : becameFull) {
            onFull.accept(subscription);
        }
        return accepted;
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
            if (!subscription.isActive() || subscription.cursor() >= nextOffset || ring == null) {
                return null;
            }
            Entry entry = ring[index(subscription.cursor())];
            if (entry == null || entry.payload == null) {
                return null;
            }
            return new Delivery(entry.offset, entry.payload);
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
            Entry entry = ring[index(subscription.cursor())];
            subscription.advanceCursor();
            if (entry != null) {
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

    private long accept(long publisherId, byte[] payload, List<Subscription> becameFull) {
        if (ring == null) {
            ring = new Entry[capacity];
        }
        long offset = nextOffset++;
        Entry entry = new Entry(offset, payload.clone(), publisherId, subscriptions.size());
        ring[index(offset)] = entry;
        if (entry.remaining == 0) {
            entry.payload = null;
            reclaim();
            return offset;
        }
        long now = System.nanoTime();
        for (Subscription subscription : subscriptions.values()) {
            markReady(subscription);
            if (nextOffset - subscription.cursor() >= outboundCapacity && subscription.markFull(now)) {
                becameFull.add(subscription);
            }
        }
        return offset;
    }

    private void releaseOwed(Subscription subscription) {
        if (ring == null) {
            return;
        }
        for (long offset = subscription.cursor(); offset < nextOffset; offset++) {
            Entry entry = ring[index(offset)];
            if (entry != null) {
                release(entry);
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

    static final class Delivery {

        private final long offset;

        private final byte[] payload;

        private Delivery(long offset, byte[] payload) {
            this.offset = offset;
            this.payload = payload;
        }

        long offset() {
            return offset;
        }

        byte[] payload() {
            return payload;
        }
    }

    private static final class Entry {

        private final long offset;

        /** Accepting connection. */
        private final long publisherId;

        /** Guarded by the topic lock. */
        private byte[] payload;

        /** Guarded by the topic lock. */
        private int remaining;

        private Entry(long offset, byte[] payload, long publisherId, int remaining) {
            this.offset = offset;
            this.payload = payload;
            this.publisherId = publisherId;
            this.remaining = remaining;
        }
    }
}
