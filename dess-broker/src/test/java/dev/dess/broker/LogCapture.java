package dev.dess.broker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

final class LogCapture implements AutoCloseable {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition arrived = lock.newCondition();
    private final List<String> lines = new ArrayList<>();
    private final Logger broker = Logger.getLogger("dev.dess.broker");
    private final Logger dess = Logger.getLogger("dev.dess");
    private final Level savedBrokerLevel;
    private final Handler handler;
    private Level savedDessLevel;
    private boolean quieted;

    private LogCapture() {
        savedBrokerLevel = broker.getLevel();
        broker.setLevel(null);
        handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage() == null) {
                    return;
                }
                if (quieted && record.getLevel().intValue() < Level.WARNING.intValue()) {
                    return;
                }
                String line = record.getLevel().getName() + " " + record.getLoggerName() + " " + record.getMessage();
                lock.lock();
                try {
                    lines.add(line);
                    arrived.signalAll();
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        handler.setLevel(Level.ALL);
        broker.addHandler(handler);
    }

    static LogCapture open() {
        return new LogCapture();
    }

    boolean await(Duration timeout, String... parts) throws InterruptedException {
        long nanos = timeout.toNanos();
        lock.lock();
        try {
            while (!matches(parts)) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = arrived.awaitNanos(nanos);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    boolean awaitAtLeast(Duration timeout, long minimum, String... parts) throws InterruptedException {
        long nanos = timeout.toNanos();
        lock.lock();
        try {
            while (matching(parts) < minimum) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = arrived.awaitNanos(nanos);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    long count(String... parts) {
        lock.lock();
        try {
            return matching(parts);
        } finally {
            lock.unlock();
        }
    }

    List<String> lines() {
        lock.lock();
        try {
            return List.copyOf(lines);
        } finally {
            lock.unlock();
        }
    }

    void quiet() {
        lock.lock();
        try {
            if (quieted) {
                return;
            }
            savedDessLevel = dess.getLevel();
            quieted = true;
            dess.setLevel(Level.WARNING);
        } finally {
            lock.unlock();
        }
    }

    void awaitAdded(ProtocolClient client, String topic) throws InterruptedException {
        String id = clientId(client.endpoint());
        long nanos = Duration.ofSeconds(2).toNanos();
        lock.lock();
        try {
            while (!added(id, topic)) {
                if (nanos <= 0) {
                    throw new AssertionError("subscription.added missing for " + id + " on " + topic + " in " + List.copyOf(lines));
                }
                nanos = arrived.awaitNanos(nanos);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        broker.removeHandler(handler);
        broker.setLevel(savedBrokerLevel);
        if (quieted) {
            dess.setLevel(savedDessLevel);
        }
    }

    private String clientId(String remoteAddress) throws InterruptedException {
        long nanos = Duration.ofSeconds(2).toNanos();
        lock.lock();
        try {
            while (true) {
                String id = findClient(remoteAddress);
                if (id != null) {
                    return id;
                }
                if (nanos <= 0) {
                    throw new AssertionError("client.connected missing for " + remoteAddress + " in " + List.copyOf(lines));
                }
                nanos = arrived.awaitNanos(nanos);
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean added(String id, String topic) {
        for (String line : lines) {
            if (line.contains("event=subscription.added")
                    && id.equals(property(line, "clientId"))
                    && topic.equals(property(line, "topic"))) {
                return true;
            }
        }
        return false;
    }

    private String findClient(String remoteAddress) {
        for (String line : lines) {
            if (line.contains("event=client.connected") && remoteAddress.equals(property(line, "remoteAddress"))) {
                return property(line, "clientId");
            }
        }
        return null;
    }

    private long matching(String... parts) {
        long count = 0;
        for (String line : lines) {
            if (containsAll(line, parts)) {
                count++;
            }
        }
        return count;
    }

    private boolean matches(String... parts) {
        for (String line : lines) {
            if (containsAll(line, parts)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsAll(String line, String... parts) {
        for (String part : parts) {
            if (!line.contains(part)) {
                return false;
            }
        }
        return true;
    }

    private static String property(String line, String key) {
        String token = key + "=";
        int at = line.indexOf(token);
        if (at < 0) {
            return "";
        }
        int start = at + token.length();
        int end = line.indexOf(' ', start);
        if (end < 0) {
            end = line.length();
        }
        return line.substring(start, end);
    }
}
