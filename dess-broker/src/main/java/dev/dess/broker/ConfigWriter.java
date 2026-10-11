package dev.dess.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Rewrites the operator document. The lock is held only while a topic name is persisted.
 * Callers must not hold a topic lock or a writer lock.
 */
final class ConfigWriter {

    private final BrokerConfig config;

    private final ReentrantLock lock = new ReentrantLock();

    private final Set<String> persisted = ConcurrentHashMap.newKeySet();

    ConfigWriter(BrokerConfig config) {
        this.config = config;
        persisted.addAll(config.topics());
    }

    void lock() {
        lock.lock();
    }

    void unlock() {
        lock.unlock();
    }

    void remember(String name) throws IOException {
        if (persisted.contains(name)) {
            return;
        }
        lock.lock();
        try {
            if (persisted.contains(name)) {
                return;
            }
            config.remember(name);
            rewrite();
            persisted.add(name);
        } finally {
            lock.unlock();
        }
    }

    void includeRecovered(Collection<String> names) throws IOException {
        lock.lock();
        try {
            ArrayList<String> extra = new ArrayList<>();
            for (String name : names) {
                if (!persisted.contains(name)) {
                    extra.add(name);
                }
            }
            if (extra.isEmpty()) {
                return;
            }
            Collections.sort(extra);
            for (String name : extra) {
                config.remember(name);
            }
            rewrite();
            persisted.addAll(extra);
        } finally {
            lock.unlock();
        }
    }

    private void rewrite() throws IOException {
        Path file = config.configPath();
        if (file == null) {
            return;
        }
        Path temporary = Path.of(file.toAbsolutePath().toString() + ".tmp");
        byte[] bytes = document().getBytes(StandardCharsets.UTF_8);
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                if (channel.write(buffer) < 0) {
                    throw new IOException("Could not write " + temporary.toAbsolutePath());
                }
            }
            channel.force(true);
        }
        Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private String document() {
        StringBuilder builder = new StringBuilder();
        builder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        builder.append("<broker>\n");
        builder.append("    <network>\n");
        element(builder, "port", config.port());
        element(builder, "maxConnections", config.maxConnections());
        element(builder, "maxPayloadBytes", config.maxPayloadBytes());
        element(builder, "idleTimeoutMs", config.idleTimeoutMillis());
        builder.append("    </network>\n");
        builder.append("    <engine>\n");
        element(builder, "maxInFlight", config.maxInFlight());
        element(builder, "topicQueueCapacity", config.topicQueueCapacity());
        element(builder, "outboundCapacity", config.outboundCapacity());
        element(builder, "slowConsumerGraceMs", config.slowConsumerGraceMillis());
        element(builder, "publishWaitMs", config.publishWaitMillis());
        element(builder, "ackTimeoutMs", config.ackTimeoutMillis());
        element(builder, "threadKeepAliveMs", config.threadKeepAliveMillis());
        element(builder, "shutdownTimeoutMs", config.shutdownTimeoutMillis());
        builder.append("    </engine>\n");
        builder.append("    <storage>\n");
        element(builder, "logDirectory", config.logDirectory().toAbsolutePath().toString());
        element(builder, "durability", config.durability().name());
        element(builder, "syncIntervalMs", config.syncIntervalMillis());
        builder.append("    </storage>\n");
        builder.append("    <topics autoCreate=\"").append(config.autoCreateTopics()).append("\">\n");
        for (String topic : config.topics()) {
            element(builder, "topic", topic);
        }
        builder.append("    </topics>\n");
        builder.append("    <logging>\n");
        element(builder, "level", config.logLevel());
        builder.append("    </logging>\n");
        builder.append("    <security>\n");
        element(builder, "keyStore", config.keyStore().toAbsolutePath().toString());
        element(builder, "trustStore", config.trustStore().toAbsolutePath().toString());
        element(builder, "passwordFile", config.passwordFile().toAbsolutePath().toString());
        builder.append("    </security>\n");
        builder.append("    <discovery enabled=\"").append(config.discoveryEnabled())
                .append("\" presence=\"").append(config.presenceEnabled()).append("\">\n");
        element(builder, "port", config.discoveryPort());
        builder.append("    </discovery>\n");
        builder.append("</broker>\n");
        return builder.toString();
    }

    private static void element(StringBuilder builder, String name, int value) {
        element(builder, name, Integer.toString(value));
    }

    private static void element(StringBuilder builder, String name, String value) {
        builder.append("        <").append(name).append('>').append(escape(value)).append("</").append(name).append(">\n");
    }

    private static String escape(String value) {
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }
}
