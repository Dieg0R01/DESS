package dev.dess.broker;

import dev.dess.common.protocol.TopicName;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public record BrokerConfig(
        int port,
        int maxConnections,
        int maxPayloadBytes,
        int idleTimeoutMillis,
        int maxInFlight,
        int topicQueueCapacity,
        int outboundCapacity,
        int slowConsumerGraceMillis,
        int publishWaitMillis,
        int ackTimeoutMillis,
        boolean autoCreateTopics,
        List<String> topics,
        Path logDirectory,
        Durability durability,
        int syncIntervalMillis,
        String logLevel,
        int threadKeepAliveMillis,
        int shutdownTimeoutMillis,
        Path configPath) {

    public static final int DEFAULT_PORT = 55123;

    public static final int DEFAULT_MAX_CONNECTIONS = 128;

    public static final int DEFAULT_MAX_PAYLOAD_BYTES = 1048576;

    public static final int DEFAULT_IDLE_TIMEOUT_MILLIS = 30000;

    public static final int DEFAULT_MAX_IN_FLIGHT = 10000;

    public static final int DEFAULT_TOPIC_QUEUE_CAPACITY = 1024;

    public static final int DEFAULT_OUTBOUND_CAPACITY = 256;

    public static final int DEFAULT_SLOW_CONSUMER_GRACE_MILLIS = 2000;

    public static final int DEFAULT_PUBLISH_WAIT_MILLIS = 1000;

    public static final int DEFAULT_ACK_TIMEOUT_MILLIS = 5000;

    public static final Path DEFAULT_LOG_DIRECTORY = Path.of("data", "logs");

    public static final int DEFAULT_SYNC_INTERVAL_MILLIS = 1000;

    public static final String DEFAULT_LOG_LEVEL = "INFO";

    public static final int DEFAULT_THREAD_KEEP_ALIVE_MILLIS = 1000;

    public static final int DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 2000;

    private static final Set<String> LOG_LEVELS = Set.of("SEVERE", "WARNING", "INFO", "FINE");

    public BrokerConfig {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
        if (maxConnections < 1) {
            throw new IllegalArgumentException("maxConnections must be at least 1");
        }
        if (maxPayloadBytes < 1) {
            throw new IllegalArgumentException("maxPayloadBytes must be at least 1");
        }
        if (idleTimeoutMillis < 1) {
            throw new IllegalArgumentException("idleTimeoutMillis must be at least 1");
        }
        if (maxInFlight < 1) {
            throw new IllegalArgumentException("maxInFlight must be at least 1");
        }
        if (topicQueueCapacity < 1) {
            throw new IllegalArgumentException("topicQueueCapacity must be at least 1");
        }
        if (outboundCapacity < 1) {
            throw new IllegalArgumentException("outboundCapacity must be at least 1");
        }
        if (outboundCapacity > topicQueueCapacity) {
            throw new IllegalArgumentException("outboundCapacity must not exceed topicQueueCapacity");
        }
        if (slowConsumerGraceMillis < 1) {
            throw new IllegalArgumentException("slowConsumerGraceMillis must be at least 1");
        }
        if (publishWaitMillis < 0) {
            throw new IllegalArgumentException("publishWaitMillis must be at least 0");
        }
        if (ackTimeoutMillis < 1) {
            throw new IllegalArgumentException("ackTimeoutMillis must be at least 1");
        }
        if (topics == null) {
            throw new IllegalArgumentException("topics are required");
        }
        if (logDirectory == null) {
            throw new IllegalArgumentException("logDirectory is required");
        }
        if (durability == null) {
            throw new IllegalArgumentException("durability is required");
        }
        if (syncIntervalMillis < 1) {
            throw new IllegalArgumentException("syncIntervalMillis must be at least 1");
        }
        if (!LOG_LEVELS.contains(logLevel)) {
            throw new IllegalArgumentException("logLevel must be SEVERE, WARNING, INFO or FINE");
        }
        if (threadKeepAliveMillis < 1) {
            throw new IllegalArgumentException("threadKeepAliveMillis must be at least 1");
        }
        if (shutdownTimeoutMillis < 1) {
            throw new IllegalArgumentException("shutdownTimeoutMillis must be at least 1");
        }
        List<String> unique = new ArrayList<>();
        for (String name : topics) {
            if (!TopicName.isValid(name)) {
                throw new IllegalArgumentException("invalid topic name");
            }
            if (!unique.contains(name)) {
                unique.add(name);
            }
        }
        topics = List.copyOf(unique);
    }

    public static BrokerConfig defaults() {
        return new BrokerConfig(
                DEFAULT_PORT,
                DEFAULT_MAX_CONNECTIONS,
                DEFAULT_MAX_PAYLOAD_BYTES,
                DEFAULT_IDLE_TIMEOUT_MILLIS,
                DEFAULT_MAX_IN_FLIGHT,
                DEFAULT_TOPIC_QUEUE_CAPACITY,
                DEFAULT_OUTBOUND_CAPACITY,
                DEFAULT_SLOW_CONSUMER_GRACE_MILLIS,
                DEFAULT_PUBLISH_WAIT_MILLIS,
                DEFAULT_ACK_TIMEOUT_MILLIS,
                true,
                List.of(),
                DEFAULT_LOG_DIRECTORY,
                Durability.ALWAYS,
                DEFAULT_SYNC_INTERVAL_MILLIS,
                DEFAULT_LOG_LEVEL,
                DEFAULT_THREAD_KEEP_ALIVE_MILLIS,
                DEFAULT_SHUTDOWN_TIMEOUT_MILLIS,
                null);
    }
}
