package dev.dess.broker;

import dev.dess.common.protocol.TopicName;
import java.util.ArrayList;
import java.util.List;

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
        List<String> topics) {

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

    public BrokerConfig(int port, int maxConnections, int maxPayloadBytes, int idleTimeoutMillis) {
        this(port, maxConnections, maxPayloadBytes, idleTimeoutMillis,
                DEFAULT_MAX_IN_FLIGHT, DEFAULT_TOPIC_QUEUE_CAPACITY, DEFAULT_OUTBOUND_CAPACITY,
                DEFAULT_SLOW_CONSUMER_GRACE_MILLIS, DEFAULT_PUBLISH_WAIT_MILLIS, DEFAULT_ACK_TIMEOUT_MILLIS,
                true, List.of());
    }

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
        return new BrokerConfig(DEFAULT_PORT, DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_PAYLOAD_BYTES, DEFAULT_IDLE_TIMEOUT_MILLIS);
    }
}
