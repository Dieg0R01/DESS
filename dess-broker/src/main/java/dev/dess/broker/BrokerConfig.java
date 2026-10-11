package dev.dess.broker;

import dev.dess.common.protocol.TopicName;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
        Path keyStore,
        Path trustStore,
        Path passwordFile,
        boolean discoveryEnabled,
        int discoveryPort,
        boolean presenceEnabled,
        String adminBindAddress,
        int adminPort,
        Path configPath) {

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
        if (keyStore == null) {
            throw new IllegalArgumentException("keyStore is required");
        }
        if (trustStore == null) {
            throw new IllegalArgumentException("trustStore is required");
        }
        if (passwordFile == null) {
            throw new IllegalArgumentException("passwordFile is required");
        }
        if (discoveryPort < 0 || discoveryPort > 65535) {
            throw new IllegalArgumentException("discoveryPort must be between 0 and 65535");
        }
        if (adminBindAddress == null || adminBindAddress.isBlank()) {
            throw new IllegalArgumentException("adminBindAddress is required");
        }
        try {
            InetAddress.getByName(adminBindAddress);
        } catch (UnknownHostException ex) {
            throw new IllegalArgumentException("adminBindAddress is not resolvable");
        }
        if (adminPort < 0 || adminPort > 65535) {
            throw new IllegalArgumentException("adminPort must be between 0 and 65535");
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
        topics = unique;
    }

    void remember(String name) {
        if (!topics.contains(name)) {
            topics.add(name);
        }
    }
}
