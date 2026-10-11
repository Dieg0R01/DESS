package dev.dess.broker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class TestConfigs {

    private TestConfigs() {
    }

    public static BrokerConfig durable(Path logDirectory, Durability durability, int syncIntervalMillis) {
        return new BrokerConfig(
                0,
                32,
                BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES,
                120_000,
                BrokerConfig.DEFAULT_MAX_IN_FLIGHT,
                BrokerConfig.DEFAULT_TOPIC_QUEUE_CAPACITY,
                BrokerConfig.DEFAULT_OUTBOUND_CAPACITY,
                BrokerConfig.DEFAULT_SLOW_CONSUMER_GRACE_MILLIS,
                BrokerConfig.DEFAULT_PUBLISH_WAIT_MILLIS,
                BrokerConfig.DEFAULT_ACK_TIMEOUT_MILLIS,
                true,
                List.of(),
                logDirectory,
                durability,
                syncIntervalMillis,
                "INFO",
                1_000,
                2_000,
                null);
    }

    public static BrokerConfig of(int port, int maxConnections, int maxPayloadBytes, int idleTimeoutMillis) {
        return full(port, maxConnections, maxPayloadBytes, idleTimeoutMillis,
                BrokerConfig.DEFAULT_MAX_IN_FLIGHT,
                BrokerConfig.DEFAULT_TOPIC_QUEUE_CAPACITY,
                BrokerConfig.DEFAULT_OUTBOUND_CAPACITY,
                BrokerConfig.DEFAULT_SLOW_CONSUMER_GRACE_MILLIS,
                BrokerConfig.DEFAULT_PUBLISH_WAIT_MILLIS,
                BrokerConfig.DEFAULT_ACK_TIMEOUT_MILLIS,
                true,
                List.of());
    }

    public static BrokerConfig full(
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
        try {
            Path logDirectory = Files.createTempDirectory("dess-test-log");
            logDirectory.toFile().deleteOnExit();
            return new BrokerConfig(
                    port,
                    maxConnections,
                    maxPayloadBytes,
                    idleTimeoutMillis,
                    maxInFlight,
                    topicQueueCapacity,
                    outboundCapacity,
                    slowConsumerGraceMillis,
                    publishWaitMillis,
                    ackTimeoutMillis,
                    autoCreateTopics,
                    topics,
                    logDirectory,
                    Durability.INTERVAL,
                    60_000,
                    "INFO",
                    1_000,
                    2_000,
                    null);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
