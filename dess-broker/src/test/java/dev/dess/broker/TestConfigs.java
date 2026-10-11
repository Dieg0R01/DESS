package dev.dess.broker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class TestConfigs {

    public static final int DEFAULT_MAX_PAYLOAD_BYTES = 1048576;

    public static final int DEFAULT_MAX_IN_FLIGHT = 10000;

    public static final int DEFAULT_TOPIC_QUEUE_CAPACITY = 1024;

    public static final int DEFAULT_OUTBOUND_CAPACITY = 256;

    public static final int DEFAULT_SLOW_CONSUMER_GRACE_MILLIS = 2000;

    public static final int DEFAULT_PUBLISH_WAIT_MILLIS = 1000;

    public static final int DEFAULT_ACK_TIMEOUT_MILLIS = 5000;

    private TestConfigs() {
    }

    public static String securityXml() {
        return """
                <security>
                    <keyStore>%s</keyStore>
                    <trustStore>%s</trustStore>
                    <passwordFile>%s</passwordFile>
                </security>
                """.formatted(
                TestPki.brokerStore().toAbsolutePath(),
                TestPki.trustStore().toAbsolutePath(),
                TestPki.passwordFile().toAbsolutePath());
    }

    public static BrokerConfig durable(Path logDirectory, Durability durability, int syncIntervalMillis) {
        return new BrokerConfig(
                0,
                32,
                TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES,
                120_000,
                TestConfigs.DEFAULT_MAX_IN_FLIGHT,
                TestConfigs.DEFAULT_TOPIC_QUEUE_CAPACITY,
                TestConfigs.DEFAULT_OUTBOUND_CAPACITY,
                TestConfigs.DEFAULT_SLOW_CONSUMER_GRACE_MILLIS,
                TestConfigs.DEFAULT_PUBLISH_WAIT_MILLIS,
                TestConfigs.DEFAULT_ACK_TIMEOUT_MILLIS,
                true,
                List.of(),
                logDirectory,
                durability,
                syncIntervalMillis,
                "INFO",
                1_000,
                2_000,
                TestPki.brokerStore(),
                TestPki.trustStore(),
                TestPki.passwordFile(),
                false,
                55124,
                false,
                "127.0.0.1",
                0,
                null);
    }

    public static BrokerConfig of(int port, int maxConnections, int maxPayloadBytes, int idleTimeoutMillis) {
        return full(port, maxConnections, maxPayloadBytes, idleTimeoutMillis,
                TestConfigs.DEFAULT_MAX_IN_FLIGHT,
                TestConfigs.DEFAULT_TOPIC_QUEUE_CAPACITY,
                TestConfigs.DEFAULT_OUTBOUND_CAPACITY,
                TestConfigs.DEFAULT_SLOW_CONSUMER_GRACE_MILLIS,
                TestConfigs.DEFAULT_PUBLISH_WAIT_MILLIS,
                TestConfigs.DEFAULT_ACK_TIMEOUT_MILLIS,
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
                    TestPki.brokerStore(),
                    TestPki.trustStore(),
                    TestPki.passwordFile(),
                    false,
                    55124,
                    false,
                    "127.0.0.1",
                    0,
                    null);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public static BrokerConfig withAdminPort(int adminPort) {
        BrokerConfig base = of(0, 32, DEFAULT_MAX_PAYLOAD_BYTES, 120_000);
        return new BrokerConfig(
                base.port(),
                base.maxConnections(),
                base.maxPayloadBytes(),
                base.idleTimeoutMillis(),
                base.maxInFlight(),
                base.topicQueueCapacity(),
                base.outboundCapacity(),
                base.slowConsumerGraceMillis(),
                base.publishWaitMillis(),
                base.ackTimeoutMillis(),
                base.autoCreateTopics(),
                base.topics(),
                base.logDirectory(),
                base.durability(),
                base.syncIntervalMillis(),
                base.logLevel(),
                base.threadKeepAliveMillis(),
                base.shutdownTimeoutMillis(),
                base.keyStore(),
                base.trustStore(),
                base.passwordFile(),
                base.discoveryEnabled(),
                base.discoveryPort(),
                base.presenceEnabled(),
                base.adminBindAddress(),
                adminPort,
                base.configPath());
    }

    public static BrokerConfig discoveryEnabled(int discoveryPort) {
        return discovery(32, true, false, discoveryPort);
    }

    public static BrokerConfig discoveryDisabled(int discoveryPort) {
        return discovery(32, false, true, discoveryPort);
    }

    public static BrokerConfig presenceEnabled(int discoveryPort) {
        return presence(32, discoveryPort);
    }

    public static BrokerConfig presence(int maxConnections, int discoveryPort) {
        return discovery(maxConnections, true, true, discoveryPort);
    }

    private static BrokerConfig discovery(
            int maxConnections, boolean discoveryEnabled, boolean presenceEnabled, int discoveryPort) {
        try {
            Path logDirectory = Files.createTempDirectory("dess-test-log");
            logDirectory.toFile().deleteOnExit();
            return new BrokerConfig(
                    0,
                    maxConnections,
                    TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES,
                    120_000,
                    TestConfigs.DEFAULT_MAX_IN_FLIGHT,
                    TestConfigs.DEFAULT_TOPIC_QUEUE_CAPACITY,
                    TestConfigs.DEFAULT_OUTBOUND_CAPACITY,
                    TestConfigs.DEFAULT_SLOW_CONSUMER_GRACE_MILLIS,
                    TestConfigs.DEFAULT_PUBLISH_WAIT_MILLIS,
                    TestConfigs.DEFAULT_ACK_TIMEOUT_MILLIS,
                    true,
                    List.of(),
                    logDirectory,
                    Durability.INTERVAL,
                    60_000,
                    "INFO",
                    1_000,
                    2_000,
                    TestPki.brokerStore(),
                    TestPki.trustStore(),
                    TestPki.passwordFile(),
                    discoveryEnabled,
                    discoveryPort,
                    presenceEnabled,
                    "127.0.0.1",
                    0,
                    null);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
