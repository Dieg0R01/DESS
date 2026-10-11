package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class BrokerConfigTest {

    @Test
    void defaultsFillEveryFieldFromTheDocumentedConstants() {
        BrokerConfig config = BrokerConfig.defaults();
        assertEquals(BrokerConfig.DEFAULT_PORT, config.port());
        assertEquals(BrokerConfig.DEFAULT_MAX_CONNECTIONS, config.maxConnections());
        assertEquals(BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, config.maxPayloadBytes());
        assertEquals(BrokerConfig.DEFAULT_IDLE_TIMEOUT_MILLIS, config.idleTimeoutMillis());
        assertEquals(10_000, config.maxInFlight());
        assertEquals(1024, config.topicQueueCapacity());
        assertEquals(256, config.outboundCapacity());
        assertEquals(2_000, config.slowConsumerGraceMillis());
        assertEquals(1_000, config.publishWaitMillis());
        assertEquals(5_000, config.ackTimeoutMillis());
        assertTrue(config.autoCreateTopics());
        assertEquals(List.of(), config.topics());
        assertEquals(Path.of("data", "logs"), config.logDirectory());
        assertEquals(Durability.ALWAYS, config.durability());
        assertEquals(1_000, config.syncIntervalMillis());
        assertEquals("INFO", config.logLevel());
        assertEquals(1_000, config.threadKeepAliveMillis());
        assertEquals(2_000, config.shutdownTimeoutMillis());
        assertNull(config.configPath());
    }

    @Test
    void testConfigsUseAnIntervalLogAndAPrivateDirectory() {
        BrokerConfig config = TestConfigs.of(0, 8, 4, 30_000);
        assertEquals(0, config.port());
        assertEquals(8, config.maxConnections());
        assertEquals(4, config.maxPayloadBytes());
        assertEquals(30_000, config.idleTimeoutMillis());
        assertEquals(Durability.INTERVAL, config.durability());
        assertEquals(60_000, config.syncIntervalMillis());
        assertEquals(1_000, config.threadKeepAliveMillis());
        assertEquals(2_000, config.shutdownTimeoutMillis());
        assertTrue(config.logDirectory().toString().contains("dess-test-log"));
    }

    @Test
    void rejectsValuesOutsideTheRangeAndCollapsesDuplicateTopics() {
        assertThrows(IllegalArgumentException.class, () -> full(0, 1024, 256, 1_000, 5_000, List.of()));
        assertThrows(IllegalArgumentException.class, () -> full(10, 0, 256, 1_000, 5_000, List.of()));
        assertThrows(IllegalArgumentException.class, () -> full(10, 1024, 0, 1_000, 5_000, List.of()));
        assertThrows(IllegalArgumentException.class, () -> full(10, 10, 11, 1_000, 5_000, List.of()));
        assertThrows(IllegalArgumentException.class, () -> full(10, 1024, 256, -1, 5_000, List.of()));
        assertThrows(IllegalArgumentException.class, () -> full(10, 1024, 256, 1_000, 0, List.of()));
        assertThrows(IllegalArgumentException.class, () -> stored(10, 1024, 256, 2_000, 1_000, 5_000, true, List.of(), null, Durability.ALWAYS, 1_000, "INFO", 1_000, 2_000));
        assertThrows(IllegalArgumentException.class, () -> stored(10, 1024, 256, 2_000, 1_000, 5_000, true, List.of(), Path.of("logs"), null, 1_000, "INFO", 1_000, 2_000));
        assertThrows(IllegalArgumentException.class, () -> stored(10, 1024, 256, 2_000, 1_000, 5_000, true, List.of(), Path.of("logs"), Durability.ALWAYS, 0, "INFO", 1_000, 2_000));
        assertThrows(IllegalArgumentException.class, () -> stored(10, 1024, 256, 2_000, 1_000, 5_000, true, List.of(), Path.of("logs"), Durability.ALWAYS, 1_000, "DEBUG", 1_000, 2_000));
        assertThrows(IllegalArgumentException.class, () -> stored(10, 1024, 256, 2_000, 1_000, 5_000, true, List.of(), Path.of("logs"), Durability.ALWAYS, 1_000, "INFO", 0, 2_000));
        assertThrows(IllegalArgumentException.class, () -> stored(10, 1024, 256, 2_000, 1_000, 5_000, true, List.of(), Path.of("logs"), Durability.ALWAYS, 1_000, "INFO", 1_000, 0));
        assertThrows(IllegalArgumentException.class, () -> full(10, 1024, 256, 1_000, 5_000, List.of("Sales")));

        BrokerConfig zeroWait = full(10, 1024, 256, 0, 5_000, List.of());
        assertEquals(0, zeroWait.publishWaitMillis());
        BrokerConfig topics = full(10, 1024, 256, 1_000, 5_000, List.of("sales", "stock", "sales"));
        assertEquals(List.of("sales", "stock"), topics.topics());
    }

    private static BrokerConfig full(int inFlight, int capacity, int outbound, int publishWait, int ackTimeout, List<String> topics) {
        return stored(inFlight, capacity, outbound, 2_000, publishWait, ackTimeout, false, topics, Path.of("logs"), Durability.ALWAYS, 1_000, "INFO", 1_000, 2_000);
    }

    private static BrokerConfig stored(
            int inFlight,
            int capacity,
            int outbound,
            int grace,
            int publishWait,
            int ackTimeout,
            boolean autoCreate,
            List<String> topics,
            Path logDirectory,
            Durability durability,
            int syncIntervalMillis,
            String logLevel,
            int threadKeepAliveMillis,
            int shutdownTimeoutMillis) {
        return new BrokerConfig(0, 8, 4, 30_000, inFlight, capacity, outbound, grace, publishWait, ackTimeout, autoCreate, topics,
                logDirectory, durability, syncIntervalMillis, logLevel, threadKeepAliveMillis, shutdownTimeoutMillis, null);
    }
}
