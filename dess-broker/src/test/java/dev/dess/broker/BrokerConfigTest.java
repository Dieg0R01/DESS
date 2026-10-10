package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class BrokerConfigTest {

    @Test
    void fourArgumentConstructorKeepsTheNewDefaults() {
        BrokerConfig config = new BrokerConfig(0, 8, 4, 30_000);
        assertEquals(0, config.port());
        assertEquals(8, config.maxConnections());
        assertEquals(4, config.maxPayloadBytes());
        assertEquals(30_000, config.idleTimeoutMillis());
        assertEquals(10_000, config.maxInFlight());
        assertEquals(1024, config.topicQueueCapacity());
        assertEquals(256, config.outboundCapacity());
        assertEquals(2_000, config.slowConsumerGraceMillis());
        assertEquals(1_000, config.publishWaitMillis());
        assertEquals(5_000, config.ackTimeoutMillis());
        assertTrue(config.autoCreateTopics());
        assertEquals(List.of(), config.topics());
    }

    @Test
    void rejectsValuesOutsideTheRangeAndCollapsesDuplicateTopics() {
        assertThrows(IllegalArgumentException.class, () -> full(0, 1024, 256, 1_000, 5_000, List.of()));
        assertThrows(IllegalArgumentException.class, () -> full(10, 0, 256, 1_000, 5_000, List.of()));
        assertThrows(IllegalArgumentException.class, () -> full(10, 1024, 0, 1_000, 5_000, List.of()));
        assertThrows(IllegalArgumentException.class, () -> full(10, 10, 11, 1_000, 5_000, List.of()));
        assertThrows(IllegalArgumentException.class, () -> full(10, 1024, 256, -1, 5_000, List.of()));
        assertThrows(IllegalArgumentException.class, () -> full(10, 1024, 256, 1_000, 0, List.of()));
        assertThrows(IllegalArgumentException.class, () -> new BrokerConfig(0, 8, 4, 30_000, 10, 1024, 256, 0, 1_000, 5_000, true, List.of()));
        assertThrows(IllegalArgumentException.class, () -> full(10, 1024, 256, 1_000, 5_000, List.of("Sales")));

        BrokerConfig zeroWait = full(10, 1024, 256, 0, 5_000, List.of());
        assertEquals(0, zeroWait.publishWaitMillis());
        BrokerConfig topics = full(10, 1024, 256, 1_000, 5_000, List.of("sales", "stock", "sales"));
        assertEquals(List.of("sales", "stock"), topics.topics());
    }

    private static BrokerConfig full(int inFlight, int capacity, int outbound, int publishWait, int ackTimeout, List<String> topics) {
        return new BrokerConfig(0, 8, 4, 30_000, inFlight, capacity, outbound, 2_000, publishWait, ackTimeout, false, topics);
    }
}
