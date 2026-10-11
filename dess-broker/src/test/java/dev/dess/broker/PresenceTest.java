package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.DiscoveryCodec;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class PresenceTest {

    @Test
    void announcementJoinsWithCertificateCommonName() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(TestConfigs.presenceEnabled(0))) {
            announce(harness.discoveryPort(), 1L, 200, "producer");
            assertTrue(logs.await(Duration.ofSeconds(2), "event=presence.joined", "name=producer",
                    "intervalMillis=200"), logs.lines()::toString);
            assertEquals(1, harness.presenceCount());
        }
    }

    @Test
    void silentInstanceLeavesAfterThreeIntervals() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(TestConfigs.presenceEnabled(0))) {
            announce(harness.discoveryPort(), 7L, 200, "producer");
            assertTrue(logs.await(Duration.ofSeconds(2), "event=presence.joined", "name=producer"),
                    logs.lines()::toString);
            assertTrue(logs.await(Duration.ofSeconds(2), "event=presence.left", "name=producer"),
                    logs.lines()::toString);
            assertEquals(0, harness.presenceCount());
        }
    }

    @Test
    void secondInstanceStaysListed() throws Exception {
        ScheduledExecutorService refresh = Executors.newSingleThreadScheduledExecutor();
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(TestConfigs.presenceEnabled(0))) {
            refresh.scheduleAtFixedRate(() -> announceQuietly(harness.discoveryPort(), 2L, 200, "second"),
                    0, 200, TimeUnit.MILLISECONDS);
            announce(harness.discoveryPort(), 1L, 200, "first");
            assertTrue(logs.await(Duration.ofSeconds(2), "event=presence.joined", "name=first"),
                    logs.lines()::toString);
            assertTrue(logs.await(Duration.ofSeconds(2), "event=presence.joined", "name=second"),
                    logs.lines()::toString);
            assertTrue(logs.await(Duration.ofSeconds(2), "event=presence.left", "name=first"),
                    logs.lines()::toString);
            assertEquals(0, logs.count("event=presence.left", "name=second"));
            assertEquals(1, harness.presenceCount());
            assertTrue(harness.hasPresence(2L));
        } finally {
            refresh.shutdownNow();
            refresh.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void disabledPresenceIgnoresTypeThreeAndDoesNotStartTheThread() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(TestConfigs.discoveryEnabled(0))) {
            assertFalse(harness.presenceRunning());
            assertTrue(Thread.getAllStackTraces().keySet().stream()
                    .noneMatch(thread -> "dess-presence".equals(thread.getName())));
            announce(harness.discoveryPort(), 1L, 200, "producer");
            assertFalse(logs.await(Duration.ofMillis(200), "event=presence.joined"), logs.lines()::toString);
            assertEquals(0, harness.presenceCount());
        }
    }

    @Test
    void mapDoesNotGrowPastMaxConnections() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(TestConfigs.presence(2, 0))) {
            announce(harness.discoveryPort(), 1L, 5_000, "one");
            announce(harness.discoveryPort(), 2L, 5_000, "two");
            announce(harness.discoveryPort(), 3L, 5_000, "three");
            assertTrue(logs.awaitAtLeast(Duration.ofSeconds(2), 2, "event=presence.joined"),
                    logs.lines()::toString);
            assertEquals(2, harness.presenceCount());
            assertEquals(0, logs.count("event=presence.joined", "name=three"));
            assertFalse(harness.hasPresence(3L));
        }
    }

    private static void announce(int discoveryPort, long instanceId, int intervalMillis, String name)
            throws IOException {
        try (DatagramSocket socket = new DatagramSocket()) {
            byte[] encoded = DiscoveryCodec.presence(instanceId, intervalMillis, name);
            socket.send(new DatagramPacket(
                    encoded, encoded.length, InetAddress.getByName("127.0.0.1"), discoveryPort));
        }
    }

    private static void announceQuietly(int discoveryPort, long instanceId, int intervalMillis, String name) {
        try {
            announce(discoveryPort, instanceId, intervalMillis, name);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
