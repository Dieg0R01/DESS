package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class BrokerHarnessTest {

    @AfterEach
    void noAcceptThreadRemains() {
        boolean alive = Thread.getAllStackTraces().keySet().stream()
                .anyMatch(thread -> thread.isAlive() && thread.getName().startsWith("dess-accept-"));
        assertFalse(alive);
    }

    @Test
    void startAcceptsAConnectionWithinTwoSeconds() throws IOException {
        long startedAt = System.nanoTime();
        try (BrokerHarness harness = BrokerHarness.start()) {
            Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);
            assertTrue(elapsed.compareTo(Duration.ofSeconds(2)) < 0);
            try (Socket client = new Socket()) {
                client.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), harness.port()), 1000);
                assertTrue(client.isConnected());
            }
        }
    }

    @Test
    void twoHarnessesListenOnDifferentPorts() {
        try (BrokerHarness first = BrokerHarness.start(); BrokerHarness second = BrokerHarness.start()) {
            assertNotEquals(first.port(), second.port());
        }
    }

    @Test
    void closeIsIdempotentAndRejectsNewConnections() {
        BrokerHarness harness = BrokerHarness.start();
        int port = harness.port();
        harness.close();
        harness.close();
        assertThrows(IOException.class, () -> connect(port));
    }

    private static void connect(int port) throws IOException {
        try (Socket client = new Socket()) {
            client.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 500);
        }
    }
}
