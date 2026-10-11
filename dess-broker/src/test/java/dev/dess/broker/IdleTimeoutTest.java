package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class IdleTimeoutTest {

    @Test
    void pingKeepsTheClientAndSilenceRemovesTheSubscription() throws Exception {
        int timeout = 400;
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(TestConfigs.of(0, 128, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, timeout));
                ProtocolClient publisher = ProtocolClient.connect(harness.port());
                ProtocolClient watcher = ProtocolClient.connect(harness.port());
                ProtocolClient silent = ProtocolClient.connect(harness.port())) {
            publisher.setReadTimeoutMillis(2_000);
            watcher.setReadTimeoutMillis(2_000);
            silent.setReadTimeoutMillis(timeout + 1_000);
            publisher.hello();
            watcher.hello();
            silent.hello();
            watcher.subscribe("sales");
            silent.subscribe("sales");
            logs.awaitAdded(watcher, "sales");
            logs.awaitAdded(silent, "sales");

            AtomicBoolean done = new AtomicBoolean();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread heartbeats = new Thread(() -> {
                try {
                    while (!done.get()) {
                        ping(publisher);
                        ping(watcher);
                    }
                } catch (Throwable ex) {
                    failure.set(ex);
                }
            });
            heartbeats.start();
            long started = System.nanoTime();
            assertThrows(IOException.class, silent::readFrame);
            done.set(true);
            heartbeats.join(2_000);
            assertNull(failure.get());
            assertTrue(Duration.ofNanos(System.nanoTime() - started).compareTo(Duration.ofMillis(timeout + 1_000)) < 0);
            long remaining = Duration.ofMillis(timeout + 1_000).toNanos() - (System.nanoTime() - started);
            assertTrue(logs.await(Duration.ofNanos(Math.max(remaining, 0)), "event=client.disconnected", "reason=TIMEOUT"), () -> logs.lines().toString());
            assertTrue(logs.lines().stream().anyMatch(line -> line.contains("event=subscription.removed") && line.contains("reason=DISCONNECT")));

            ping(publisher);
            byte[] payload = "later".getBytes(StandardCharsets.UTF_8);
            publisher.publish("sales", payload);
            assertEquals(Command.ACK, publisher.readFrame().command());
            Frame delivered = watcher.readFrame();
            assertEquals(Command.DELIVER, delivered.command());
            assertArrayEquals(payload, delivered.payload());
        }
    }

    private static void ping(ProtocolClient client) throws Exception {
        client.ping();
        Frame pong = client.readFrame();
        assertEquals(Command.PONG, pong.command());
        assertEquals(0, pong.header().length);
        assertEquals(0, pong.payload().length);
    }

}
