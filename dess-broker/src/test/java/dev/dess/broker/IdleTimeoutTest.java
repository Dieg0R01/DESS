package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
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
        List<String> lines = new CopyOnWriteArrayList<>();
        Logger logger = Logger.getLogger("dev.dess.broker");
        Handler handler = capture(lines);
        logger.setLevel(Level.INFO);
        logger.addHandler(handler);
        try (BrokerHarness harness = BrokerHarness.start(new BrokerConfig(0, 128, BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, timeout));
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
            assertTrue(waitFor(lines, started, timeout, "event=client.disconnected", "reason=TIMEOUT"));
            assertTrue(lines.stream().anyMatch(line -> line.contains("event=subscription.removed") && line.contains("reason=DISCONNECT")));

            ping(publisher);
            byte[] payload = "later".getBytes(StandardCharsets.UTF_8);
            publisher.publish("sales", payload);
            assertEquals(Command.ACK, publisher.readFrame().command());
            Frame delivered = watcher.readFrame();
            assertEquals(Command.DELIVER, delivered.command());
            assertArrayEquals(payload, delivered.payload());
        } finally {
            logger.removeHandler(handler);
        }
    }

    private static void ping(ProtocolClient client) throws Exception {
        client.ping();
        Frame pong = client.readFrame();
        assertEquals(Command.PONG, pong.command());
        assertEquals(0, pong.header().length);
        assertEquals(0, pong.payload().length);
    }

    private static boolean waitFor(List<String> lines, long started, int timeout, String... parts) {
        long deadline = started + Duration.ofMillis(timeout + 1_000).toNanos();
        while (System.nanoTime() < deadline) {
            if (lines.stream().anyMatch(line -> containsAll(line, parts))) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsAll(String line, String... parts) {
        for (String part : parts) {
            if (!line.contains(part)) {
                return false;
            }
        }
        return true;
    }

    private static Handler capture(List<String> lines) {
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage() != null) {
                    lines.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        handler.setLevel(Level.ALL);
        return handler;
    }
}
