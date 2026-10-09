package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
class SubscriptionTest {

    @Test
    void unsubscribeStopsDeliveryAndARepeatSubscribeStaysOneCopy() throws Exception {
        List<String> lines = new CopyOnWriteArrayList<>();
        Logger logger = Logger.getLogger("dev.dess.broker");
        Handler handler = capture(lines);
        logger.setLevel(Level.INFO);
        logger.addHandler(handler);
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient consumer = ProtocolClient.connect(harness.port())) {
            producer.setReadTimeoutMillis(2_000);
            consumer.setReadTimeoutMillis(2_000);
            producer.hello();
            consumer.hello();
            consumer.subscribe("sales");
            awaitSubscribed(producer, consumer, "sales");
            assertEquals(1, count(lines, "event=subscription.added", "topic=sales"));

            consumer.subscribe("sales");
            byte[] once = "one".getBytes(StandardCharsets.UTF_8);
            producer.publish("sales", once);
            assertEquals(Command.ACK, producer.readFrame().command());
            Frame delivered = consumer.readFrame();
            assertEquals(Command.DELIVER, delivered.command());
            assertArrayEquals(once, delivered.payload());
            consumer.setReadTimeoutMillis(200);
            assertThrows(SocketTimeoutException.class, consumer::readFrame);
            assertEquals(1, count(lines, "event=subscription.added", "topic=sales"));

            consumer.setReadTimeoutMillis(2_000);
            consumer.unsubscribe("sales");
            assertTrue(waitFor(lines, "event=subscription.removed", "topic=sales", "reason=UNSUBSCRIBE"));
            producer.publish("sales", "gone".getBytes(StandardCharsets.UTF_8));
            assertEquals(Command.ACK, producer.readFrame().command());
            consumer.setReadTimeoutMillis(200);
            assertThrows(SocketTimeoutException.class, consumer::readFrame);

            consumer.setReadTimeoutMillis(2_000);
            consumer.unsubscribe("quiet");
            consumer.setReadTimeoutMillis(200);
            assertThrows(SocketTimeoutException.class, consumer::readFrame);
            consumer.setReadTimeoutMillis(2_000);
            consumer.ping();
            assertEquals(Command.PONG, consumer.readFrame().command());
        } finally {
            logger.removeHandler(handler);
        }
    }

    private static void awaitSubscribed(ProtocolClient producer, ProtocolClient consumer, String topic) throws Exception {
        byte[] ready = "ready".getBytes(StandardCharsets.UTF_8);
        consumer.setReadTimeoutMillis(200);
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            producer.publish(topic, ready);
            assertEquals(Command.ACK, producer.readFrame().command());
            try {
                Frame delivered = consumer.readFrame();
                assertEquals(Command.DELIVER, delivered.command());
                assertArrayEquals(ready, delivered.payload());
                consumer.setReadTimeoutMillis(2_000);
                return;
            } catch (SocketTimeoutException ex) {
                // The subscribe frame has not been registered yet.
            }
        }
        throw new AssertionError("subscriber did not become active");
    }

    private static long count(List<String> lines, String... parts) {
        return lines.stream().filter(line -> containsAll(line, parts)).count();
    }

    private static boolean waitFor(List<String> lines, String... parts) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            if (count(lines, parts) > 0) {
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
