package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
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
class ConnectionIsolationTest {

    @Test
    void newHandshakeFinishesWithinOneSecondAmongFiftyIdleClients() throws Exception {
        List<ProtocolClient> idle = new ArrayList<>();
        try (BrokerHarness harness = BrokerHarness.start()) {
            try {
                for (int index = 0; index < 50; index++) {
                    ProtocolClient client = ProtocolClient.connect(harness.port());
                    idle.add(client);
                    client.setReadTimeoutMillis(2_000);
                    client.hello();
                }
                long started = System.nanoTime();
                try (ProtocolClient newbie = ProtocolClient.connect(harness.port())) {
                    newbie.setReadTimeoutMillis(2_000);
                    newbie.hello();
                }
                Duration elapsed = Duration.ofNanos(System.nanoTime() - started);
                assertTrue(elapsed.compareTo(Duration.ofSeconds(1)) < 0, elapsed::toString);
            } finally {
                closeAll(idle);
            }
        }
    }

    @Test
    void partialFrameDoesNotBlockAnotherPublish() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start();
                Socket stuck = new Socket()) {
            stuck.setTcpNoDelay(true);
            stuck.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), harness.port()), 2_000);
            stuck.getOutputStream().write(new byte[] {0, 0, 0, 100});
            stuck.getOutputStream().flush();
            try (ProtocolClient producer = ProtocolClient.connect(harness.port());
                    ProtocolClient consumer = ProtocolClient.connect(harness.port())) {
                producer.setReadTimeoutMillis(2_000);
                consumer.setReadTimeoutMillis(2_000);
                producer.hello();
                consumer.hello();
                consumer.subscribe("sales");
                awaitSubscribed(producer, consumer, "sales");
                byte[] payload = "kept".getBytes(StandardCharsets.UTF_8);
                producer.publish("sales", payload);
                Frame ack = producer.readFrame();
                assertEquals(Command.ACK, ack.command());
                Frame delivered = consumer.readFrame();
                assertEquals(Command.DELIVER, delivered.command());
                assertArrayEquals(payload, delivered.payload());
            }
        }
    }

    @Test
    void connectionBeyondTheLimitReceivesBrokerBusyAndCloses() throws Exception {
        int limit = 3;
        List<ProtocolClient> open = new ArrayList<>();
        try (BrokerHarness harness = BrokerHarness.start(new BrokerConfig(0, limit, BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, 120_000))) {
            try {
                long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
                while (open.size() < limit && System.nanoTime() < deadline) {
                    ProtocolClient client = occupy(harness.port());
                    if (client != null) {
                        open.add(client);
                    }
                }
                assertEquals(limit, open.size());
                try (ProtocolClient extra = ProtocolClient.connect(harness.port())) {
                    extra.setReadTimeoutMillis(2_000);
                    Frame error = extra.readFrame();
                    assertEquals(Command.ERROR, error.command());
                    assertEquals("BROKER_BUSY", FrameCodec.errorCode(error.header()));
                    assertClosed(extra);
                }
            } finally {
                closeAll(open);
            }
        }
    }

    @Test
    void closingASubscriberDropsItBeforeTheNextPublish() throws Exception {
        List<String> lines = new CopyOnWriteArrayList<>();
        Logger logger = Logger.getLogger("dev.dess.broker");
        Handler handler = capture(lines);
        logger.addHandler(handler);
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient staying = ProtocolClient.connect(harness.port());
                ProtocolClient leaving = ProtocolClient.connect(harness.port())) {
            producer.setReadTimeoutMillis(2_000);
            staying.setReadTimeoutMillis(2_000);
            leaving.setReadTimeoutMillis(2_000);
            producer.hello();
            staying.hello();
            leaving.hello();
            staying.subscribe("sales");
            leaving.subscribe("sales");
            awaitSubscribed(producer, List.of(staying, leaving), "sales");
            leaving.close();
            assertTrue(waitFor(lines, "event=subscription.removed", "reason=DISCONNECT"));
            byte[] payload = "after".getBytes(StandardCharsets.UTF_8);
            producer.publish("sales", payload);
            assertEquals(Command.ACK, producer.readFrame().command());
            Frame delivered = staying.readFrame();
            assertArrayEquals(payload, delivered.payload());
            staying.setReadTimeoutMillis(200);
            assertThrows(SocketTimeoutException.class, staying::readFrame);
        } finally {
            logger.removeHandler(handler);
        }
    }

    private static ProtocolClient occupy(int port) throws IOException {
        ProtocolClient client = ProtocolClient.connect(port);
        try {
            client.setReadTimeoutMillis(1_000);
            client.writeEncoded(FrameCodec.encode(new Frame(Command.HELLO, FrameCodec.helloHeader(FrameCodec.PROTOCOL_VERSION), new byte[0])));
            Frame reply = client.readFrame();
            if (reply.command() == Command.ERROR) {
                client.close();
                return null;
            }
            assertEquals(Command.HELLO, reply.command());
            assertEquals(FrameCodec.PROTOCOL_VERSION, FrameCodec.helloVersion(reply.header()));
            return client;
        } catch (IOException ex) {
            client.close();
            return null;
        }
    }

    private static void assertClosed(ProtocolClient client) throws IOException {
        client.setReadTimeoutMillis(1_000);
        IOException ex = assertThrows(IOException.class, client::readFrame);
        assertTrue(ex instanceof EOFException || ex instanceof SocketException, ex.toString());
    }

    private static void awaitSubscribed(ProtocolClient producer, ProtocolClient consumer, String topic) throws Exception {
        awaitSubscribed(producer, List.of(consumer), topic);
    }

    private static void awaitSubscribed(ProtocolClient producer, List<ProtocolClient> consumers, String topic) throws Exception {
        byte[] ready = ("ready-" + topic).getBytes(StandardCharsets.UTF_8);
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            producer.publish(topic, ready);
            assertEquals(Command.ACK, producer.readFrame().command());
            int seen = 0;
            for (ProtocolClient consumer : consumers) {
                consumer.setReadTimeoutMillis(200);
                try {
                    Frame delivered = consumer.readFrame();
                    if (new String(delivered.payload(), StandardCharsets.UTF_8).startsWith("ready-")) {
                        seen++;
                    }
                } catch (SocketTimeoutException ex) {
                    // This subscriber has not registered yet.
                }
                consumer.setReadTimeoutMillis(2_000);
            }
            if (seen == consumers.size()) {
                return;
            }
        }
        throw new AssertionError("subscribers did not become active");
    }

    private static boolean waitFor(List<String> lines, String... parts) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
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

    private static void closeAll(List<ProtocolClient> clients) throws IOException {
        for (ProtocolClient client : clients) {
            client.close();
        }
    }
}
