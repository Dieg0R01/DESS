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
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
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
                logs.awaitAdded(consumer, "sales");
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
        try (BrokerHarness harness = BrokerHarness.start(TestConfigs.of(0, limit, BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, 120_000))) {
            try {
                for (int index = 0; index < limit; index++) {
                    ProtocolClient client = occupy(harness.port());
                    assertTrue(client != null, "connection " + index + " was not accepted");
                    open.add(client);
                }
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
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
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
            awaitSubscribed(logs, List.of(staying, leaving), "sales");
            leaving.close();
            assertTrue(logs.await(Duration.ofSeconds(2), "event=subscription.removed", "reason=DISCONNECT"), () -> logs.lines().toString());
            byte[] payload = "after".getBytes(StandardCharsets.UTF_8);
            producer.publish("sales", payload);
            assertEquals(Command.ACK, producer.readFrame().command());
            Frame delivered = staying.readFrame();
            assertArrayEquals(payload, delivered.payload());
            staying.setReadTimeoutMillis(200);
            assertThrows(SocketTimeoutException.class, staying::readFrame);
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

    private static void awaitSubscribed(LogCapture logs, List<ProtocolClient> consumers, String topic) throws Exception {
        for (ProtocolClient consumer : consumers) {
            logs.awaitAdded(consumer, topic);
        }
    }

    private static void closeAll(List<ProtocolClient> clients) throws IOException {
        for (ProtocolClient client : clients) {
            client.close();
        }
    }
}
