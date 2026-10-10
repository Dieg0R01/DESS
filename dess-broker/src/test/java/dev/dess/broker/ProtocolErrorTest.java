package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class ProtocolErrorTest {

    @Test
    void opaquePayloadsAreDeliveredByteForByte() throws Exception {
        byte[] colon = ":".getBytes(StandardCharsets.UTF_8);
        byte[] newline = "\n".getBytes(StandardCharsets.UTF_8);
        byte[] empty = new byte[0];
        byte[] cafe = "café".getBytes(StandardCharsets.UTF_8);
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient consumer = ProtocolClient.connect(harness.port())) {
            arm(producer, consumer);
            producer.hello();
            consumer.hello();
            consumer.subscribe("sales");
            logs.awaitAdded(consumer, "sales");
            for (byte[] payload : new byte[][] {colon, newline, empty, cafe}) {
                producer.publish("sales", payload);
                Frame ack = producer.readFrame();
                assertEquals(Command.ACK, ack.command());
                assertEquals("sales", FrameCodec.topic(ack.header()));
                assertEquals(0, ack.payload().length);
                Frame delivered = consumer.readFrame();
                assertEquals(Command.DELIVER, delivered.command());
                assertArrayEquals(payload, delivered.payload());
            }
        }
    }

    @Test
    void versionOneHelloIsAnsweredAndASecondHelloCloses() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient client = ProtocolClient.connect(harness.port())) {
            arm(client);
            client.hello();
            client.writeEncoded(FrameCodec.encode(new Frame(Command.HELLO, FrameCodec.helloHeader(1), new byte[0])));
            assertError(client, "MALFORMED_FRAME");
            assertClosed(client);
        }
    }

    @Test
    void unsupportedVersionAndMissingHandshakeClose() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient unsupported = ProtocolClient.connect(harness.port());
                ProtocolClient ping = ProtocolClient.connect(harness.port())) {
            arm(unsupported, ping);
            unsupported.writeEncoded(FrameCodec.encode(new Frame(Command.HELLO, FrameCodec.helloHeader(99), new byte[0])));
            assertError(unsupported, "UNSUPPORTED_VERSION");
            assertClosed(unsupported);
            ping.ping();
            assertError(ping, "HANDSHAKE_REQUIRED");
            assertClosed(ping);
        }
    }

    @Test
    void lengthMismatchClosesAndUnknownCommandStaysOpen() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient client = ProtocolClient.connect(harness.port())) {
            arm(client);
            client.hello();
            client.writeEncoded(rawFrame(8, Command.PING.code(), new byte[0], 0, new byte[] {0}));
            assertError(client, "MALFORMED_FRAME");
            assertClosed(client);
        }
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient client = ProtocolClient.connect(harness.port())) {
            arm(client);
            client.hello();
            client.writeEncoded(rawFrame(7, 0, new byte[0], 0, new byte[0]));
            assertError(client, "UNKNOWN_COMMAND");
            assertStillOpen(client);
        }
    }

    @Test
    void oversizePayloadStaysOpenAndOversizeFrameCloses() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start(new BrokerConfig(0, 128, 4, 120_000));
                ProtocolClient client = ProtocolClient.connect(harness.port())) {
            arm(client);
            client.hello();
            client.publish("sales", new byte[5]);
            assertError(client, "PAYLOAD_TOO_LARGE");
            assertStillOpen(client);
        }
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient client = ProtocolClient.connect(harness.port())) {
            arm(client);
            client.hello();
            client.writeEncoded(lengthPrefix(FrameCodec.MAX_FRAME_LENGTH + 1));
            assertError(client, "PAYLOAD_TOO_LARGE");
            assertClosed(client);
        }
    }

    @Test
    void invalidUtf8ClosesAndABadTopicNameStaysOpen() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient client = ProtocolClient.connect(harness.port())) {
            arm(client);
            client.hello();
            byte[] header = new byte[] {0, 1, (byte) 0xff};
            client.writeEncoded(rawFrame(7 + header.length, Command.PUBLISH.code(), header, 0, new byte[0]));
            assertError(client, "MALFORMED_FRAME");
            assertClosed(client);
        }
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient client = ProtocolClient.connect(harness.port())) {
            arm(client);
            client.hello();
            client.publish("Sales", new byte[] {1});
            assertError(client, "INVALID_TOPIC_NAME");
            assertStillOpen(client);
        }
    }

    @Test
    void clientAckIsIgnored() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient client = ProtocolClient.connect(harness.port())) {
            arm(client);
            client.hello();
            client.writeEncoded(FrameCodec.encode(new Frame(Command.ACK, new byte[0], new byte[0])));
            assertStillOpen(client);
        }
    }

    private static void arm(ProtocolClient... clients) throws IOException {
        for (ProtocolClient client : clients) {
            client.setReadTimeoutMillis(5_000);
        }
    }

    private static void assertError(ProtocolClient client, String code) throws IOException {
        Frame error = client.readFrame();
        assertEquals(Command.ERROR, error.command());
        assertEquals(code, FrameCodec.errorCode(error.header()));
        assertEquals(0, error.payload().length);
    }

    private static void assertClosed(ProtocolClient client) throws IOException {
        client.setReadTimeoutMillis(1_000);
        IOException ex = assertThrows(IOException.class, client::readFrame);
        assertTrue(ex instanceof EOFException || ex instanceof SocketException, ex.toString());
    }

    private static void assertStillOpen(ProtocolClient client) throws IOException {
        client.ping();
        Frame pong = client.readFrame();
        assertEquals(Command.PONG, pong.command());
        assertEquals(0, pong.header().length);
        assertEquals(0, pong.payload().length);
    }

    private static byte[] rawFrame(int frameLength, int command, byte[] header, int payloadLength, byte[] payload) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeInt(frameLength);
        out.writeByte(command);
        out.writeShort(header.length);
        out.write(header);
        out.writeInt(payloadLength);
        out.write(payload);
        return buffer.toByteArray();
    }

    private static byte[] lengthPrefix(int frameLength) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeInt(frameLength);
        return buffer.toByteArray();
    }
}
