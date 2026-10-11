package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.ConfirmationMode;
import dev.dess.common.protocol.Delivery;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import dev.dess.common.protocol.StartPosition;
import dev.dess.common.security.Digests;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.zip.CRC32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class IntegrityTest {

    @Test
    void publishHeaderWithoutDigestIsMalformedAndCloses() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient client = ProtocolClient.connect(harness.port())) {
            client.setReadTimeoutMillis(2_000);
            client.hello();
            byte[] topic = FrameCodec.topicHeader("sales");
            byte[] header = Arrays.copyOf(topic, topic.length + 1);
            header[topic.length] = (byte) ConfirmationMode.BROKER.wire();
            client.writeEncoded(FrameCodec.encode(new Frame(Command.PUBLISH, header, "x".getBytes(StandardCharsets.UTF_8))));
            Frame error = client.readFrame();
            assertEquals(Command.ERROR, error.command());
            assertEquals("MALFORMED_FRAME", FrameCodec.errorCode(error.header()));
            assertThrows(Exception.class, client::readFrame, "connection should close");
        }
    }

    @Test
    void mismatchedDigestIsRejectedAndNotPersisted() throws Exception {
        BrokerConfig config = TestConfigs.of(0, 32, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 120_000);
        Path topicFile = config.logDirectory().resolve("sales.log");
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config);
                ProtocolClient subscriber = ProtocolClient.connect(harness.port());
                ProtocolClient producer = ProtocolClient.connect(harness.port())) {
            subscriber.setReadTimeoutMillis(2_000);
            producer.setReadTimeoutMillis(2_000);
            subscriber.hello();
            producer.hello();
            subscriber.subscribe("sales");
            logs.awaitAdded(subscriber, "sales");
            producer.publish("sales", "kept".getBytes(StandardCharsets.UTF_8));
            assertEquals(Command.ACK, producer.readFrame().command());
            assertEquals(Command.DELIVER, subscriber.readFrame().command());
            long size = Files.size(topicFile);
            publishDigest(producer, "sales", "tampered".getBytes(StandardCharsets.UTF_8), wrongDigest());
            Frame error = producer.readFrame();
            assertEquals(Command.ERROR, error.command());
            assertEquals("INTEGRITY_FAILED", FrameCodec.errorCode(error.header()));
            assertEquals(size, Files.size(topicFile));
            assertTrue(logs.await(Duration.ofSeconds(2), "event=message.rejected", "errorCode=INTEGRITY_FAILED",
                    "identity=producer"), logs.lines()::toString);
            producer.ping();
            assertEquals(Command.PONG, producer.readFrame().command());
            subscriber.setReadTimeoutMillis(200);
            assertThrows(Exception.class, subscriber::readFrame);
        }
    }

    @Test
    void oneThousandMismatchedDigestsAreRejected() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port())) {
            logs.quiet();
            producer.setReadTimeoutMillis(5_000);
            producer.hello();
            byte[] payload = "x".getBytes(StandardCharsets.UTF_8);
            byte[] digest = wrongDigest();
            for (int index = 0; index < 1_000; index++) {
                publishDigest(producer, "sales", payload, digest);
                Frame error = producer.readFrame();
                assertEquals(Command.ERROR, error.command());
                assertEquals("INTEGRITY_FAILED", FrameCodec.errorCode(error.header()));
            }
            producer.ping();
            assertEquals(Command.PONG, producer.readFrame().command());
        }
    }

    @Test
    void acceptedPublishWritesVersionTwoDigest() throws Exception {
        BrokerConfig config = TestConfigs.of(0, 32, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 120_000);
        Path topicFile = config.logDirectory().resolve("sales.log");
        byte[] payload = "ok".getBytes(StandardCharsets.UTF_8);
        try (BrokerHarness harness = BrokerHarness.start(config);
                ProtocolClient producer = ProtocolClient.connect(harness.port())) {
            producer.setReadTimeoutMillis(2_000);
            producer.hello();
            producer.publish("sales", payload);
            assertEquals(Command.ACK, producer.readFrame().command());
        }
        assertArrayEquals(Digests.of("sales", payload), storedDigest(topicFile));
    }

    @Test
    void earliestDeliversStoredDigest() throws Exception {
        byte[] payload = "replay".getBytes(StandardCharsets.UTF_8);
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port())) {
            producer.setReadTimeoutMillis(2_000);
            producer.hello();
            producer.publish("sales", payload);
            assertEquals(Command.ACK, producer.readFrame().command());
            try (ProtocolClient subscriber = ProtocolClient.connect(harness.port())) {
                subscriber.setReadTimeoutMillis(2_000);
                subscriber.hello();
                subscriber.subscribe("sales", StartPosition.earliest());
                logs.awaitAdded(subscriber, "sales");
                Frame deliver = subscriber.readFrame();
                assertEquals(Command.DELIVER, deliver.command());
                Delivery delivery = FrameCodec.delivery(deliver.header());
                assertArrayEquals(payload, deliver.payload());
                assertArrayEquals(Digests.of("sales", payload), delivery.digest());
                assertTrue(Digests.matches("sales", deliver.payload(), delivery.digest()));
            }
        }
    }

    @Test
    void digestForAnotherTopicIsRejected() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port())) {
            producer.setReadTimeoutMillis(2_000);
            producer.hello();
            byte[] payload = "same".getBytes(StandardCharsets.UTF_8);
            publishDigest(producer, "other", payload, Digests.of("sales", payload));
            Frame error = producer.readFrame();
            assertEquals(Command.ERROR, error.command());
            assertEquals("INTEGRITY_FAILED", FrameCodec.errorCode(error.header()));
            assertFalse(harness.hasTopic("other"));
            assertTrue(logs.await(Duration.ofSeconds(2), "event=message.rejected", "errorCode=INTEGRITY_FAILED"),
                    logs.lines()::toString);
        }
    }

    @Test
    void versionOneRecordReplaysWithComputedDigest() throws Exception {
        BrokerConfig config = TestConfigs.of(0, 32, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 120_000);
        byte[] payload = "legacy".getBytes(StandardCharsets.UTF_8);
        writeVersion1(config.logDirectory().resolve("sales.log"), payload);
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config);
                ProtocolClient subscriber = ProtocolClient.connect(harness.port())) {
            subscriber.setReadTimeoutMillis(2_000);
            subscriber.hello();
            subscriber.subscribe("sales", StartPosition.earliest());
            logs.awaitAdded(subscriber, "sales");
            Frame deliver = subscriber.readFrame();
            assertEquals(Command.DELIVER, deliver.command());
            Delivery delivery = FrameCodec.delivery(deliver.header());
            assertArrayEquals(payload, deliver.payload());
            assertArrayEquals(Digests.of("sales", payload), delivery.digest());
        }
    }

    private static void publishDigest(ProtocolClient client, String topic, byte[] payload, byte[] digest)
            throws IOException {
        client.writeEncoded(FrameCodec.encode(new Frame(
                Command.PUBLISH, FrameCodec.publishHeader(topic, ConfirmationMode.BROKER, digest), payload)));
    }

    private static byte[] wrongDigest() {
        byte[] digest = new byte[Digests.LENGTH];
        Arrays.fill(digest, (byte) 0x11);
        return digest;
    }

    private static byte[] storedDigest(Path file) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(file)).order(ByteOrder.BIG_ENDIAN);
        int bodyLength = buffer.getInt();
        byte[] body = new byte[bodyLength];
        buffer.get(body);
        ByteBuffer view = ByteBuffer.wrap(body).order(ByteOrder.BIG_ENDIAN);
        assertEquals(2, view.get() & 0xff);
        view.getLong();
        view.getLong();
        byte[] digest = new byte[Digests.LENGTH];
        view.get(digest);
        return digest;
    }

    private static void writeVersion1(Path file, byte[] payload) throws IOException {
        int bodyLength = 25 + payload.length;
        ByteBuffer record = ByteBuffer.allocate(4 + bodyLength).order(ByteOrder.BIG_ENDIAN);
        record.putInt(bodyLength);
        int crcStart = record.position();
        record.put((byte) 1);
        record.putLong(0L);
        record.putLong(1L);
        record.putInt(payload.length);
        record.put(payload);
        CRC32 crc = new CRC32();
        crc.update(record.array(), crcStart, record.position() - crcStart);
        record.putInt((int) crc.getValue());
        Files.write(file, record.array());
    }
}
