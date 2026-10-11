package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import javax.net.ssl.SSLSocket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class AuthorizationTest {

    @Test
    void consumerPublishIsUnauthorizedAndLeavesTheConnectionOpen() throws Exception {
        BrokerConfig config = TestConfigs.of(0, 32, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 120_000);
        Path topicFile = config.logDirectory().resolve("sales.log");
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config);
                ProtocolClient subscriber = ProtocolClient.connect(harness.port());
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient consumer = ProtocolClient.connect(harness.port(), TestPki.consumerStore())) {
            subscriber.setReadTimeoutMillis(2_000);
            producer.setReadTimeoutMillis(2_000);
            consumer.setReadTimeoutMillis(2_000);
            subscriber.hello();
            producer.hello();
            consumer.hello();
            subscriber.subscribe("sales");
            logs.awaitAdded(subscriber, "sales");
            producer.publish("sales", "kept".getBytes(StandardCharsets.UTF_8));
            assertEquals(Command.ACK, producer.readFrame().command());
            assertEquals(Command.DELIVER, subscriber.readFrame().command());
            assertTrue(Files.isRegularFile(topicFile));
            long size = Files.size(topicFile);
            consumer.publish("sales", "denied".getBytes(StandardCharsets.UTF_8));
            Frame error = consumer.readFrame();
            assertEquals(Command.ERROR, error.command());
            assertEquals("UNAUTHORIZED", FrameCodec.errorCode(error.header()));
            assertEquals(size, Files.size(topicFile));
            assertTrue(logs.await(Duration.ofSeconds(2), "event=message.rejected", "errorCode=UNAUTHORIZED",
                    "identity=consumer"), logs.lines()::toString);
            subscriber.setReadTimeoutMillis(200);
            try {
                Frame extra = subscriber.readFrame();
                throw new AssertionError("unexpected frame " + extra.command());
            } catch (SocketTimeoutException ex) {
                // No DELIVER for the rejected publish.
            }
            consumer.subscribe("sales");
            logs.awaitAdded(consumer, "sales");
            consumer.ping();
            assertEquals(Command.PONG, consumer.readFrame().command());
        }
    }

    @Test
    void producerPublishIsAcceptedAndLogsIdentity() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port())) {
            producer.setReadTimeoutMillis(2_000);
            producer.hello();
            assertTrue(logs.await(Duration.ofSeconds(2), "event=client.connected", "identity=producer",
                    "roles=producer"), logs.lines()::toString);
            producer.publish("sales", "ok".getBytes(StandardCharsets.UTF_8));
            assertEquals(Command.ACK, producer.readFrame().command());
        }
    }

    @Test
    void producerCertificateMaySubscribeAndReceive() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient subscriber = ProtocolClient.connect(harness.port());
                ProtocolClient publisher = ProtocolClient.connect(harness.port())) {
            subscriber.setReadTimeoutMillis(2_000);
            publisher.setReadTimeoutMillis(2_000);
            subscriber.hello();
            publisher.hello();
            subscriber.subscribe("sales");
            logs.awaitAdded(subscriber, "sales");
            publisher.publish("sales", "hello".getBytes(StandardCharsets.UTF_8));
            assertEquals(Command.ACK, publisher.readFrame().command());
            Frame deliver = subscriber.readFrame();
            assertEquals(Command.DELIVER, deliver.command());
            assertEquals("hello", new String(deliver.payload(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void handshakeWithoutAClientCertificateIsRejected() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                SSLSocket socket = TestPki.anonymousSocket(harness.port())) {
            String remote = socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort();
            try {
                socket.startHandshake();
            } catch (Exception ex) {
                // TLS 1.3 may finish on the client before the broker closes.
            }
            assertTrue(logs.await(Duration.ofSeconds(2), "event=tls.handshake.failed", "reason=NO_CERTIFICATE",
                    "remoteAddress=" + remote), logs.lines()::toString);
            assertEquals(0, logs.count("event=client.connected"));
        }
    }

    @Test
    void unauthorizedPublishDoesNotAutoCreateATopic() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient consumer = ProtocolClient.connect(harness.port(), TestPki.consumerStore())) {
            consumer.setReadTimeoutMillis(2_000);
            consumer.hello();
            consumer.publish("secret", "denied".getBytes(StandardCharsets.UTF_8));
            Frame error = consumer.readFrame();
            assertEquals(Command.ERROR, error.command());
            assertEquals("UNAUTHORIZED", FrameCodec.errorCode(error.header()));
            assertFalse(harness.hasTopic("secret"));
            assertEquals(0L, logs.count("event=topic.created", "topic=secret"));
        }
    }
}
