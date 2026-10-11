package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.BrokerAck;
import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.ConfirmationMode;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class ProtocolVersionTest {

    @Test
    void versionOneHelloIsRejectedAndOffsetsStayWithTheTopic() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient rejected = ProtocolClient.connect(harness.port());
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient consumer = ProtocolClient.connect(harness.port())) {
            rejected.setReadTimeoutMillis(2_000);
            rejected.writeEncoded(FrameCodec.encode(new Frame(Command.HELLO, FrameCodec.helloHeader(1), new byte[0])));
            Frame error = rejected.readFrame();
            assertEquals(Command.ERROR, error.command());
            assertEquals("UNSUPPORTED_VERSION", FrameCodec.errorCode(error.header()));
            assertClosed(rejected);

            try (ProtocolClient versionTwo = ProtocolClient.connect(harness.port())) {
                versionTwo.setReadTimeoutMillis(2_000);
                versionTwo.writeEncoded(FrameCodec.encode(new Frame(Command.HELLO, FrameCodec.helloHeader(2), new byte[0])));
                Frame versionTwoError = versionTwo.readFrame();
                assertEquals(Command.ERROR, versionTwoError.command());
                assertEquals("UNSUPPORTED_VERSION", FrameCodec.errorCode(versionTwoError.header()));
                assertClosed(versionTwo);
            }

            producer.setReadTimeoutMillis(2_000);
            consumer.setReadTimeoutMillis(2_000);
            producer.hello();
            consumer.hello();
            consumer.subscribe("sales");
            logs.awaitAdded(consumer, "sales");
            byte[] payload = "one".getBytes(StandardCharsets.UTF_8);
            for (int offset = 0; offset < 3; offset++) {
                producer.publish("sales", payload);
                Frame ack = producer.readFrame();
                BrokerAck view = producer.brokerAck(ack);
                assertEquals(ConfirmationMode.BROKER, view.mode());
                assertEquals(0, view.acknowledgements());
                assertEquals(offset, view.offset());
                Frame delivered = consumer.readFrame();
                assertEquals(offset, producer.deliverOffset(delivered));
                assertTrue(logs.await(Duration.ofSeconds(2), "event=message.accepted", "topic=sales", "offset=" + offset));
            }
            producer.publish("stock", payload);
            assertEquals(0, producer.brokerAck(producer.readFrame()).offset());

            consumer.unsubscribe("sales");
            assertTrue(logs.await(Duration.ofSeconds(2), "event=subscription.removed", "topic=sales", "reason=UNSUBSCRIBE"));
            long added = logs.count("event=subscription.added", "topic=sales");
            consumer.subscribe("sales");
            assertTrue(logs.awaitAtLeast(Duration.ofSeconds(2), added + 1, "event=subscription.added", "topic=sales"));
            producer.publish("sales", payload);
            assertEquals(3, producer.brokerAck(producer.readFrame()).offset());
            assertEquals(3, producer.deliverOffset(consumer.readFrame()));
        }
    }

    @Test
    void emptyClientAckAndUnknownModeClose() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient client = ProtocolClient.connect(harness.port())) {
            client.setReadTimeoutMillis(2_000);
            client.hello();
            client.writeEncoded(FrameCodec.encode(new Frame(Command.ACK, new byte[0], new byte[0])));
            Frame error = client.readFrame();
            assertEquals("MALFORMED_FRAME", FrameCodec.errorCode(error.header()));
            assertClosed(client);
        }
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient client = ProtocolClient.connect(harness.port())) {
            client.setReadTimeoutMillis(2_000);
            client.hello();
            byte[] topic = FrameCodec.topicHeader("sales");
            byte[] header = Arrays.copyOf(topic, topic.length + 1);
            header[header.length - 1] = 2;
            client.writeEncoded(FrameCodec.encode(new Frame(Command.PUBLISH, header, new byte[] {1})));
            Frame error = client.readFrame();
            assertEquals("MALFORMED_FRAME", FrameCodec.errorCode(error.header()));
            assertClosed(client);
        }
    }

    private static void assertClosed(ProtocolClient client) throws IOException {
        client.setReadTimeoutMillis(1_000);
        IOException ex = assertThrows(IOException.class, client::readFrame);
        assertTrue(ex instanceof EOFException || ex instanceof SocketException, ex.toString());
    }
}
