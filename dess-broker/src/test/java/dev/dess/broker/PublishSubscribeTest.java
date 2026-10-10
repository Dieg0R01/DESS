package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class PublishSubscribeTest {

    @Test
    void subscriberReceivesOneHundredRoundsInOrder() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient consumer = ProtocolClient.connect(harness.port())) {
            producer.setReadTimeoutMillis(5_000);
            consumer.setReadTimeoutMillis(5_000);
            producer.hello();
            consumer.hello();
            consumer.subscribe("sales");
            logs.awaitAdded(consumer, "sales");
            logs.quiet();

            for (int round = 0; round < 100; round++) {
                for (int index = 0; index < 1_000; index++) {
                    byte[] payload = payload(round, index);
                    producer.publish("sales", payload);
                    Frame ack = producer.readFrame();
                    assertEquals(Command.ACK, ack.command());
                    assertEquals("sales", FrameCodec.topic(ack.header()));
                    assertEquals(0, ack.payload().length);

                    Frame delivered = consumer.readFrame();
                    assertEquals(Command.DELIVER, delivered.command());
                    assertEquals("sales", FrameCodec.topic(delivered.header()));
                    assertArrayEquals(payload, delivered.payload());
                }
            }

            consumer.setReadTimeoutMillis(200);
            assertThrows(SocketTimeoutException.class, consumer::readFrame);
        }
    }

    @Test
    void invalidTopicIsRejectedWithoutAnAck() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port())) {
            producer.setReadTimeoutMillis(5_000);
            producer.hello();
            producer.publish("Sales", "nope".getBytes(StandardCharsets.UTF_8));
            Frame error = producer.readFrame();
            assertEquals(Command.ERROR, error.command());
            assertEquals("INVALID_TOPIC_NAME", FrameCodec.errorCode(error.header()));
            assertEquals(0, error.payload().length);
            producer.setReadTimeoutMillis(200);
            assertThrows(SocketTimeoutException.class, producer::readFrame);
        }
    }

    private static byte[] payload(int round, int index) {
        return (round + ":" + index).getBytes(StandardCharsets.UTF_8);
    }
}
