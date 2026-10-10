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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class SubscriptionTest {

    @Test
    void unsubscribeStopsDeliveryAndARepeatSubscribeStaysOneCopy() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient consumer = ProtocolClient.connect(harness.port())) {
            producer.setReadTimeoutMillis(2_000);
            consumer.setReadTimeoutMillis(2_000);
            producer.hello();
            consumer.hello();
            consumer.subscribe("sales");
            logs.awaitAdded(consumer, "sales");
            assertEquals(1, logs.count("event=subscription.added", "topic=sales"));

            consumer.subscribe("sales");
            byte[] once = "one".getBytes(StandardCharsets.UTF_8);
            producer.publish("sales", once);
            assertEquals(Command.ACK, producer.readFrame().command());
            Frame delivered = consumer.readFrame();
            assertEquals(Command.DELIVER, delivered.command());
            assertArrayEquals(once, delivered.payload());
            consumer.setReadTimeoutMillis(200);
            assertThrows(SocketTimeoutException.class, consumer::readFrame);
            assertEquals(1, logs.count("event=subscription.added", "topic=sales"));

            consumer.setReadTimeoutMillis(2_000);
            consumer.unsubscribe("sales");
            assertTrue(logs.await(Duration.ofSeconds(2), "event=subscription.removed", "topic=sales", "reason=UNSUBSCRIBE"));
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
        }
    }
}
