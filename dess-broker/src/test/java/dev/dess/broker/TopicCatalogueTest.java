package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
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
class TopicCatalogueTest {

    @Test
    void oneConnectionReceivesBothTopicsInOffsetOrder() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient subscriber = ProtocolClient.connect(harness.port());
                ProtocolClient publisher = ProtocolClient.connect(harness.port())) {
            subscriber.setReadTimeoutMillis(2_000);
            publisher.setReadTimeoutMillis(2_000);
            subscriber.hello();
            publisher.hello();
            subscriber.subscribe("a");
            subscriber.subscribe("b");
            logs.awaitAdded(subscriber, "a");
            logs.awaitAdded(subscriber, "b");
            publish(publisher, "a", "a0");
            publish(publisher, "a", "a1");
            publish(publisher, "b", "b0");
            publish(publisher, "b", "b1");
            publish(publisher, "a", "a2");
            List<Frame> deliveries = new ArrayList<>();
            while (deliveries.size() < 5) {
                Frame frame = subscriber.readFrame();
                if (frame.command() == Command.DELIVER) {
                    deliveries.add(frame);
                }
            }
            List<Long> offsetsA = new ArrayList<>();
            List<String> payloadsA = new ArrayList<>();
            List<Long> offsetsB = new ArrayList<>();
            List<String> payloadsB = new ArrayList<>();
            for (Frame frame : deliveries) {
                String topic = FrameCodec.delivery(frame.header()).topic();
                long offset = FrameCodec.delivery(frame.header()).offset();
                String text = new String(frame.payload(), StandardCharsets.UTF_8);
                if ("a".equals(topic)) {
                    offsetsA.add(offset);
                    payloadsA.add(text);
                } else if ("b".equals(topic)) {
                    offsetsB.add(offset);
                    payloadsB.add(text);
                }
            }
            assertEquals(List.of(0L, 1L, 2L), offsetsA);
            assertEquals(List.of("a0", "a1", "a2"), payloadsA);
            assertEquals(List.of(0L, 1L), offsetsB);
            assertEquals(List.of("b0", "b1"), payloadsB);
        }
    }

    @Test
    void firstUseLogsTopicCreatedOnce() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient publisher = ProtocolClient.connect(harness.port());
                ProtocolClient subscriber = ProtocolClient.connect(harness.port())) {
            publisher.setReadTimeoutMillis(2_000);
            subscriber.setReadTimeoutMillis(2_000);
            publisher.hello();
            subscriber.hello();
            publish(publisher, "alpha", "one");
            subscriber.subscribe("beta");
            assertTrue(logs.await(Duration.ofSeconds(2), "event=topic.created", "topic=alpha", "source=AUTO"));
            assertTrue(logs.await(Duration.ofSeconds(2), "event=topic.created", "topic=beta", "source=AUTO"));
            publish(publisher, "alpha", "two");
            subscriber.subscribe("beta");
            subscriber.ping();
            assertEquals(Command.PONG, readUntil(subscriber, Command.PONG).command());
            assertEquals(1L, logs.count("event=topic.created", "topic=alpha", "source=AUTO"));
            assertEquals(1L, logs.count("event=topic.created", "topic=beta", "source=AUTO"));
        }
    }

    @Test
    void configuredTopicRejectsUnknownNames() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(configured());
                ProtocolClient client = ProtocolClient.connect(harness.port())) {
            client.setReadTimeoutMillis(2_000);
            client.hello();
            assertTrue(createdBeforeStart(logs));
            assertEquals(1L, logs.count("event=topic.created", "topic=sales", "source=CONFIG"));
            client.publish("other", new byte[] {1});
            assertEquals("UNKNOWN_TOPIC", FrameCodec.errorCode(readUntil(client, Command.ERROR).header()));
            client.subscribe("other");
            assertEquals("UNKNOWN_TOPIC", FrameCodec.errorCode(readUntil(client, Command.ERROR).header()));
            assertEquals(2L, logs.count("event=message.rejected", "errorCode=UNKNOWN_TOPIC", "topic=other"));
            client.ping();
            assertEquals(Command.PONG, readUntil(client, Command.PONG).command());
            publish(client, "sales", "kept");
            client.unsubscribe("other");
            client.ping();
            assertEquals(Command.PONG, readUntil(client, Command.PONG).command());
            assertFalse(harness.hasTopic("other"));
            assertEquals(0L, logs.count("event=topic.created", "topic=other"));
        }
    }

    private static boolean createdBeforeStart(LogCapture logs) throws InterruptedException {
        assertTrue(logs.await(Duration.ofSeconds(2), "event=topic.created", "topic=sales", "source=CONFIG"));
        assertTrue(logs.await(Duration.ofSeconds(2), "event=broker.started"));
        int createdAt = -1;
        int startedAt = -1;
        int index = 0;
        for (String line : logs.lines()) {
            if (createdAt < 0 && line.contains("event=topic.created") && line.contains("topic=sales") && line.contains("source=CONFIG")) {
                createdAt = index;
            }
            if (startedAt < 0 && line.contains("event=broker.started")) {
                startedAt = index;
            }
            index++;
        }
        return createdAt >= 0 && startedAt >= 0 && createdAt < startedAt;
    }

    private static void publish(ProtocolClient client, String topic, String text) throws Exception {
        client.publish(topic, text.getBytes(StandardCharsets.UTF_8));
        assertEquals(Command.ACK, readUntil(client, Command.ACK).command());
    }

    private static Frame readUntil(ProtocolClient client, Command command) throws Exception {
        while (true) {
            Frame frame = client.readFrame();
            if (frame.command() == command) {
                return frame;
            }
        }
    }

    private static BrokerConfig configured() {
        return new BrokerConfig(0, 16, BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, 120_000,
                BrokerConfig.DEFAULT_MAX_IN_FLIGHT, 1024, 256, 30_000, 1_000,
                BrokerConfig.DEFAULT_ACK_TIMEOUT_MILLIS, false, List.of("sales"));
    }
}
