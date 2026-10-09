package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class FanoutTest {

    @Test
    void threeSubscribersEachReceiveOneCopy() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient first = ProtocolClient.connect(harness.port());
                ProtocolClient second = ProtocolClient.connect(harness.port());
                ProtocolClient third = ProtocolClient.connect(harness.port())) {
            arm(producer, first, second, third);
            first.subscribe("sales");
            second.subscribe("sales");
            third.subscribe("sales");
            awaitSubscribed(producer, List.of(first, second, third), "sales");

            byte[] payload = "one".getBytes(StandardCharsets.UTF_8);
            producer.publish("sales", payload);
            assertAck(producer, "sales");
            for (ProtocolClient consumer : List.of(first, second, third)) {
                Frame delivered = consumer.readFrame();
                assertEquals(Command.DELIVER, delivered.command());
                assertArrayEquals(payload, delivered.payload());
                consumer.setReadTimeoutMillis(200);
                assertThrows(SocketTimeoutException.class, consumer::readFrame);
            }
        }
    }

    @Test
    void otherTopicsAndLateSubscribersDoNotSeeThePublish() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient topicA = ProtocolClient.connect(harness.port());
                ProtocolClient topicB = ProtocolClient.connect(harness.port());
                ProtocolClient late = ProtocolClient.connect(harness.port())) {
            arm(producer, topicA, topicB, late);
            topicA.subscribe("a");
            topicB.subscribe("b");
            awaitSubscribed(producer, List.of(topicA), "a");
            awaitSubscribed(producer, List.of(topicB), "b");

            byte[] forB = "only-b".getBytes(StandardCharsets.UTF_8);
            producer.publish("b", forB);
            assertAck(producer, "b");
            Frame delivered = topicB.readFrame();
            assertArrayEquals(forB, delivered.payload());
            topicA.setReadTimeoutMillis(200);
            assertThrows(SocketTimeoutException.class, topicA::readFrame);

            byte[] discarded = "gone".getBytes(StandardCharsets.UTF_8);
            producer.publish("quiet", discarded);
            assertAck(producer, "quiet");
            late.subscribe("quiet");
            late.setReadTimeoutMillis(200);
            assertThrows(SocketTimeoutException.class, late::readFrame);

            byte[] after = "after".getBytes(StandardCharsets.UTF_8);
            producer.publish("quiet", after);
            assertAck(producer, "quiet");
            late.setReadTimeoutMillis(5_000);
            Frame lateDelivery = late.readFrame();
            assertArrayEquals(after, lateDelivery.payload());
        }
    }

    @Test
    void failedSubscriberIsClosedAndTheOtherStillReceives() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient healthy = ProtocolClient.connect(harness.port())) {
            ClientConnection.forceSendBuffer(1024);
            try {
                try (ProtocolClient failing = ProtocolClient.connect(harness.port())) {
                failing.setReadTimeoutMillis(5_000);
                failing.hello();
                ClientConnection.forceSendBuffer(0);
                arm(producer, healthy);
                healthy.subscribe("sales");
                failing.subscribe("sales");
                awaitSubscribed(producer, List.of(healthy, failing), "sales");
                failing.shrinkReceiveBuffer(1024);

                byte[] payload = new byte[512 * 1024];
                payload[0] = 7;
                AtomicReference<Frame> delivered = new AtomicReference<>();
                AtomicReference<Throwable> readerFailure = new AtomicReference<>();
                Thread reader = new Thread(() -> {
                    try {
                        healthy.setReadTimeoutMillis(5_000);
                        delivered.set(healthy.readFrame());
                    } catch (Throwable ex) {
                        readerFailure.set(ex);
                    }
                });
                reader.start();
                try {
                    producer.publish("sales", payload);
                    producer.setReadTimeoutMillis(300);
                    try {
                        producer.readFrame();
                        throw new AssertionError("delivery did not block on the stalled subscriber");
                    } catch (SocketTimeoutException blocked) {
                        failing.abort();
                        producer.setReadTimeoutMillis(5_000);
                        assertAck(producer, "sales");
                    }
                } finally {
                    reader.join(5_000);
                }
                assertTrue(readerFailure.get() == null, () -> String.valueOf(readerFailure.get()));
                Frame frame = delivered.get();
                assertEquals(Command.DELIVER, frame.command());
                assertArrayEquals(payload, frame.payload());
                producer.setReadTimeoutMillis(200);
                assertThrows(SocketTimeoutException.class, producer::readFrame);
                }
            } finally {
                ClientConnection.forceSendBuffer(0);
            }
        }
    }

    @Test
    void concurrentProducersKeepTheirOwnOrder() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start();
                ProtocolClient first = ProtocolClient.connect(harness.port());
                ProtocolClient second = ProtocolClient.connect(harness.port());
                ProtocolClient consumer = ProtocolClient.connect(harness.port())) {
            arm(first, second, consumer);
            consumer.subscribe("sales");
            awaitSubscribed(first, List.of(consumer), "sales");

            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread left = publisher(first, "A", 30, failure);
            Thread right = publisher(second, "B", 30, failure);
            left.start();
            right.start();
            List<String> received = new ArrayList<>();
            consumer.setReadTimeoutMillis(5_000);
            for (int index = 0; index < 60; index++) {
                received.add(new String(consumer.readFrame().payload(), StandardCharsets.UTF_8));
            }
            left.join(5_000);
            right.join(5_000);
            assertTrue(failure.get() == null, () -> String.valueOf(failure.get()));
            assertEquals(30, received.stream().filter(payload -> payload.startsWith("A-")).count());
            assertEquals(30, received.stream().filter(payload -> payload.startsWith("B-")).count());
            assertInRelativeOrder(received, "A-");
            assertInRelativeOrder(received, "B-");
        }
    }

    private static Thread publisher(ProtocolClient client, String prefix, int count, AtomicReference<Throwable> failure) {
        return new Thread(() -> {
            try {
                for (int index = 0; index < count; index++) {
                    client.publish("sales", (prefix + "-" + index).getBytes(StandardCharsets.UTF_8));
                    assertAck(client, "sales");
                }
            } catch (Throwable ex) {
                failure.compareAndSet(null, ex);
            }
        });
    }

    private static void arm(ProtocolClient... clients) throws Exception {
        for (ProtocolClient client : clients) {
            client.setReadTimeoutMillis(5_000);
            client.hello();
        }
    }

    private static void awaitSubscribed(ProtocolClient producer, List<ProtocolClient> consumers, String topic) throws Exception {
        byte[] ready = ("ready-" + topic).getBytes(StandardCharsets.UTF_8);
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < deadline) {
            producer.publish(topic, ready);
            assertAck(producer, topic);
            int seen = 0;
            for (ProtocolClient consumer : consumers) {
                consumer.setReadTimeoutMillis(200);
                try {
                    while (true) {
                        Frame delivered = consumer.readFrame();
                        if (new String(delivered.payload(), StandardCharsets.UTF_8).startsWith("ready-")) {
                            seen++;
                            break;
                        }
                    }
                } catch (SocketTimeoutException ex) {
                    // This subscriber has not registered yet.
                }
                consumer.setReadTimeoutMillis(5_000);
            }
            if (seen == consumers.size()) {
                return;
            }
        }
        throw new AssertionError("subscribers did not become active");
    }

    private static void assertAck(ProtocolClient producer, String topic) throws Exception {
        Frame ack = producer.readFrame();
        assertEquals(Command.ACK, ack.command());
        assertEquals(topic, FrameCodec.topic(ack.header()));
    }

    private static void assertInRelativeOrder(List<String> received, String prefix) {
        int previous = -1;
        for (String payload : received) {
            if (!payload.startsWith(prefix)) {
                continue;
            }
            int index = Integer.parseInt(payload.substring(prefix.length()));
            assertTrue(index > previous);
            previous = index;
        }
        assertEquals(29, previous);
    }
}
