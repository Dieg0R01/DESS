package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class DecoupledDeliveryTest {

    @Test
    void acknowledgementsArriveWhileTheSubscriberNeverReads() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient subscriber = ProtocolClient.connect(harness.port())) {
            arm(producer, subscriber);
            subscriber.subscribe("sales");
            logs.awaitAdded(subscriber, "sales");
            producer.setReadTimeoutMillis(1_000);
            long started = System.nanoTime();
            for (int index = 0; index < 100; index++) {
                producer.publish("sales", new byte[] {(byte) index});
                Frame ack = producer.readFrame();
                assertEquals(Command.ACK, ack.command());
                assertEquals(index, producer.brokerAck(ack).offset());
            }
            assertTrue(System.nanoTime() - started < Duration.ofSeconds(1).toNanos());
        }
    }

    @Test
    void subscriberReceivesFiveThousandMessagesInOffsetOrder() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient subscriber = ProtocolClient.connect(harness.port())) {
            arm(producer, subscriber);
            subscriber.subscribe("sales");
            logs.awaitAdded(subscriber, "sales");
            logs.quiet();
            producer.setReadTimeoutMillis(5_000);
            subscriber.setReadTimeoutMillis(10_000);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread reader = new Thread(() -> {
                try {
                    for (int index = 0; index < 5_000; index++) {
                        Frame delivered = subscriber.readFrame();
                        assertEquals(index, subscriber.deliverOffset(delivered));
                        assertEquals(index, ByteBuffer.wrap(delivered.payload()).getLong());
                    }
                } catch (Throwable ex) {
                    failure.set(ex);
                }
            });
            reader.start();
            try {
                for (int index = 0; index < 5_000; index++) {
                    producer.publish("sales", payload(index));
                    Frame ack = producer.readFrame();
                    assertEquals(Command.ACK, ack.command());
                    assertEquals(index, producer.brokerAck(ack).offset());
                }
            } finally {
                reader.join(30_000);
            }
            assertTrue(failure.get() == null, () -> String.valueOf(failure.get()));
        }
    }

    @Test
    void subscriberAddedWhilePublishingReceivesAContiguousRun() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient subscriber = ProtocolClient.connect(harness.port())) {
            arm(producer, subscriber);
            producer.setReadTimeoutMillis(5_000);
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch subscribed = new CountDownLatch(1);
            AtomicInteger acknowledged = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread publishing = new Thread(() -> {
                try {
                    int index = 0;
                    int afterSubscribe = 0;
                    while (afterSubscribe < 50) {
                        producer.publish("sales", payload(index));
                        Frame ack = producer.readFrame();
                        assertEquals(index, producer.brokerAck(ack).offset());
                        index++;
                        acknowledged.incrementAndGet();
                        if (index == 20) {
                            started.countDown();
                        }
                        if (subscribed.getCount() == 0) {
                            afterSubscribe++;
                        }
                    }
                } catch (Throwable ex) {
                    failure.set(ex);
                    started.countDown();
                }
            });
            publishing.start();
            assertTrue(started.await(5, TimeUnit.SECONDS));
            long seen = acknowledged.get();
            subscriber.subscribe("sales");
            logs.awaitAdded(subscriber, "sales");
            subscribed.countDown();
            publishing.join(10_000);
            assertTrue(failure.get() == null, () -> String.valueOf(failure.get()));
            subscriber.setReadTimeoutMillis(1_000);
            List<Long> offsets = new ArrayList<>();
            while (true) {
                Frame delivered;
                try {
                    delivered = subscriber.readFrame();
                } catch (SocketTimeoutException ex) {
                    break;
                }
                long offset = subscriber.deliverOffset(delivered);
                assertEquals(offset, ByteBuffer.wrap(delivered.payload()).getLong());
                offsets.add(offset);
            }
            assertTrue(offsets.size() > 1, () -> offsets.toString());
            for (int index = 1; index < offsets.size(); index++) {
                assertEquals(offsets.get(index - 1) + 1, offsets.get(index));
            }
            assertTrue(offsets.get(0) >= seen, () -> offsets.get(0) + " followed acknowledgements " + seen);
        }
    }

    @Test
    void twoProducersKeepRelativeOrderAtTwoSubscribers() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient first = ProtocolClient.connect(harness.port());
                ProtocolClient second = ProtocolClient.connect(harness.port());
                ProtocolClient left = ProtocolClient.connect(harness.port());
                ProtocolClient right = ProtocolClient.connect(harness.port())) {
            arm(first, second, left, right);
            left.subscribe("sales");
            right.subscribe("sales");
            logs.awaitAdded(left, "sales");
            logs.awaitAdded(right, "sales");
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread leftPublisher = publisher(first, "A", failure);
            Thread rightPublisher = publisher(second, "B", failure);
            leftPublisher.start();
            rightPublisher.start();
            List<String> leftMessages = readPayloads(left, 60);
            List<String> rightMessages = readPayloads(right, 60);
            leftPublisher.join(5_000);
            rightPublisher.join(5_000);
            assertTrue(failure.get() == null, () -> String.valueOf(failure.get()));
            assertRelativeOrder(leftMessages);
            assertRelativeOrder(rightMessages);
        }
    }

    private static Thread publisher(ProtocolClient client, String prefix, AtomicReference<Throwable> failure) {
        return new Thread(() -> {
            try {
                for (int index = 0; index < 30; index++) {
                    client.publish("sales", (prefix + "-" + index).getBytes(StandardCharsets.UTF_8));
                    Frame ack = client.readFrame();
                    assertEquals(Command.ACK, ack.command());
                }
            } catch (Throwable ex) {
                failure.compareAndSet(null, ex);
            }
        });
    }

    private static List<String> readPayloads(ProtocolClient client, int count) throws Exception {
        client.setReadTimeoutMillis(5_000);
        List<String> payloads = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            payloads.add(new String(client.readFrame().payload(), StandardCharsets.UTF_8));
        }
        return payloads;
    }

    private static void assertRelativeOrder(List<String> payloads) {
        assertEquals(30, payloads.stream().filter(payload -> payload.startsWith("A-")).count());
        assertEquals(30, payloads.stream().filter(payload -> payload.startsWith("B-")).count());
        assertIncreasing(payloads, "A-");
        assertIncreasing(payloads, "B-");
    }

    private static void assertIncreasing(List<String> payloads, String prefix) {
        int previous = -1;
        for (String payload : payloads) {
            if (!payload.startsWith(prefix)) {
                continue;
            }
            int index = Integer.parseInt(payload.substring(prefix.length()));
            assertTrue(index > previous);
            previous = index;
        }
    }

    private static byte[] payload(long offset) {
        return ByteBuffer.allocate(8).putLong(offset).array();
    }

    private static void arm(ProtocolClient... clients) throws Exception {
        for (ProtocolClient client : clients) {
            client.setReadTimeoutMillis(5_000);
            client.hello();
        }
    }
}
