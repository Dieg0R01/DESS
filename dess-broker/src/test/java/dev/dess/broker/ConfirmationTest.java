package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.AckTimeout;
import dev.dess.common.protocol.BrokerAck;
import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.ConfirmationMode;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class ConfirmationTest {

    @Test
    void allWaitsUntilEverySubscriberAcknowledges() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config(2_000));
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient first = ProtocolClient.connect(harness.port());
                ProtocolClient second = ProtocolClient.connect(harness.port());
                ProtocolClient third = ProtocolClient.connect(harness.port())) {
            arm(producer, first, second, third);
            subscribe(logs, first, second, third);
            CountDownLatch seen = new CountDownLatch(3);
            CountDownLatch early = new CountDownLatch(2);
            CountDownLatch hold = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicReference<Frame> reply = new AtomicReference<>();
            CountDownLatch replied = new CountDownLatch(1);
            acknowledgeAfter(first, seen, null, early, failure);
            acknowledgeAfter(second, seen, null, early, failure);
            acknowledgeAfter(third, seen, hold, null, failure);
            producer.publish("sales", new byte[] {1}, ConfirmationMode.ALL);
            Thread reader = new Thread(() -> readReply(producer, reply, replied, failure), "dess-confirm-reader");
            reader.start();
            assertTrue(seen.await(2, TimeUnit.SECONDS));
            assertTrue(early.await(2, TimeUnit.SECONDS));
            assertFalse(replied.await(400, TimeUnit.MILLISECONDS));
            hold.countDown();
            assertTrue(replied.await(2, TimeUnit.SECONDS));
            assertNull(failure.get());
            BrokerAck ack = producer.brokerAck(reply.get());
            assertEquals(ConfirmationMode.ALL, ack.mode());
            assertEquals(3, ack.acknowledgements());
            assertEquals(0L, ack.offset());
            reader.join(1_000);
        }
    }

    @Test
    void silentSubscriberTimesOutAndLeavesThePublisherOpen() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config(500));
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient first = ProtocolClient.connect(harness.port());
                ProtocolClient second = ProtocolClient.connect(harness.port());
                ProtocolClient silent = ProtocolClient.connect(harness.port())) {
            arm(producer, first, second, silent);
            subscribe(logs, first, second, silent);
            producer.setReadTimeoutMillis(2_000);
            producer.publish("sales", new byte[] {1}, ConfirmationMode.ALL);
            acknowledgeDelivered(first);
            acknowledgeDelivered(second);
            Frame reply = readUntilReply(producer);
            assertEquals(Command.ERROR, reply.command());
            AckTimeout timeout = FrameCodec.ackTimeout(reply.header());
            assertEquals("ACK_TIMEOUT", timeout.code());
            assertEquals(3, timeout.expected());
            assertEquals(2, timeout.received());
            assertTrue(logs.await(Duration.ofSeconds(2), "event=ack.timeout", "expected=3", "received=2"));
            producer.ping();
            assertEquals(Command.PONG, producer.readFrame().command());
        }
    }

    @Test
    void disconnectDuringTheWaitStopsCountingThatSubscriber() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config(2_000));
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient first = ProtocolClient.connect(harness.port());
                ProtocolClient second = ProtocolClient.connect(harness.port());
                ProtocolClient leaving = ProtocolClient.connect(harness.port())) {
            arm(producer, first, second, leaving);
            subscribe(logs, first, second, leaving);
            producer.publish("sales", new byte[] {1}, ConfirmationMode.ALL);
            Frame delivered = readUntil(leaving, Command.DELIVER);
            assertEquals(0L, FrameCodec.delivery(delivered.header()).offset());
            leaving.abort();
            acknowledgeDelivered(first);
            acknowledgeDelivered(second);
            BrokerAck ack = producer.brokerAck(readUntilReply(producer));
            assertEquals(ConfirmationMode.ALL, ack.mode());
            assertEquals(2, ack.acknowledgements());
        }
    }

    @Test
    void noSubscribersAreConfirmedImmediatelyWithZero() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start(config(5_000));
                ProtocolClient producer = ProtocolClient.connect(harness.port())) {
            producer.setReadTimeoutMillis(1_000);
            producer.hello();
            producer.publish("sales", new byte[] {1}, ConfirmationMode.ALL);
            BrokerAck ack = producer.brokerAck(readUntilReply(producer));
            assertEquals(ConfirmationMode.ALL, ack.mode());
            assertEquals(0, ack.acknowledgements());
            assertEquals(0L, ack.offset());
        }
    }

    @Test
    void publisherOnItsOwnTopicIsNotExpected() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config(500));
                ProtocolClient producer = ProtocolClient.connect(harness.port())) {
            producer.setReadTimeoutMillis(2_000);
            producer.hello();
            producer.subscribe("sales");
            logs.awaitAdded(producer, "sales");
            producer.publish("sales", new byte[] {1}, ConfirmationMode.ALL);
            BrokerAck ack = producer.brokerAck(readUntilReply(producer));
            assertEquals(ConfirmationMode.ALL, ack.mode());
            assertEquals(0, ack.acknowledgements());
        }
    }

    @Test
    void unexpectedAcknowledgementWarnsAndStaysOpen() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config(2_000));
                ProtocolClient subscriber = ProtocolClient.connect(harness.port())) {
            subscriber.setReadTimeoutMillis(2_000);
            subscriber.hello();
            subscriber.subscribe("sales");
            logs.awaitAdded(subscriber, "sales");
            String id = clientId(logs, subscriber);
            subscriber.acknowledge("sales", 5);
            subscriber.acknowledge("billing", 0);
            assertTrue(logs.await(Duration.ofSeconds(2), "WARNING", "clientId=" + id, "topic=sales", "offset=5"));
            assertTrue(logs.await(Duration.ofSeconds(2), "WARNING", "clientId=" + id, "topic=billing", "offset=0"));
            assertEquals(2, logs.count("WARNING", "clientId=" + id));
            subscriber.ping();
            assertEquals(Command.PONG, subscriber.readFrame().command());
        }
    }

    @Test
    void repeatedAcknowledgementCountsOnce() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config(2_000));
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient first = ProtocolClient.connect(harness.port());
                ProtocolClient second = ProtocolClient.connect(harness.port())) {
            arm(producer, first, second);
            subscribe(logs, first, second);
            producer.publish("sales", new byte[] {1}, ConfirmationMode.ALL);
            Frame delivered = readUntil(first, Command.DELIVER);
            long offset = FrameCodec.delivery(delivered.header()).offset();
            first.acknowledge("sales", offset);
            first.acknowledge("sales", offset);
            acknowledgeDelivered(second);
            BrokerAck ack = producer.brokerAck(readUntilReply(producer));
            assertEquals(2, ack.acknowledgements());
            assertEquals(ConfirmationMode.ALL, ack.mode());
        }
    }

    @Test
    void acknowledgementsBeforeTheNextPublishStayComplete() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config(2_000));
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient first = ProtocolClient.connect(harness.port());
                ProtocolClient second = ProtocolClient.connect(harness.port());
                ProtocolClient third = ProtocolClient.connect(harness.port())) {
            arm(producer, first, second, third);
            subscribe(logs, first, second, third);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread one = acknowledgeAll(first, 2, failure);
            Thread two = acknowledgeAll(second, 2, failure);
            Thread three = acknowledgeAll(third, 2, failure);
            for (int offset = 0; offset < 2; offset++) {
                producer.publish("sales", new byte[] {(byte) offset}, ConfirmationMode.ALL);
                BrokerAck ack = producer.brokerAck(readUntilReply(producer));
                assertEquals(ConfirmationMode.ALL, ack.mode());
                assertEquals(3, ack.acknowledgements());
                assertEquals(offset, ack.offset());
            }
            one.join(2_000);
            two.join(2_000);
            three.join(2_000);
            assertFalse(one.isAlive());
            assertFalse(two.isAlive());
            assertFalse(three.isAlive());
            assertNull(failure.get());
            assertEquals(0, logs.count("event=ack.timeout"));
        }
    }

    @Test
    void brokerModeReportsZeroAcknowledgements() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config(2_000));
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient subscriber = ProtocolClient.connect(harness.port())) {
            producer.setReadTimeoutMillis(1_000);
            arm(producer, subscriber);
            subscribe(logs, subscriber);
            producer.publish("sales", new byte[] {1});
            BrokerAck ack = producer.brokerAck(readUntilReply(producer));
            assertEquals(ConfirmationMode.BROKER, ack.mode());
            assertEquals(0, ack.acknowledgements());
        }
    }

    private static void acknowledgeAfter(ProtocolClient client, CountDownLatch seen, CountDownLatch hold, CountDownLatch early, AtomicReference<Throwable> failure) {
        Thread thread = new Thread(() -> {
            try {
                Frame delivered = readUntil(client, Command.DELIVER);
                seen.countDown();
                if (hold != null && !hold.await(5, TimeUnit.SECONDS)) {
                    failure.compareAndSet(null, new AssertionError("acknowledgement was not released"));
                    return;
                }
                long offset = FrameCodec.delivery(delivered.header()).offset();
                client.acknowledge("sales", offset);
                if (early != null) {
                    early.countDown();
                }
            } catch (Throwable ex) {
                failure.compareAndSet(null, ex);
            }
        }, "dess-confirm-subscriber");
        thread.start();
    }

    private static Thread acknowledgeAll(ProtocolClient client, int times, AtomicReference<Throwable> failure) {
        Thread thread = new Thread(() -> {
            try {
                for (int index = 0; index < times; index++) {
                    acknowledgeDelivered(client);
                }
            } catch (Throwable ex) {
                failure.compareAndSet(null, ex);
            }
        }, "dess-confirm-ack");
        thread.start();
        return thread;
    }

    private static void acknowledgeDelivered(ProtocolClient client) throws Exception {
        Frame delivered = readUntil(client, Command.DELIVER);
        client.acknowledge("sales", FrameCodec.delivery(delivered.header()).offset());
    }

    private static void readReply(ProtocolClient client, AtomicReference<Frame> reply, CountDownLatch replied, AtomicReference<Throwable> failure) {
        try {
            reply.set(readUntilReply(client));
            replied.countDown();
        } catch (Throwable ex) {
            failure.compareAndSet(null, ex);
            replied.countDown();
        }
    }

    private static Frame readUntilReply(ProtocolClient client) throws Exception {
        return readUntil(client, Command.ACK, Command.ERROR);
    }

    private static Frame readUntil(ProtocolClient client, Command... commands) throws Exception {
        while (true) {
            Frame frame = client.readFrame();
            for (Command command : commands) {
                if (frame.command() == command) {
                    return frame;
                }
            }
        }
    }

    private static void subscribe(LogCapture logs, ProtocolClient... clients) throws Exception {
        for (ProtocolClient client : clients) {
            client.subscribe("sales");
            logs.awaitAdded(client, "sales");
        }
    }

    private static void arm(ProtocolClient... clients) throws Exception {
        for (ProtocolClient client : clients) {
            client.setReadTimeoutMillis(5_000);
            client.hello();
        }
    }

    private static String clientId(LogCapture logs, ProtocolClient client) throws InterruptedException {
        String remote = "remoteAddress=" + client.endpoint();
        assertTrue(logs.await(Duration.ofSeconds(2), "event=client.connected", remote));
        for (String line : logs.lines()) {
            if (line.contains("event=client.connected") && line.contains(remote)) {
                int start = line.indexOf("clientId=") + "clientId=".length();
                int end = line.indexOf(' ', start);
                return line.substring(start, end < 0 ? line.length() : end);
            }
        }
        throw new AssertionError(logs.lines());
    }

    private static BrokerConfig config(int ackTimeoutMillis) {
        return new BrokerConfig(0, 32, BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, 120_000,
                BrokerConfig.DEFAULT_MAX_IN_FLIGHT, 1024, 256, 30_000, 1_000,
                ackTimeoutMillis, true, List.of());
    }
}
