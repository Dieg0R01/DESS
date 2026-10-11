package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class FlowControlTest {

    private static final int MEGABYTE = 1024 * 1024;

    @Test
    void floodStaysWithinTheInFlightLimit() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config(10, 1024, 1024, 30_000, 300));
                ProtocolClient stalled = ProtocolClient.connect(harness.port());
                ProtocolClient first = ProtocolClient.connect(harness.port());
                ProtocolClient second = ProtocolClient.connect(harness.port());
                ProtocolClient third = ProtocolClient.connect(harness.port());
                ProtocolClient fourth = ProtocolClient.connect(harness.port())) {
            arm(stalled, first, second, third, fourth);
            stalled.subscribe("sales");
            logs.awaitAdded(stalled, "sales");
            CountDownLatch stop = new CountDownLatch(1);
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            AtomicInteger busy = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread[] producers = new Thread[4];
            ProtocolClient[] clients = {first, second, third, fourth};
            try {
                for (int index = 0; index < clients.length; index++) {
                    ProtocolClient client = clients[index];
                    producers[index] = new Thread(() -> publishUntil(client, stop, busy, failure));
                    producers[index].start();
                }
                scheduler.schedule(stop::countDown, 2, TimeUnit.SECONDS);
                assertTrue(stop.await(5, TimeUnit.SECONDS));
                for (Thread producer : producers) {
                    producer.join(10_000);
                }
            } finally {
                scheduler.shutdownNow();
            }
            assertTrue(failure.get() == null, () -> String.valueOf(failure.get()));
            assertTrue(busy.get() > 0);
            assertEquals(10, harness.inFlightPeak());
            assertEquals(busy.get(), logs.count("event=message.rejected", "errorCode=BROKER_BUSY"));
            assertTrue(logs.count("event=flow.throttled", "outcome=BUSY", "inFlight=", "limit=", "waitedMs=") >= busy.get());
        }
    }

    @Test
    void publishPastTheLimitWaitsThenRejects() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config(10, 1024, 1024, 30_000, 300));
                ProtocolClient stalled = ProtocolClient.connect(harness.port());
                ProtocolClient producer = ProtocolClient.connect(harness.port())) {
            arm(stalled, producer);
            stalled.subscribe("sales");
            logs.awaitAdded(stalled, "sales");
            int guard = 0;
            while (harness.inFlight() < 10) {
                producer.publish("sales", payload());
                assertEquals(Command.ACK, producer.readFrame().command());
                assertTrue(++guard < 30, () -> "in flight stayed at " + harness.inFlight());
            }
            assertEquals(10, harness.inFlight());
            producer.setReadTimeoutMillis(2_000);
            long started = System.nanoTime();
            producer.publish("sales", payload());
            Frame reply = producer.readFrame();
            long elapsed = System.nanoTime() - started;
            assertEquals(Command.ERROR, reply.command());
            assertEquals("BROKER_BUSY", FrameCodec.errorCode(reply.header()));
            assertTrue(elapsed >= Duration.ofMillis(300).toNanos(), () -> "rejected after " + elapsed);
            producer.ping();
            assertEquals(Command.PONG, producer.readFrame().command());
            assertEquals(1L, logs.count("event=message.rejected", "errorCode=BROKER_BUSY"));
            assertTrue(logs.await(Duration.ofSeconds(2), "event=flow.throttled", "outcome=BUSY", "inFlight=", "limit=", "waitedMs="));
        }
    }

    @Test
    void waitingPublishIsAcceptedWhenTheReaderCatchesUp() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config(1, 64, 64, 30_000, 2_000, 8 * MEGABYTE));
                ProtocolClient stalled = ProtocolClient.connect(harness.port());
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient waiter = ProtocolClient.connect(harness.port())) {
            arm(stalled, producer, waiter);
            stalled.subscribe("sales");
            logs.awaitAdded(stalled, "sales");
            producer.publish("sales", stall());
            assertEquals(Command.ACK, producer.readFrame().command());
            assertEquals(1, harness.inFlight());
            AtomicReference<Frame> reply = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread publishing = new Thread(() -> {
                try {
                    waiter.setReadTimeoutMillis(5_000);
                    waiter.publish("sales", stall());
                    reply.set(waiter.readFrame());
                } catch (Throwable ex) {
                    failure.set(ex);
                }
            });
            publishing.start();
            awaitMillis(200);
            assertEquals(1, harness.inFlight());
            stalled.setReadTimeoutMillis(5_000);
            assertEquals(Command.DELIVER, stalled.readFrame().command());
            publishing.join(5_000);
            assertTrue(failure.get() == null, () -> String.valueOf(failure.get()));
            assertEquals(Command.ACK, reply.get().command());
            assertTrue(logs.await(Duration.ofSeconds(2), "event=flow.throttled", "outcome=ACCEPTED", "inFlight=", "limit=", "waitedMs="));
            String line = logs.lines().stream().filter(item -> item.contains("outcome=ACCEPTED")).findFirst().orElseThrow();
            int at = line.indexOf("waitedMs=");
            int end = line.indexOf(' ', at);
            int waitedMs = Integer.parseInt(line.substring(at + "waitedMs=".length(), end));
            assertTrue(waitedMs > 0, () -> line);
        }
    }

    @Test
    void fullTopicQueueThrottlesThePublish() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config(10_000, 4, 4, 30_000, 300, 8 * MEGABYTE));
                ProtocolClient stalled = ProtocolClient.connect(harness.port());
                ProtocolClient producer = ProtocolClient.connect(harness.port())) {
            arm(stalled, producer);
            stalled.subscribe("sales");
            logs.awaitAdded(stalled, "sales");
            producer.setReadTimeoutMillis(2_000);
            for (int index = 0; index < 4; index++) {
                producer.publish("sales", stall());
                assertEquals(Command.ACK, producer.readFrame().command());
            }
            long started = System.nanoTime();
            producer.publish("sales", stall());
            Frame reply = producer.readFrame();
            long elapsed = System.nanoTime() - started;
            assertEquals(Command.ERROR, reply.command());
            assertEquals("BROKER_BUSY", FrameCodec.errorCode(reply.header()));
            assertTrue(elapsed >= Duration.ofMillis(300).toNanos(), () -> "rejected after " + elapsed);
            assertEquals(1L, logs.count("event=message.rejected", "errorCode=BROKER_BUSY"));
            assertTrue(logs.await(Duration.ofSeconds(2), "event=flow.throttled", "outcome=BUSY", "inFlight=", "limit=", "waitedMs="));
        }
    }

    @Test
    void shutdownCountsMessagesStillInFlight() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config(10, 1024, 1024, 30_000, 300));
                ProtocolClient stalled = ProtocolClient.connect(harness.port());
                ProtocolClient producer = ProtocolClient.connect(harness.port())) {
            arm(stalled, producer);
            stalled.subscribe("sales");
            logs.awaitAdded(stalled, "sales");
            producer.publish("sales", payload());
            assertEquals(Command.ACK, producer.readFrame().command());
            assertEquals(1, harness.inFlight());
            harness.close();
            assertTrue(logs.await(Duration.ofSeconds(2), "event=broker.stopped", "inFlightLost=1"));
        }
    }

    private static void publishUntil(ProtocolClient client, CountDownLatch stop, AtomicInteger busy, AtomicReference<Throwable> failure) {
        try {
            client.setReadTimeoutMillis(5_000);
            byte[] body = payload();
            while (stop.getCount() > 0) {
                client.publish("sales", body);
                Frame reply = client.readFrame();
                if (reply.command() == Command.ERROR) {
                    assertEquals("BROKER_BUSY", FrameCodec.errorCode(reply.header()));
                    busy.incrementAndGet();
                }
            }
        } catch (Throwable ex) {
            failure.compareAndSet(null, ex);
        }
    }

    private static byte[] payload() {
        return new byte[MEGABYTE];
    }

    private static byte[] stall() {
        return new byte[4 * MEGABYTE];
    }

    private static void awaitMillis(long millis) throws InterruptedException {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            CountDownLatch latch = new CountDownLatch(1);
            scheduler.schedule(latch::countDown, millis, TimeUnit.MILLISECONDS);
            assertTrue(latch.await(millis + 1_000L, TimeUnit.MILLISECONDS));
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static BrokerConfig config(int maxInFlight, int queue, int outbound, int grace, int publishWait) {
        return config(maxInFlight, queue, outbound, grace, publishWait, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES);
    }

    private static BrokerConfig config(int maxInFlight, int queue, int outbound, int grace, int publishWait, int maxPayload) {
        return TestConfigs.full(0, 32, maxPayload, 120_000,
                maxInFlight, queue, outbound, grace, publishWait,
                TestConfigs.DEFAULT_ACK_TIMEOUT_MILLIS, true, List.of());
    }

    private static void arm(ProtocolClient... clients) throws IOException {
        for (ProtocolClient client : clients) {
            client.setReadTimeoutMillis(5_000);
            client.hello();
        }
    }
}
