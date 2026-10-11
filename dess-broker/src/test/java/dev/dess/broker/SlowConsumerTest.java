package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import dev.dess.common.protocol.FrameException;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class SlowConsumerTest {

    @Test
    void fastSubscribersKeepTheirRateBesideASlowReader() throws Exception {
        BrokerConfig config = config(1024, 1024, BrokerConfig.DEFAULT_SLOW_CONSUMER_GRACE_MILLIS);
        long baseline = meanBatchNanos(config, 0L);
        long[] best = {Long.MAX_VALUE};
        boolean passed = false;
        for (int run = 0; run < 3 && !passed; run++) {
            long withSlow = meanBatchNanos(config, Math.max(1L, baseline / 1_000L) * 10L);
            best[0] = Math.min(best[0], withSlow);
            passed = withSlow * 9L <= baseline * 10L;
        }
        assertTrue(passed, () -> "best fast batch " + best[0] + " exceeded 90% of baseline " + baseline);
    }

    @Test
    void stalledSubscriberIsCutOffAndACatchUpStays() throws Exception {
        int grace = 300;
        BrokerConfig limits = config(8, 128, grace);
        cutOff(limits, grace);
        catchUp(limits, grace);
    }

    @Test
    void blockedWriteClosesWithoutAnErrorFrame() throws Exception {
        int grace = 300;
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config(256, 1024, grace));
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient stalled = ProtocolClient.connect(harness.port())) {
            arm(producer, stalled);
            stalled.subscribe("sales");
            logs.awaitAdded(stalled, "sales");
            producer.setReadTimeoutMillis(5_000);
            long started = System.nanoTime();
            byte[] payload = new byte[64 * 1024];
            for (int index = 0; index < 32; index++) {
                producer.publish("sales", payload);
                assertEquals(Command.ACK, producer.readFrame().command());
            }
            assertTrue(logs.await(Duration.ofMillis(grace * 2L + 1_000L), "event=client.disconnected", "reason=SLOW_CONSUMER"));
            long elapsed = System.nanoTime() - started;
            assertTrue(elapsed < Duration.ofMillis(grace * 2L).toNanos(), () -> "closed after " + elapsed);
            stalled.setReadTimeoutMillis(500);
            boolean closed = false;
            try {
                while (true) {
                    Frame frame = stalled.readFrame();
                    assertNotEquals(Command.ERROR, frame.command());
                }
            } catch (EOFException | SocketException ex) {
                closed = true;
            } catch (FrameException ex) {
                if (!"frame is truncated".equals(ex.getMessage())) {
                    throw ex;
                }
                closed = true;
            }
            assertTrue(closed);
        }
    }

    private static void cutOff(BrokerConfig limits, int grace) throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(limits);
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient healthy = ProtocolClient.connect(harness.port());
                ProtocolClient slow = ProtocolClient.connect(harness.port())) {
            arm(producer, healthy, slow);
            healthy.subscribe("sales");
            slow.subscribe("sales");
            logs.awaitAdded(healthy, "sales");
            logs.awaitAdded(slow, "sales");
            producer.publish("sales", new byte[] {1});
            assertEquals(Command.ACK, producer.readFrame().command());
            assertEquals(Command.DELIVER, healthy.readFrame().command());
            assertEquals(Command.DELIVER, slow.readFrame().command());

            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread reader = new Thread(() -> {
                try {
                    healthy.setReadTimeoutMillis(10_000);
                    for (int index = 0; index < 32; index++) {
                        assertEquals(Command.DELIVER, healthy.readFrame().command());
                    }
                } catch (Throwable ex) {
                    failure.set(ex);
                }
            });
            reader.start();
            producer.setReadTimeoutMillis(5_000);
            byte[] payload = new byte[64 * 1024];
            for (int index = 0; index < 32; index++) {
                producer.publish("sales", payload);
                assertEquals(Command.ACK, producer.readFrame().command());
            }
            awaitMillis(grace + 80L);
            slow.setReadTimeoutMillis(2_000);
            boolean sawError = false;
            for (int index = 0; index < 40 && !sawError; index++) {
                Frame frame = slow.readFrame();
                if (frame.command() == Command.ERROR) {
                    assertEquals("SLOW_CONSUMER", FrameCodec.errorCode(frame.header()));
                    sawError = true;
                }
            }
            assertTrue(sawError);
            assertTrue(logs.await(Duration.ofSeconds(2), "event=client.disconnected", "reason=SLOW_CONSUMER"));
            reader.join(10_000);
            assertTrue(failure.get() == null, () -> String.valueOf(failure.get()));
            slow.setReadTimeoutMillis(500);
            assertThrows(IOException.class, slow::readFrame);
        }
    }

    private static void catchUp(BrokerConfig limits, int grace) throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(limits);
                ProtocolClient slow = ProtocolClient.connect(harness.port());
                ProtocolClient producer = ProtocolClient.connect(harness.port())) {
            arm(producer, slow);
            slow.subscribe("sales");
            logs.awaitAdded(slow, "sales");
            producer.setReadTimeoutMillis(5_000);
            producer.publish("sales", new byte[] {1});
            assertEquals(Command.ACK, producer.readFrame().command());
            assertEquals(Command.DELIVER, slow.readFrame().command());
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            try {
                CountDownLatch drain = new CountDownLatch(1);
                scheduler.schedule(drain::countDown, grace / 3L, TimeUnit.MILLISECONDS);
                byte[] payload = new byte[64 * 1024];
                for (int index = 0; index < 32; index++) {
                    producer.publish("sales", payload);
                    assertEquals(Command.ACK, producer.readFrame().command());
                }
                assertTrue(drain.await(2, TimeUnit.SECONDS));
                slow.setReadTimeoutMillis(500);
                while (true) {
                    try {
                        Frame frame = slow.readFrame();
                        assertEquals(Command.DELIVER, frame.command());
                    } catch (SocketTimeoutException ex) {
                        break;
                    }
                }
                awaitMillis(grace + 200L);
                assertEquals(0L, logs.count("reason=SLOW_CONSUMER"));
                slow.setReadTimeoutMillis(1_000);
                slow.ping();
                assertEquals(Command.PONG, slow.readFrame().command());
            } finally {
                scheduler.shutdownNow();
            }
        }
    }

    private static long meanBatchNanos(BrokerConfig config, long slowDelayNanos) throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config);
                ProtocolClient producer = ProtocolClient.connect(harness.port());
                ProtocolClient left = ProtocolClient.connect(harness.port());
                ProtocolClient right = ProtocolClient.connect(harness.port());
                ProtocolClient slow = ProtocolClient.connect(harness.port())) {
            arm(producer, left, right, slow);
            left.subscribe("sales");
            right.subscribe("sales");
            logs.awaitAdded(left, "sales");
            logs.awaitAdded(right, "sales");
            if (slowDelayNanos > 0L) {
                slow.subscribe("sales");
                logs.awaitAdded(slow, "sales");
            }
            logs.quiet();
            producer.setReadTimeoutMillis(5_000);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            long[] finished = new long[2];
            long started = System.nanoTime();
            Thread leftReader = reader(left, finished, 0, failure);
            Thread rightReader = reader(right, finished, 1, failure);
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            leftReader.start();
            rightReader.start();
            if (slowDelayNanos > 0L) {
                readSlowly(scheduler, slow, slowDelayNanos);
            }
            try {
                for (int index = 0; index < 1_000; index++) {
                    producer.publish("sales", new byte[] {(byte) index});
                    assertEquals(Command.ACK, producer.readFrame().command());
                }
                leftReader.join(20_000);
                rightReader.join(20_000);
            } finally {
                scheduler.shutdownNow();
            }
            assertTrue(failure.get() == null, () -> String.valueOf(failure.get()));
            return ((finished[0] - started) + (finished[1] - started)) / 2L;
        }
    }

    private static Thread reader(ProtocolClient client, long[] finished, int slot, AtomicReference<Throwable> failure) {
        return new Thread(() -> {
            try {
                client.setReadTimeoutMillis(10_000);
                for (int index = 0; index < 1_000; index++) {
                    client.readFrame();
                }
                finished[slot] = System.nanoTime();
            } catch (Throwable ex) {
                failure.compareAndSet(null, ex);
            }
        });
    }

    private static void readSlowly(ScheduledExecutorService scheduler, ProtocolClient client, long delayNanos) {
        scheduler.execute(new Runnable() {
            private int seen;

            @Override
            public void run() {
                if (seen >= 1_000) {
                    return;
                }
                try {
                    client.setReadTimeoutMillis(5_000);
                    client.readFrame();
                } catch (IOException ex) {
                    return;
                }
                seen++;
                scheduler.schedule(this, delayNanos, TimeUnit.NANOSECONDS);
            }
        });
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

    private static BrokerConfig config(int outbound, int queue, int grace) {
        return TestConfigs.full(0, 32, BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, 120_000,
                BrokerConfig.DEFAULT_MAX_IN_FLIGHT, queue, outbound, grace,
                BrokerConfig.DEFAULT_PUBLISH_WAIT_MILLIS, BrokerConfig.DEFAULT_ACK_TIMEOUT_MILLIS,
                true, List.of());
    }

    private static void arm(ProtocolClient... clients) throws IOException {
        for (ProtocolClient client : clients) {
            client.setReadTimeoutMillis(5_000);
            client.hello();
        }
    }
}
