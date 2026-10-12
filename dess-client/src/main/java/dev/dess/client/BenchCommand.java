package dev.dess.client;

import dev.dess.common.logging.Logging;
import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.ConfirmationMode;
import dev.dess.common.protocol.Delivery;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import dev.dess.common.protocol.StartPosition;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLContext;

final class BenchCommand {

    private BenchCommand() {
    }

    static int execute(ClientRequest request, PrintStream out, PrintStream err) {
        SSLContext producerTls;
        SSLContext consumerTls;
        try {
            producerTls = ClientTls.context(request.identity(), request.trustStore(), request.passwordFile());
            consumerTls = ClientTls.context(request.consumerIdentity(), request.trustStore(), request.passwordFile());
        } catch (IOException ex) {
            return fail(err, ex.getMessage());
        }
        Discovery.Endpoint endpoint;
        try {
            endpoint = Discovery.resolve(request);
        } catch (IOException ex) {
            return fail(err, "no broker found");
        }
        if (endpoint == null) {
            return fail(err, "no broker found");
        }
        int producerCount = request.producers();
        int consumerCount = request.consumers();
        int perProducer = request.messages();
        int total = producerCount * perProducer;
        String topic = request.topics().get(0);
        List<BrokerSession> producers = new ArrayList<>();
        List<BrokerSession> consumers = new ArrayList<>();
        try {
            for (int index = 0; index < producerCount; index++) {
                producers.add(BrokerSession.connect(endpoint.host(), endpoint.port(), request.heartbeatMs(), producerTls));
            }
            for (int index = 0; index < consumerCount; index++) {
                consumers.add(BrokerSession.connect(endpoint.host(), endpoint.port(), request.heartbeatMs(), consumerTls));
            }
        } catch (IOException ex) {
            closeAll(producers);
            closeAll(consumers);
            return fail(err, unreachable(endpoint, ex));
        }
        ConcurrentHashMap<Long, Long> startedAt = new ConcurrentHashMap<>();
        List<Long> latencies = new ArrayList<>();
        Object latencyLock = new Object();
        CountDownLatch unique = new CountDownLatch(total);
        CountDownLatch drained = new CountDownLatch(total * consumerCount);
        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicLong sequences = new AtomicLong();
        List<Thread> workers = new ArrayList<>();
        try {
            for (BrokerSession consumer : consumers) {
                consumer.subscribe(topic, StartPosition.earliest());
                Thread worker = new Thread(() -> consume(consumer, startedAt, latencies, latencyLock, unique, drained, failed),
                        "dess-bench-consumer");
                worker.start();
                workers.add(worker);
            }
            long wallStart = System.nanoTime();
            for (BrokerSession producer : producers) {
                Thread worker = new Thread(() -> produce(producer, topic, request.payloadBytes(), perProducer, sequences, startedAt, failed),
                        "dess-bench-producer");
                worker.start();
                workers.add(worker);
            }
            if (!unique.await(10, TimeUnit.MINUTES) || failed.get()) {
                return fail(err, failed.get() ? "bench failed" : "bench timed out");
            }
            drained.await(1, TimeUnit.MINUTES);
            double seconds = (System.nanoTime() - wallStart) / 1_000_000_000.0;
            if (seconds <= 0) {
                seconds = Double.MIN_VALUE;
            }
            report(out, request, total, total / seconds, latencies);
            return 0;
        } catch (IOException | InterruptedException ex) {
            Thread.currentThread().interrupt();
            return fail(err, "connection closed");
        } finally {
            failed.set(true);
            closeAll(producers);
            closeAll(consumers);
            for (Thread worker : workers) {
                try {
                    worker.join(2_000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private static void produce(BrokerSession session, String topic, int payloadBytes, int count, AtomicLong sequences,
            ConcurrentHashMap<Long, Long> startedAt, AtomicBoolean failed) {
        try {
            for (int index = 0; index < count && !failed.get(); index++) {
                long sequence = sequences.getAndIncrement();
                byte[] payload = new byte[payloadBytes];
                putSequence(payload, sequence);
                startedAt.put(sequence, System.nanoTime());
                session.publish(topic, payload, ConfirmationMode.BROKER);
                Frame reply = awaitReply(session);
                if (reply.command() != Command.ACK) {
                    failed.set(true);
                    return;
                }
            }
        } catch (IOException ex) {
            failed.set(true);
        }
    }

    private static void consume(BrokerSession session, ConcurrentHashMap<Long, Long> startedAt, List<Long> latencies,
            Object latencyLock, CountDownLatch unique, CountDownLatch drained, AtomicBoolean failed) {
        try {
            while (!failed.get() && drained.getCount() > 0) {
                Frame frame = session.readFrame();
                switch (frame.command()) {
                    case DELIVER -> {
                        Delivery delivery = FrameCodec.delivery(frame.header());
                        if (!BrokerSession.intact(delivery, frame.payload())) {
                            failed.set(true);
                            return;
                        }
                        long sequence = getSequence(frame.payload());
                        Long started = startedAt.get(sequence);
                        if (started != null && unique.getCount() > 0) {
                            synchronized (latencyLock) {
                                if (startedAt.remove(sequence) != null) {
                                    latencies.add(System.nanoTime() - started);
                                    unique.countDown();
                                }
                            }
                        }
                        session.acknowledge(delivery.topic(), delivery.offset());
                        drained.countDown();
                    }
                    case PONG, PING -> {
                    }
                    case ERROR -> {
                        failed.set(true);
                        return;
                    }
                    case HELLO, PUBLISH, SUBSCRIBE, UNSUBSCRIBE, ACK -> {
                        failed.set(true);
                        return;
                    }
                    default -> {
                        Command unexpected = frame.command();
                        throw new IllegalStateException("Unhandled command " + unexpected);
                    }
                }
            }
        } catch (IOException ex) {
            if (!failed.get() && drained.getCount() > 0 && unique.getCount() > 0) {
                failed.set(true);
            }
        }
    }

    private static Frame awaitReply(BrokerSession session) throws IOException {
        while (true) {
            Frame frame = session.readFrame();
            switch (frame.command()) {
                case ACK, ERROR -> {
                    return frame;
                }
                case PONG -> {
                }
                case HELLO, PUBLISH, SUBSCRIBE, UNSUBSCRIBE, DELIVER, PING -> {
                    return frame;
                }
                default -> {
                    Command unexpected = frame.command();
                    throw new IllegalStateException("Unhandled command " + unexpected);
                }
            }
        }
    }

    private static void report(PrintStream out, ClientRequest request, int messages, double perSecond, List<Long> latencies) {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        out.println("payloadBytes " + request.payloadBytes());
        out.println("producers " + request.producers());
        out.println("consumers " + request.consumers());
        out.println("messages " + messages);
        out.println("messagesPerSecond " + perSecond);
        out.println("latencyP50Ms " + percentileMs(latencies, 0.50));
        out.println("latencyP99Ms " + percentileMs(latencies, 0.99));
        out.println("os " + System.getProperty("os.name") + " " + System.getProperty("os.version") + " "
                + System.getProperty("os.arch"));
        out.println("processors " + os.getAvailableProcessors());
        out.println("maxMemoryBytes " + Runtime.getRuntime().maxMemory());
        if (os.getSystemLoadAverage() > os.getAvailableProcessors()) {
            out.println("loadWarning true");
        }
    }

    private static double percentileMs(List<Long> nanos, double percentile) {
        if (nanos.isEmpty()) {
            return 0.0;
        }
        long[] sorted = nanos.stream().mapToLong(Long::longValue).sorted().toArray();
        int index = Math.min(sorted.length - 1, Math.max(0, (int) Math.ceil(percentile * sorted.length) - 1));
        return sorted[index] / 1_000_000.0;
    }

    private static void putSequence(byte[] payload, long sequence) {
        int n = Math.min(8, payload.length);
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(sequence);
        byte[] encoded = buffer.array();
        System.arraycopy(encoded, 8 - n, payload, 0, n);
    }

    private static long getSequence(byte[] payload) {
        int n = Math.min(8, payload.length);
        byte[] encoded = new byte[8];
        System.arraycopy(payload, 0, encoded, 8 - n, n);
        return ByteBuffer.wrap(encoded).getLong();
    }

    private static void closeAll(List<BrokerSession> sessions) {
        for (BrokerSession session : sessions) {
            session.close();
        }
    }

    private static String unreachable(Discovery.Endpoint endpoint, IOException ex) {
        String message = ex.getMessage();
        if (ClientTls.isHandshakeFailure(message)) {
            return message;
        }
        String target = endpoint.host() + ":" + endpoint.port();
        if (message != null && message.contains(target)) {
            return message;
        }
        return "broker unreachable at " + target;
    }

    private static int fail(PrintStream err, String message) {
        err.println(Logging.format("ERROR", "dev.dess.client", message));
        return 1;
    }
}
