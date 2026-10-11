package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Delivery;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class StressTest {

    private static final int PRODUCERS = 10;

    private static final int CONSUMERS = 10;

    private static final int TOPICS = 5;

    private static final int MESSAGES = 10_000;

    /** Pool keep-alive is 1s; joining allows that plus 2s. */
    private static final long THREAD_EXIT_MILLIS = 1_000L + 2_000L;

    @Test
    void repeatedRunsLoseNothing() throws Exception {
        int runs = Integer.getInteger("dess.stress.runs", 1);
        for (int run = 1; run <= runs; run++) {
            runOnce(run);
        }
    }

    private static void runOnce(int run) throws Exception {
        AtomicInteger lost = new AtomicInteger();
        AtomicInteger duplicated = new AtomicInteger();
        AtomicInteger crossTopic = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<ProtocolClient> clients = new ArrayList<>();
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(config())) {
            List<ProtocolClient> producers = connect(harness, PRODUCERS, clients);
            List<ProtocolClient> consumers = connect(harness, CONSUMERS, clients);
            List<List<String>> subscriptions = new ArrayList<>();
            for (int index = 0; index < CONSUMERS; index++) {
                List<String> topics = topicsFor(index);
                subscriptions.add(topics);
                for (String topic : topics) {
                    consumers.get(index).subscribe(topic);
                    logs.awaitAdded(consumers.get(index), topic);
                }
            }
            logs.quiet();
            List<Thread> threads = new ArrayList<>();
            for (int index = 0; index < CONSUMERS; index++) {
                ProtocolClient client = consumers.get(index);
                List<String> topics = subscriptions.get(index);
                threads.add(start("stress-consumer-" + index, () -> consume(client, topics, lost, duplicated, crossTopic, failure)));
            }
            for (int index = 0; index < PRODUCERS; index++) {
                ProtocolClient client = producers.get(index);
                int producer = index;
                threads.add(start("stress-producer-" + index, () -> produce(client, producer, failure)));
            }
            for (Thread thread : threads) {
                thread.join(180_000L);
                assertFalse(thread.isAlive(), thread.getName());
            }
            System.out.println("stress run " + run + " lost=" + lost.get() + " duplicated=" + duplicated.get() + " crossTopic=" + crossTopic.get());
            assertNull(failure.get(), () -> String.valueOf(failure.get()));
            assertEquals(0, lost.get(), "Topic lost a delivery");
            assertEquals(0, duplicated.get(), "Topic duplicated a delivery");
            assertEquals(0, crossTopic.get(), "ClientConnection misrouted a delivery");
            closeAll(clients);
            assertWorkersReleased(harness.port());
        } finally {
            closeAll(clients);
        }
    }

    private static void produce(ProtocolClient client, int producer, AtomicReference<Throwable> failure) {
        try {
            int sequence = 0;
            while (sequence < MESSAGES && failure.get() == null) {
                String topic = "t" + (sequence % TOPICS);
                client.publish(topic, payload(producer, sequence, topic));
                Frame reply = readUntil(client, Command.ACK, Command.ERROR);
                if (reply.command() == Command.ERROR) {
                    String code = FrameCodec.errorCode(reply.header());
                    if (!"BROKER_BUSY".equals(code)) {
                        failure.compareAndSet(null, new AssertionError("Broker rejected a publish with " + code));
                        return;
                    }
                    continue;
                }
                sequence++;
            }
        } catch (Throwable ex) {
            failure.compareAndSet(null, ex);
        }
    }

    private static void consume(ProtocolClient client, List<String> topics, AtomicInteger lost, AtomicInteger duplicated, AtomicInteger crossTopic, AtomicReference<Throwable> failure) {
        try {
            int[][] next = new int[TOPICS][PRODUCERS];
            for (int topic = 0; topic < TOPICS; topic++) {
                Arrays.fill(next[topic], topic);
            }
            int expected = topics.size() * PRODUCERS * (MESSAGES / TOPICS);
            int received = 0;
            while (received < expected && failure.get() == null) {
                Frame frame = client.readFrame();
                if (frame.command() != Command.DELIVER) {
                    failure.compareAndSet(null, new AssertionError("ClientConnection received " + frame.command()));
                    return;
                }
                Delivery delivery = FrameCodec.delivery(frame.header());
                Parsed parsed = parse(frame.payload());
                if (!delivery.topic().equals(parsed.topic()) || !topics.contains(delivery.topic())) {
                    crossTopic.incrementAndGet();
                    failure.compareAndSet(null, new AssertionError("ClientConnection cross-topic delivery frame " + delivery.topic() + " payload " + parsed.topic()));
                    return;
                }
                int topic = delivery.topic().charAt(1) - '0';
                int expectedSequence = next[topic][parsed.producer()];
                if (parsed.sequence() != expectedSequence) {
                    if (parsed.sequence() < expectedSequence) {
                        duplicated.incrementAndGet();
                        failure.compareAndSet(null, new AssertionError("Topic duplicated producer " + parsed.producer() + " " + delivery.topic() + " sequence " + parsed.sequence()));
                    } else {
                        lost.addAndGet((parsed.sequence() - expectedSequence) / TOPICS);
                        failure.compareAndSet(null, new AssertionError("Topic reordered producer " + parsed.producer() + " " + delivery.topic() + " expected " + expectedSequence + " got " + parsed.sequence()));
                    }
                    return;
                }
                next[topic][parsed.producer()] = parsed.sequence() + TOPICS;
                client.acknowledge(delivery.topic(), delivery.offset());
                received++;
            }
            for (String topicName : topics) {
                int topic = topicName.charAt(1) - '0';
                for (int producer = 0; producer < PRODUCERS; producer++) {
                    if (next[topic][producer] != topic + MESSAGES) {
                        lost.incrementAndGet();
                        failure.compareAndSet(null, new AssertionError("Topic lost producer " + producer + " " + topicName + " at " + next[topic][producer]));
                        return;
                    }
                }
            }
        } catch (Throwable ex) {
            lost.incrementAndGet();
            failure.compareAndSet(null, ex);
        }
    }

    private static void assertWorkersReleased(int port) throws InterruptedException {
        List<Thread> workers = new ArrayList<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            String name = thread.getName();
            if (name.startsWith("dess-client-") || name.startsWith("dess-writer-")) {
                workers.add(thread);
            }
        }
        for (Thread thread : workers) {
            thread.join(THREAD_EXIT_MILLIS);
            assertFalse(thread.isAlive(), "Broker thread " + thread.getName() + " did not exit");
        }
        List<String> remaining = new ArrayList<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            String name = thread.getName();
            if (thread.isAlive() && name.startsWith("dess-")) {
                remaining.add(name);
            }
        }
        remaining.sort(null);
        assertEquals(List.of("dess-accept-" + port, "dess-flow-timer", "dess-log-sync"), remaining);
    }

    private static List<ProtocolClient> connect(BrokerHarness harness, int count, List<ProtocolClient> clients) throws IOException {
        List<ProtocolClient> connected = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            ProtocolClient client = ProtocolClient.connect(harness.port());
            client.setReadTimeoutMillis(60_000);
            client.hello();
            connected.add(client);
            clients.add(client);
        }
        return connected;
    }

    private static List<String> topicsFor(int consumer) {
        return List.of("t" + (consumer % TOPICS), "t" + ((consumer + 1) % TOPICS));
    }

    private static byte[] payload(int producer, int sequence, String topic) {
        return (producer + " " + sequence + " " + topic).getBytes(StandardCharsets.UTF_8);
    }

    private static Parsed parse(byte[] payload) {
        String text = new String(payload, StandardCharsets.UTF_8);
        int first = text.indexOf(' ');
        int second = text.indexOf(' ', first + 1);
        return new Parsed(Integer.parseInt(text.substring(0, first)), Integer.parseInt(text.substring(first + 1, second)), text.substring(second + 1));
    }

    private static Frame readUntil(ProtocolClient client, Command... commands) throws IOException {
        while (true) {
            Frame frame = client.readFrame();
            for (Command command : commands) {
                if (frame.command() == command) {
                    return frame;
                }
            }
        }
    }

    private static Thread start(String name, Runnable task) {
        Thread thread = new Thread(task, name);
        thread.start();
        return thread;
    }

    private static void closeAll(List<ProtocolClient> clients) {
        for (ProtocolClient client : clients) {
            try {
                client.close();
            } catch (IOException ex) {
                // The peer is already gone.
            }
        }
    }

    private static BrokerConfig config() {
        return TestConfigs.full(0, 64, BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, 120_000,
                100_000, 8_192, 8_192, 120_000, 30_000,
                BrokerConfig.DEFAULT_ACK_TIMEOUT_MILLIS, true, List.of());
    }

    private record Parsed(int producer, int sequence, String topic) {
    }
}
