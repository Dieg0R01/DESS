package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import dev.dess.common.protocol.StartPosition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class FaultInjectionTest {

    private static final int KILL_COUNT = 100;

    private static final int CUT_COUNT = 30;

    private static final int PRODUCER_CUTS = 20;

    @Test
    void threeCasesHoldForEveryRun() throws Exception {
        int runs = Integer.getInteger("dess.fault.runs", 1);
        for (int run = 0; run < runs; run++) {
            brokerKill();
            consumerCut();
            producerCut();
        }
    }

    private static void brokerKill() throws Exception {
        Path directory = Files.createTempDirectory("dess-fault-kill");
        FaultBrokerProcess process = null;
        ProtocolClient producer = null;
        List<Long> acknowledged = new ArrayList<>();
        try {
            process = FaultBrokerProcess.start(directory);
            producer = ProtocolClient.connect(process.port());
            producer.setReadTimeoutMillis(5_000);
            producer.hello();
            for (int index = 0; index < KILL_COUNT; index++) {
                producer.publish("sales", payload("kill", index));
                acknowledged.add(readAck(producer));
            }
            producer.close();
            producer = null;
            Path logsDir = process.logDirectory();
            process.destroyForcibly();
            process.close();
            process = null;

            try (LogCapture logs = LogCapture.open();
                    Broker broker = Broker.start(TestConfigs.durable(logsDir, Durability.ALWAYS, 1_000));
                    ProtocolClient consumer = ProtocolClient.connect(broker.port(), TestPki.consumerStore())) {
                assertTrue(logs.await(Duration.ofSeconds(2), "event=log.recovered"), logs.lines()::toString);
                consumer.setReadTimeoutMillis(5_000);
                consumer.hello();
                consumer.subscribe("sales", StartPosition.earliest());
                logs.awaitAdded(consumer, "sales");
                logs.quiet();
                Set<Long> recovered = new HashSet<>();
                for (int index = 0; index < KILL_COUNT; index++) {
                    Frame delivered = readDeliver(consumer);
                    long offset = FrameCodec.delivery(delivered.header()).offset();
                    recovered.add(offset);
                    consumer.acknowledge("sales", offset);
                }
                int lost = 0;
                for (Long offset : acknowledged) {
                    if (!recovered.contains(offset)) {
                        lost++;
                    }
                }
                assertEquals(0, lost, "lost=" + lost + " acked=" + acknowledged);
                assertEquals(KILL_COUNT, recovered.size());
            }
        } finally {
            closeQuietly(producer);
            if (process != null) {
                process.close();
            }
            delete(directory);
        }
    }

    private static void consumerCut() throws Exception {
        Path directory = Files.createTempDirectory("dess-fault-consumer");
        try (LogCapture logs = LogCapture.open();
                Broker broker = Broker.start(TestConfigs.durable(directory, Durability.ALWAYS, 1_000))) {
            int first = CUT_COUNT / 2;
            List<String> received = new ArrayList<>();
            try (ProtocolClient producer = ProtocolClient.connect(broker.port());
                    ProtocolClient firstConsumer = ProtocolClient.connect(broker.port(), TestPki.consumerStore())) {
                producer.setReadTimeoutMillis(5_000);
                firstConsumer.setReadTimeoutMillis(5_000);
                producer.hello();
                firstConsumer.hello();
                firstConsumer.subscribe("ops", StartPosition.earliest());
                logs.awaitAdded(firstConsumer, "ops");
                for (int index = 0; index < first; index++) {
                    producer.publish("ops", payload("cut", index));
                    readAck(producer);
                    Frame delivered = readDeliver(firstConsumer);
                    received.add(new String(delivered.payload(), StandardCharsets.UTF_8));
                    firstConsumer.acknowledge("ops", FrameCodec.delivery(delivered.header()).offset());
                }
                firstConsumer.abort();
            }
            try (ProtocolClient producer = ProtocolClient.connect(broker.port());
                    ProtocolClient resumed = ProtocolClient.connect(broker.port(), TestPki.consumerStore())) {
                producer.setReadTimeoutMillis(5_000);
                resumed.setReadTimeoutMillis(5_000);
                producer.hello();
                resumed.hello();
                resumed.subscribe("ops", StartPosition.at(first));
                logs.awaitAdded(resumed, "ops");
                for (int index = first; index < CUT_COUNT; index++) {
                    producer.publish("ops", payload("cut", index));
                    readAck(producer);
                    Frame delivered = readDeliver(resumed);
                    received.add(new String(delivered.payload(), StandardCharsets.UTF_8));
                    resumed.acknowledge("ops", FrameCodec.delivery(delivered.header()).offset());
                }
            }
            int duplicated = 0;
            Set<String> unique = new HashSet<>();
            for (String payload : received) {
                if (!unique.add(payload)) {
                    duplicated++;
                }
            }
            assertEquals(0, duplicated, "duplicated=" + duplicated + " received=" + received);
            assertEquals(CUT_COUNT, received.size(), received::toString);
            for (int index = 0; index < CUT_COUNT; index++) {
                assertTrue(unique.contains(text("cut", index)), received::toString);
            }
        } finally {
            delete(directory);
        }
    }

    private static void producerCut() throws Exception {
        Path directory = Files.createTempDirectory("dess-fault-producer");
        int unclear = 0;
        try (LogCapture logs = LogCapture.open();
                Broker broker = Broker.start(TestConfigs.durable(directory, Durability.ALWAYS, 1_000))) {
            logs.quiet();
            for (int index = 0; index < PRODUCER_CUTS; index++) {
                byte[] body = payload("prod", index);
                try (ProtocolClient first = ProtocolClient.connect(broker.port())) {
                    first.setReadTimeoutMillis(2_000);
                    first.hello();
                    first.publish("billing", body);
                    first.abort();
                } catch (IOException ex) {
                    // The cut may land before hello completes; the suite retries below.
                }
                try (ProtocolClient retry = ProtocolClient.connect(broker.port())) {
                    retry.setReadTimeoutMillis(5_000);
                    retry.hello();
                    retry.publish("billing", body);
                    Frame reply = readPublishReply(retry);
                    switch (reply.command()) {
                        case ACK, ERROR -> {
                        }
                        case HELLO, PUBLISH, SUBSCRIBE, UNSUBSCRIBE, DELIVER, PING, PONG -> {
                            unclear++;
                        }
                        default -> {
                            Command unexpected = reply.command();
                            throw new IllegalStateException("Unhandled command " + unexpected);
                        }
                    }
                } catch (IOException ex) {
                    unclear++;
                }
            }
        } finally {
            delete(directory);
        }
        assertEquals(0, unclear, "unclear=" + unclear);
    }

    private static long readAck(ProtocolClient client) throws IOException {
        Frame frame = readPublishReply(client);
        assertEquals(Command.ACK, frame.command(), frame.command()::name);
        return client.brokerAck(frame).offset();
    }

    private static Frame readPublishReply(ProtocolClient client) throws IOException {
        while (true) {
            Frame frame = client.readFrame();
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

    private static Frame readDeliver(ProtocolClient client) throws IOException {
        while (true) {
            Frame frame = client.readFrame();
            switch (frame.command()) {
                case DELIVER -> {
                    return frame;
                }
                case PONG, PING -> {
                }
                case HELLO, PUBLISH, SUBSCRIBE, UNSUBSCRIBE, ACK, ERROR -> {
                    throw new AssertionError("unexpected " + frame.command());
                }
                default -> {
                    Command unexpected = frame.command();
                    throw new IllegalStateException("Unhandled command " + unexpected);
                }
            }
        }
    }

    private static byte[] payload(String prefix, int index) {
        return text(prefix, index).getBytes(StandardCharsets.UTF_8);
    }

    private static String text(String prefix, int index) {
        return prefix + "-" + index;
    }

    private static void closeQuietly(ProtocolClient client) {
        if (client == null) {
            return;
        }
        try {
            client.close();
        } catch (IOException ex) {
            // The broker process is about to be killed.
        }
    }

    private static void delete(Path root) throws IOException {
        if (root == null || Files.notExists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            List<Path> paths = walk.sorted(Comparator.reverseOrder()).toList();
            for (Path path : paths) {
                Files.deleteIfExists(path);
            }
        }
    }
}
