package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import dev.dess.common.protocol.StartPosition;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class ReplayTest {

    @Test
    void earliestReplaysHistoryThenStaysContiguous() throws Exception {
        try (Broker broker = startReplay();
                ProtocolClient publisher = ProtocolClient.connect(broker.port())) {
            publisher.setReadTimeoutMillis(5_000);
            publisher.hello();
            try (LogCapture quiet = LogCapture.open()) {
                quiet.quiet();
                publishAll(publisher, 500);
            }
            try (LogCapture logs = LogCapture.open();
                    ProtocolClient subscriber = ProtocolClient.connect(broker.port())) {
                subscriber.setReadTimeoutMillis(15_000);
                subscriber.hello();
                subscriber.subscribe("sales", StartPosition.earliest());
                assertTrue(logs.await(Duration.ofSeconds(2), "event=subscription.added", "startPosition=EARLIEST"), logs.lines()::toString);
                List<Received> history = readDeliveries(subscriber, 500);
                for (int index = 0; index < history.size(); index++) {
                    assertEquals(index, history.get(index).offset());
                    assertEquals(payloadText(index), history.get(index).text());
                }
                publisher.publish("sales", "live".getBytes(StandardCharsets.UTF_8));
                assertEquals(500L, readAck(publisher));
                Received live = readDeliveries(subscriber, 1).get(0);
                assertEquals(500L, live.offset());
                assertEquals("live", live.text());
            }
        }
    }

    @Test
    void offsetPastTheEndKeepsTheConnectionOpen() throws Exception {
        try (Broker broker = startReplay();
                LogCapture logs = LogCapture.open();
                ProtocolClient client = ProtocolClient.connect(broker.port())) {
            client.setReadTimeoutMillis(5_000);
            client.hello();
            client.publish("sales", payload(0));
            assertEquals(0L, readAck(client));
            client.subscribe("sales", StartPosition.at(2));
            Frame error = client.readFrame();
            assertEquals(Command.ERROR, error.command());
            assertEquals("INVALID_OFFSET", FrameCodec.errorCode(error.header()));
            assertTrue(logs.await(Duration.ofSeconds(2), "event=message.rejected", "errorCode=INVALID_OFFSET"), logs.lines()::toString);
            client.ping();
            assertEquals(Command.PONG, client.readFrame().command());
            client.publish("sales", payload(1));
            assertEquals(1L, readAck(client));
        }
    }

    @Test
    void offsetAtTheEndDeliversOnlyLaterPublishes() throws Exception {
        try (Broker broker = startReplay();
                LogCapture logs = LogCapture.open();
                ProtocolClient publisher = ProtocolClient.connect(broker.port());
                ProtocolClient subscriber = ProtocolClient.connect(broker.port())) {
            publisher.setReadTimeoutMillis(5_000);
            publisher.hello();
            publishAll(publisher, 2);
            subscriber.hello();
            subscriber.subscribe("sales", StartPosition.at(2));
            assertTrue(logs.await(Duration.ofSeconds(2), "event=subscription.added", "startPosition=2"), logs.lines()::toString);
            subscriber.setReadTimeoutMillis(300);
            assertThrows(SocketTimeoutException.class, subscriber::readFrame);
            subscriber.setReadTimeoutMillis(5_000);
            publisher.publish("sales", payload(2));
            assertEquals(2L, readAck(publisher));
            Received delivered = readDeliveries(subscriber, 1).get(0);
            assertEquals(2L, delivered.offset());
            assertEquals(payloadText(2), delivered.text());
        }
    }

    @Test
    void latestSkipsRetainedHistory() throws Exception {
        try (Broker broker = startReplay();
                ProtocolClient publisher = ProtocolClient.connect(broker.port())) {
            publisher.setReadTimeoutMillis(5_000);
            publisher.hello();
            try (LogCapture quiet = LogCapture.open()) {
                quiet.quiet();
                publishAll(publisher, 500);
            }
            try (LogCapture logs = LogCapture.open();
                    ProtocolClient subscriber = ProtocolClient.connect(broker.port())) {
                subscriber.hello();
                subscriber.subscribe("sales");
                assertTrue(logs.await(Duration.ofSeconds(2), "event=subscription.added", "startPosition=LATEST"), logs.lines()::toString);
                subscriber.setReadTimeoutMillis(300);
                assertThrows(SocketTimeoutException.class, subscriber::readFrame);
            }
        }
    }

    @Test
    void backwardsTimestampsStayInOffsetOrder() throws Exception {
        Path directory = Files.createTempDirectory("dess-replay");
        try {
            long[] clock = {2_000L};
            TopicLog log = TopicLog.open(directory.resolve("sales.log"), Durability.INTERVAL, () -> {
                long time = clock[0];
                clock[0] = 1_000L;
                return time;
            });
            try {
                log.append("first".getBytes(StandardCharsets.UTF_8));
                log.append("second".getBytes(StandardCharsets.UTF_8));
            } finally {
                log.close();
            }
            try (Broker broker = startReplay(directory);
                    ProtocolClient subscriber = ProtocolClient.connect(broker.port())) {
                subscriber.setReadTimeoutMillis(5_000);
                subscriber.hello();
                subscriber.subscribe("sales", StartPosition.earliest());
                List<Received> delivered = readDeliveries(subscriber, 2);
                assertEquals(0L, delivered.get(0).offset());
                assertEquals("first", delivered.get(0).text());
                assertEquals(1L, delivered.get(1).offset());
                assertEquals("second", delivered.get(1).text());
            }
        } finally {
            delete(directory);
        }
    }

    @Test
    void earliestAfterKillDeliversOffsetZero() throws Exception {
        Path directory = Files.createTempDirectory("dess-replay");
        Path error = Files.createTempFile("dess-replay-err", ".txt");
        byte[] payload = "kept".getBytes(StandardCharsets.UTF_8);
        Process process = startTrial(directory, error);
        Stdout stdout = new Stdout(process);
        ProtocolClient client = null;
        try {
            String started = stdout.await("event=broker.started", Duration.ofSeconds(10));
            assertTrue(started.contains("event=broker.started"), started + Files.readString(error));
            int port = portFrom(started);
            client = ProtocolClient.connect(port);
            client.setReadTimeoutMillis(5_000);
            client.hello();
            client.publish("sales", payload);
            assertEquals(0L, readAck(client));
        } finally {
            if (client != null) {
                client.close();
            }
            kill(process);
            Files.deleteIfExists(error);
        }
        try (Broker broker = Broker.start(TestConfigs.durable(directory, Durability.ALWAYS, 1_000));
                ProtocolClient subscriber = ProtocolClient.connect(broker.port())) {
            subscriber.setReadTimeoutMillis(5_000);
            subscriber.hello();
            subscriber.subscribe("sales", StartPosition.earliest());
            Received delivered = readDeliveries(subscriber, 1).get(0);
            assertEquals(0L, delivered.offset());
            assertEquals("kept", delivered.text());
        } finally {
            delete(directory);
        }
    }

    private static Broker startReplay() throws IOException {
        return Broker.start(TestConfigs.full(
                0,
                32,
                TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES,
                120_000,
                TestConfigs.DEFAULT_MAX_IN_FLIGHT,
                1_024,
                1_024,
                30_000,
                TestConfigs.DEFAULT_PUBLISH_WAIT_MILLIS,
                TestConfigs.DEFAULT_ACK_TIMEOUT_MILLIS,
                true,
                List.of()));
    }

    private static Broker startReplay(Path directory) throws IOException {
        return Broker.start(new BrokerConfig(
                0,
                32,
                TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES,
                120_000,
                TestConfigs.DEFAULT_MAX_IN_FLIGHT,
                1_024,
                1_024,
                30_000,
                TestConfigs.DEFAULT_PUBLISH_WAIT_MILLIS,
                TestConfigs.DEFAULT_ACK_TIMEOUT_MILLIS,
                true,
                List.of(),
                directory,
                Durability.INTERVAL,
                60_000,
                "INFO",
                1_000,
                2_000,
                TestPki.brokerStore(),
                TestPki.trustStore(),
                TestPki.passwordFile(),
                false,
                55124,
                false,
                null));
    }

    private static void publishAll(ProtocolClient client, int count) throws IOException {
        for (int index = 0; index < count; index++) {
            client.publish("sales", payload(index));
            assertEquals(index, readAck(client));
        }
    }

    private static long readAck(ProtocolClient client) throws IOException {
        Frame frame = client.readFrame();
        assertEquals(Command.ACK, frame.command(), frame.command()::name);
        return client.brokerAck(frame).offset();
    }

    private static List<Received> readDeliveries(ProtocolClient client, int count) throws IOException {
        List<Received> delivered = new ArrayList<>();
        while (delivered.size() < count) {
            Frame frame = client.readFrame();
            if (frame.command() == Command.DELIVER) {
                delivered.add(new Received(FrameCodec.delivery(frame.header()).offset(), new String(frame.payload(), StandardCharsets.UTF_8)));
            }
        }
        return delivered;
    }

    private static byte[] payload(int index) {
        return payloadText(index).getBytes(StandardCharsets.UTF_8);
    }

    private static String payloadText(int index) {
        return "m" + index;
    }

    private static Process startTrial(Path directory, Path error) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add("dev.dess.broker.DurabilityTrial");
        command.add(directory.toString());
        command.add(Durability.ALWAYS.name());
        command.add("1000");
        return new ProcessBuilder(command).redirectError(error.toFile()).start();
    }

    private static void kill(Process process) throws Exception {
        if (!process.isAlive()) {
            return;
        }
        Process kill = new ProcessBuilder("kill", "-9", Long.toString(process.pid())).start();
        assertEquals(0, kill.waitFor());
        assertTrue(process.waitFor(5, TimeUnit.SECONDS));
    }

    private static int portFrom(String text) {
        String marker = "tcpPort=";
        int at = text.indexOf(marker);
        if (at < 0) {
            throw new AssertionError(text);
        }
        int start = at + marker.length();
        int end = start;
        while (end < text.length() && Character.isDigit(text.charAt(end))) {
            end++;
        }
        return Integer.parseInt(text.substring(start, end));
    }

    private static void delete(Path root) throws IOException {
        if (Files.notExists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private record Received(long offset, String text) {
    }

    private static final class Stdout {

        private final StringBuilder text = new StringBuilder();

        private final LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<>();

        private Stdout(Process process) {
            Thread reader = new Thread(() -> pump(process), "dess-replay-stdout");
            reader.setDaemon(true);
            reader.start();
        }

        private void pump(Process process) {
            try (BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = input.readLine()) != null) {
                    synchronized (text) {
                        text.append(line).append('\n');
                    }
                    lines.put(line);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            } catch (IOException ex) {
                // The trial closed its output.
            }
        }

        private String await(String needle, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            String current = text();
            if (current.contains(needle)) {
                return current;
            }
            while (true) {
                long wait = deadline - System.nanoTime();
                if (wait <= 0) {
                    return text();
                }
                String line = lines.poll(wait, TimeUnit.NANOSECONDS);
                if (line == null) {
                    return text();
                }
                current = text();
                if (current.contains(needle)) {
                    return current;
                }
            }
        }

        private String text() {
            synchronized (text) {
                return text.toString();
            }
        }
    }
}
