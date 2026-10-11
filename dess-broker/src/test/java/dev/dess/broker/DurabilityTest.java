package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class DurabilityTest {

    @Test
    void acknowledgedAlwaysPublishSurvivesKill() throws Exception {
        Path directory = Files.createTempDirectory("dess-durability");
        byte[] payload = "kept".getBytes(StandardCharsets.UTF_8);
        Path error = Files.createTempFile("dess-durability-err", ".txt");
        Process process = startTrial(directory, Durability.ALWAYS, 1_000, error);
        Stdout stdout = new Stdout(process);
        ProtocolClient client = null;
        try {
            int port = startedPort(stdout, error);
            client = ProtocolClient.connect(port);
            client.setReadTimeoutMillis(5_000);
            client.hello();
            client.publish("sales", payload);
            FrameAck ack = readAck(client);
            assertEquals(0L, ack.offset());
            assertTrue(stdout.await("event=message.accepted", Duration.ofSeconds(2)).contains("timestamp="), stdout.text());
        } finally {
            close(client);
            kill(process);
            Files.deleteIfExists(error);
        }

        try (LogCapture logs = LogCapture.open();
                Broker broker = Broker.start(TestConfigs.durable(directory, Durability.ALWAYS, 1_000));
                ProtocolClient again = ProtocolClient.connect(broker.port())) {
            assertTrue(logs.await(Duration.ofSeconds(2), "event=log.recovered", "records=1"), logs.lines()::toString);
            again.setReadTimeoutMillis(5_000);
            again.hello();
            again.publish("sales", "next".getBytes(StandardCharsets.UTF_8));
            assertEquals(1L, readAck(again).offset());
        }
        List<StoredRecord> records = readLog(directory.resolve("sales.log"));
        assertTrue(records.size() >= 1, records::toString);
        assertEquals(0L, records.get(0).offset());
        assertEquals("kept", new String(records.get(0).payload(), StandardCharsets.UTF_8));
        delete(directory);
    }

    @Test
    void fiftyAlwaysTrialsLoseNothing() throws Exception {
        for (int trial = 0; trial < 50; trial++) {
            Path directory = Files.createTempDirectory("dess-durability");
            byte[] payload = ("trial-" + trial).getBytes(StandardCharsets.UTF_8);
            Path error = Files.createTempFile("dess-durability-err", ".txt");
            Process process = startTrial(directory, Durability.ALWAYS, 1_000, error);
            Stdout stdout = new Stdout(process);
            ProtocolClient client = null;
            try {
                int port = startedPort(stdout, error);
                client = ProtocolClient.connect(port);
                client.setReadTimeoutMillis(5_000);
                client.hello();
                client.publish("sales", payload);
                assertEquals(0L, readAck(client).offset());
            } finally {
                close(client);
                kill(process);
                Files.deleteIfExists(error);
            }
            List<StoredRecord> records = readLog(directory.resolve("sales.log"));
            assertTrue(contains(records, 0L, payload), records + " trial " + trial);
            delete(directory);
        }
    }

    @Test
    void intervalKeepsMessagesConfirmedBeforeTheLastSync() throws Exception {
        Path directory = Files.createTempDirectory("dess-durability");
        Path error = Files.createTempFile("dess-durability-err", ".txt");
        Process process = startTrial(directory, Durability.INTERVAL, 200, error);
        Stdout stdout = new Stdout(process);
        ProtocolClient client = null;
        int batch = 4;
        try {
            int port = startedPort(stdout, error);
            client = ProtocolClient.connect(port);
            client.setReadTimeoutMillis(5_000);
            client.hello();
            for (int index = 0; index < batch; index++) {
                client.publish("sales", payload("a", index));
                assertEquals(index, readAck(client).offset());
            }
            int synced = stdout.count("event=log.synced");
            assertTrue(stdout.awaitCount("event=log.synced", synced + 2, Duration.ofSeconds(5)), stdout.text());
            for (int index = 0; index < batch; index++) {
                client.publish("sales", payload("b", index));
                assertEquals(batch + index, readAck(client).offset());
            }
        } finally {
            close(client);
            kill(process);
            Files.deleteIfExists(error);
        }

        List<StoredRecord> records = readLog(directory.resolve("sales.log"));
        for (int index = 0; index < batch; index++) {
            assertTrue(contains(records, index, payload("a", index)), records::toString);
        }
        boolean missing = false;
        for (int index = 0; index < batch; index++) {
            boolean present = contains(records, batch + index, payload("b", index));
            if (!present) {
                missing = true;
            } else {
                assertFalse(missing, records::toString);
            }
        }
        try (Broker broker = Broker.start(TestConfigs.durable(directory, Durability.INTERVAL, 60_000))) {
            assertTrue(broker.port() > 0);
        }
        delete(directory);
    }

    @Test
    void orderlyCloseForcesTheIntervalLog() throws Exception {
        Path directory = Files.createTempDirectory("dess-durability");
        byte[] first = "one".getBytes(StandardCharsets.UTF_8);
        byte[] second = "two".getBytes(StandardCharsets.UTF_8);
        try (Broker broker = Broker.start(TestConfigs.durable(directory, Durability.INTERVAL, 60_000));
                ProtocolClient client = ProtocolClient.connect(broker.port())) {
            client.setReadTimeoutMillis(5_000);
            client.hello();
            client.publish("sales", first);
            assertEquals(0L, readAck(client).offset());
            client.publish("sales", second);
            assertEquals(1L, readAck(client).offset());
        }
        List<StoredRecord> records = readLog(directory.resolve("sales.log"));
        assertTrue(contains(records, 0L, first), records::toString);
        assertTrue(contains(records, 1L, second), records::toString);
        try (Broker restarted = Broker.start(TestConfigs.durable(directory, Durability.INTERVAL, 60_000));
                ProtocolClient client = ProtocolClient.connect(restarted.port())) {
            client.setReadTimeoutMillis(5_000);
            client.hello();
            client.publish("sales", "three".getBytes(StandardCharsets.UTF_8));
            assertEquals(2L, readAck(client).offset());
        }
        delete(directory);
    }

    @Test
    void aDirectoryInPlaceOfTheLogIsStorageUnavailable() throws Exception {
        Path directory = Files.createTempDirectory("dess-durability");
        Files.createDirectory(directory.resolve("sales.log"));
        try (LogCapture logs = LogCapture.open();
                Broker broker = Broker.start(TestConfigs.durable(directory, Durability.ALWAYS, 1_000));
                ProtocolClient producer = ProtocolClient.connect(broker.port());
                ProtocolClient consumer = ProtocolClient.connect(broker.port())) {
            producer.setReadTimeoutMillis(5_000);
            consumer.setReadTimeoutMillis(5_000);
            producer.hello();
            consumer.hello();
            consumer.subscribe("other");
            logs.awaitAdded(consumer, "other");
            producer.publish("sales", "nope".getBytes(StandardCharsets.UTF_8));
            Frame error = producer.readFrame();
            assertEquals(Command.ERROR, error.command());
            assertEquals("STORAGE_UNAVAILABLE", FrameCodec.errorCode(error.header()));
            assertTrue(logs.await(Duration.ofSeconds(2), "event=message.rejected", "errorCode=STORAGE_UNAVAILABLE"), logs.lines()::toString);
            consumer.setReadTimeoutMillis(200);
            assertThrows(SocketTimeoutException.class, consumer::readFrame);
            consumer.setReadTimeoutMillis(5_000);
            producer.publish("other", "yes".getBytes(StandardCharsets.UTF_8));
            assertEquals(0L, readAck(producer).offset());
            Frame delivered = consumer.readFrame();
            assertEquals(Command.DELIVER, delivered.command());
            assertTrue(logs.await(Duration.ofSeconds(2), "event=message.accepted", "topic=other", "timestamp="), logs.lines()::toString);
        }
        delete(directory);
    }

    private static byte[] payload(String batch, int index) {
        return (batch + index).getBytes(StandardCharsets.UTF_8);
    }

    private static boolean contains(List<StoredRecord> records, long offset, byte[] payload) {
        for (StoredRecord record : records) {
            if (record.offset() == offset && Arrays.equals(record.payload(), payload)) {
                return true;
            }
        }
        return false;
    }

    private static FrameAck readAck(ProtocolClient client) throws IOException {
        Frame frame = client.readFrame();
        assertEquals(Command.ACK, frame.command());
        return new FrameAck(client.brokerAck(frame).offset());
    }

    private static int startedPort(Stdout stdout, Path error) throws Exception {
        String text = stdout.await("event=broker.started", Duration.ofSeconds(10));
        assertTrue(text.contains("event=broker.started"), text + Files.readString(error));
        return portFrom(text);
    }

    private static Process startTrial(Path directory, Durability durability, int syncIntervalMillis, Path error) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add("dev.dess.broker.DurabilityTrial");
        command.add(directory.toString());
        command.add(durability.name());
        command.add(Integer.toString(syncIntervalMillis));
        return new ProcessBuilder(command).redirectError(error.toFile()).start();
    }

    private static void close(ProtocolClient client) {
        if (client == null) {
            return;
        }
        try {
            client.close();
        } catch (IOException ex) {
            // The trial is about to be killed.
        }
    }

    private static void kill(Process process) throws Exception {
        if (!process.isAlive()) {
            return;
        }
        Process kill = new ProcessBuilder("kill", "-9", Long.toString(process.pid())).start();
        assertEquals(0, kill.waitFor());
        assertTrue(process.waitFor(5, TimeUnit.SECONDS));
    }

    private static void delete(Path root) throws IOException {
        if (Files.notExists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            List<Path> paths = walk.sorted(Comparator.reverseOrder()).toList();
            for (Path path : paths) {
                Files.deleteIfExists(path);
            }
        }
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

    private static List<StoredRecord> readLog(Path file) throws IOException {
        byte[] data = Files.readAllBytes(file);
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        List<StoredRecord> records = new ArrayList<>();
        while (buffer.remaining() >= 4) {
            int mark = buffer.position();
            int bodyLength = buffer.getInt();
            if (bodyLength < 25 || buffer.remaining() < bodyLength) {
                buffer.position(mark);
                break;
            }
            byte[] body = new byte[bodyLength];
            buffer.get(body);
            CRC32 crc = new CRC32();
            crc.update(body, 0, bodyLength - 4);
            ByteBuffer view = ByteBuffer.wrap(body).order(ByteOrder.BIG_ENDIAN);
            int version = view.get() & 0xff;
            long offset = view.getLong();
            view.getLong();
            int payloadLength = view.getInt();
            if (version != 1 || payloadLength < 0 || 25L + payloadLength != bodyLength) {
                break;
            }
            byte[] payload = new byte[payloadLength];
            view.get(payload);
            long actual = Integer.toUnsignedLong(view.getInt());
            if (actual != crc.getValue()) {
                break;
            }
            records.add(new StoredRecord(offset, payload));
        }
        return records;
    }

    private record FrameAck(long offset) {
    }

    private record StoredRecord(long offset, byte[] payload) {
    }

    private static final class Stdout {

        private final StringBuilder text = new StringBuilder();

        private final LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<>();

        private Stdout(Process process) {
            Thread reader = new Thread(() -> pump(process), "dess-durability-stdout");
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

        private boolean awaitCount(String needle, int minimum, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (count(needle) < minimum) {
                long wait = deadline - System.nanoTime();
                if (wait <= 0) {
                    return false;
                }
                if (lines.poll(wait, TimeUnit.NANOSECONDS) == null) {
                    return count(needle) >= minimum;
                }
            }
            return true;
        }

        private int count(String needle) {
            String current = text();
            int count = 0;
            int at = 0;
            while ((at = current.indexOf(needle, at)) >= 0) {
                count++;
                at += needle.length();
            }
            return count;
        }

        private String text() {
            synchronized (text) {
                return text.toString();
            }
        }
    }
}
