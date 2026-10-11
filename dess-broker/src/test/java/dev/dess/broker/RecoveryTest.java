package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class RecoveryTest {

    @Test
    void restartRebuildsEveryTopicOffset() throws Exception {
        Path directory = Files.createTempDirectory("dess-recovery");
        try {
            try (Broker broker = start(directory)) {
                try (ProtocolClient client = connect(broker)) {
                    assertEquals(0L, publish(client, "sales", "one"));
                    assertEquals(0L, publish(client, "billing", "two"));
                }
            }
            try (LogCapture logs = LogCapture.open();
                    Broker broker = start(directory);
                    ProtocolClient client = connect(broker)) {
                assertTrue(logs.await(Duration.ofSeconds(2),
                        "event=log.recovered", "files=2", "records=2", "truncatedRecords=0", "durationMs="),
                        logs.lines()::toString);
                assertTrue(logs.await(Duration.ofSeconds(2), "event=topic.created", "topic=sales", "source=RECOVERED"),
                        logs.lines()::toString);
                assertTrue(logs.await(Duration.ofSeconds(2), "event=topic.created", "topic=billing", "source=RECOVERED"),
                        logs.lines()::toString);
                assertEquals(1L, publish(client, "sales", "three"));
                assertEquals(1L, publish(client, "billing", "four"));
            }
        } finally {
            delete(directory);
        }
    }

    @Test
    void tornTailIsDiscardedOnce() throws Exception {
        Path directory = Files.createTempDirectory("dess-recovery");
        Path log = directory.resolve("sales.log");
        try {
            try (Broker broker = start(directory)) {
                try (ProtocolClient client = connect(broker)) {
                    assertEquals(0L, publish(client, "sales", "kept"));
                }
            }
            long position = Files.size(log);
            Files.write(log, new byte[] {9, 8, 7, 6, 5}, StandardOpenOption.APPEND);
            try (LogCapture logs = LogCapture.open();
                    Broker broker = start(directory);
                    ProtocolClient client = connect(broker)) {
                String absolute = log.toAbsolutePath().toString();
                assertTrue(logs.await(Duration.ofSeconds(2),
                        "WARNING", "event=log.truncated", absolute, "position=" + position),
                        logs.lines()::toString);
                assertEquals(position, Files.size(log));
                assertTrue(logs.await(Duration.ofSeconds(2), "event=log.recovered", "truncatedRecords=1"),
                        logs.lines()::toString);
                assertTrue(before(logs.lines(), "event=log.truncated", "event=broker.started"), logs.lines()::toString);
                assertTrue(before(logs.lines(), "event=log.recovered", "event=broker.started"), logs.lines()::toString);
                assertEquals(1L, publish(client, "sales", "next"));
            }
            try (LogCapture logs = LogCapture.open();
                    Broker broker = start(directory)) {
                assertTrue(logs.await(Duration.ofSeconds(2), "event=log.recovered", "truncatedRecords=0"),
                        logs.lines()::toString);
                assertEquals(0L, logs.count("event=log.truncated"));
            }
        } finally {
            delete(directory);
        }
    }

    @Test
    void midFileDamageRefusesStartup() throws Exception {
        Path directory = Files.createTempDirectory("dess-recovery");
        Path log = directory.resolve("sales.log");
        try {
            try (Broker broker = start(directory)) {
                try (ProtocolClient client = connect(broker)) {
                    assertEquals(0L, publish(client, "sales", "alpha"));
                    assertEquals(1L, publish(client, "sales", "beta"));
                }
            }
            byte[] bytes = Files.readAllBytes(log);
            bytes[25] ^= 0x01;
            Files.write(log, bytes);
            try (LogCapture logs = LogCapture.open()) {
                assertThrows(IOException.class, () -> start(directory));
                String absolute = log.toAbsolutePath().toString();
                assertTrue(logs.await(Duration.ofSeconds(2), "SEVERE", "event=log.corrupt", absolute, "position=0"),
                        logs.lines()::toString);
                assertEquals(0L, logs.count("event=broker.started"));
            }
        } finally {
            delete(directory);
        }
    }

    @Test
    void oneHundredThousandRecordsRecoverWithinTenSeconds() throws Exception {
        Path directory = Files.createTempDirectory("dess-recovery");
        byte[] payload = new byte[1024];
        TopicLog log = TopicLog.open(directory.resolve("bulk.log"), Durability.INTERVAL);
        try {
            for (int index = 0; index < 100_000; index++) {
                log.append(payload);
            }
        } finally {
            log.close();
        }
        try (LogCapture logs = LogCapture.open();
                Broker broker = start(directory)) {
            assertTrue(logs.await(Duration.ofSeconds(2),
                    "event=log.recovered", "files=1", "records=100000", "truncatedRecords=0"),
                    logs.lines()::toString);
            String recovered = line(logs.lines(), "event=log.recovered");
            long durationMs = Long.parseLong(property(recovered, "durationMs"));
            assertTrue(durationMs < 10_000L, recovered);
            assertTrue(broker.port() > 0);
        } finally {
            delete(directory);
        }
    }

    @Test
    void unexpectedFileRefusesStartup() throws Exception {
        Path directory = Files.createTempDirectory("dess-recovery");
        Path unexpected = directory.resolve("notes.txt");
        Files.writeString(unexpected, "nope");
        try {
            IOException failure = assertThrows(IOException.class, () -> start(directory));
            assertTrue(failure.getMessage().contains(unexpected.toAbsolutePath().toString()), failure::getMessage);
        } finally {
            delete(directory);
        }
    }

    @Test
    void emptyLogRecoversOffsetZero() throws Exception {
        Path directory = Files.createTempDirectory("dess-recovery");
        Files.createFile(directory.resolve("sales.log"));
        try (LogCapture logs = LogCapture.open();
                Broker broker = start(directory);
                ProtocolClient client = connect(broker)) {
            assertTrue(logs.await(Duration.ofSeconds(2),
                    "event=log.recovered", "files=1", "records=0", "truncatedRecords=0"),
                    logs.lines()::toString);
            assertTrue(logs.await(Duration.ofSeconds(2), "event=topic.created", "topic=sales", "source=RECOVERED"),
                    logs.lines()::toString);
            assertEquals(0L, publish(client, "sales", "first"));
        } finally {
            delete(directory);
        }
    }

    @Test
    void secondBrokerFindsTheDirectoryInUse() throws Exception {
        Path directory = Files.createTempDirectory("dess-recovery");
        try (Broker first = start(directory)) {
            IOException failure = assertThrows(IOException.class, () -> start(directory));
            String message = failure.getMessage();
            assertTrue(message.contains(directory.toAbsolutePath().toString()), message);
            assertTrue(message.contains("in use"), message);
            try (ProtocolClient client = connect(first)) {
                assertEquals(0L, publish(client, "sales", "still"));
            }
        } finally {
            delete(directory);
        }
    }

    private static Broker start(Path directory) throws IOException {
        return Broker.start(TestConfigs.durable(directory, Durability.INTERVAL, 60_000));
    }

    private static ProtocolClient connect(Broker broker) throws IOException {
        ProtocolClient client = ProtocolClient.connect(broker.port());
        client.setReadTimeoutMillis(5_000);
        client.hello();
        return client;
    }

    private static long publish(ProtocolClient client, String topic, String payload) throws IOException {
        client.publish(topic, payload.getBytes(StandardCharsets.UTF_8));
        Frame frame = client.readFrame();
        assertEquals(Command.ACK, frame.command());
        return client.brokerAck(frame).offset();
    }

    private static boolean before(List<String> lines, String earlier, String later) {
        int first = indexOf(lines, earlier);
        int second = indexOf(lines, later);
        return first >= 0 && second > first;
    }

    private static int indexOf(List<String> lines, String part) {
        for (int index = 0; index < lines.size(); index++) {
            if (lines.get(index).contains(part)) {
                return index;
            }
        }
        return -1;
    }

    private static String line(List<String> lines, String part) {
        for (String candidate : lines) {
            if (candidate.contains(part)) {
                return candidate;
            }
        }
        throw new AssertionError(lines);
    }

    private static String property(String line, String key) {
        String token = key + "=";
        int start = line.indexOf(token);
        if (start < 0) {
            throw new AssertionError(line);
        }
        start += token.length();
        int end = line.indexOf(' ', start);
        if (end < 0) {
            end = line.length();
        }
        return line.substring(start, end);
    }

    private static void delete(Path root) throws IOException {
        if (Files.notExists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            List<Path> paths = walk.sorted(Comparator.reverseOrder()).toList();
            for (Path path : paths) {
                Files.deleteIfExists(path);
            }
        }
    }
}
