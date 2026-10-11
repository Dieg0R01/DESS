package dev.dess.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.ClientAck;
import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import dev.dess.common.protocol.StartPosition;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ConsumeCommandTest {

    @Test
    void deliveriesPrintTopicAndUtf8Payload() throws Exception {
        Path error = Files.createTempFile("dess-consume-err", ".txt");
        try (ScriptedBroker broker = ScriptedBroker.start()) {
            Process process = CliProcess.start(null, null, error, List.of(
                    "consume", "--topic", "sales", "--topic", "billing",
                    "--port", Integer.toString(broker.port()), "--heartbeat-ms", "200"));
            Stdout stdout = new Stdout(process);
            try {
                assertTrue(broker.await(Command.SUBSCRIBE, "sales", Duration.ofSeconds(2)));
                assertTrue(broker.await(Command.SUBSCRIBE, "billing", Duration.ofSeconds(2)));
                assertEquals(List.of("sales", "billing"), broker.topics(Command.SUBSCRIBE));
                assertTrue(broker.await(Command.PING, null, Duration.ofSeconds(2)));
                broker.deliver("sales", "hello".getBytes(StandardCharsets.UTF_8));
                broker.deliver("billing", new byte[] {(byte) 0xff});
                String text = stdout.await("billing");
                String newline = System.lineSeparator();
                assertTrue(text.contains("sales hello" + newline), text);
                assertTrue(text.contains("billing \uFFFD" + newline), text);
                assertTrue(broker.await(Command.ACK, "sales", Duration.ofSeconds(2)));
                assertTrue(broker.await(Command.ACK, "billing", Duration.ofSeconds(2)));
                assertEquals(0L, acknowledgement(broker, "sales").offset());
                assertEquals(1L, acknowledgement(broker, "billing").offset());
            } finally {
                stop(process);
                Files.deleteIfExists(error);
            }
        }
    }

    @Test
    void confirmFlagIsRejected() throws Exception {
        CliProcess.Result result = CliProcess.run(null, List.of(
                "consume", "--topic", "sales", "--confirm", "ALL"));
        assertEquals(2, result.exitCode(), result.stderr());
    }

    @Test
    void unknownTopicExitsOneAndNamesTheCode() throws Exception {
        assertBrokerErrorExitsOne("UNKNOWN_TOPIC");
    }

    @Test
    void slowConsumerExitsOneAndNamesTheCode() throws Exception {
        assertBrokerErrorExitsOne("SLOW_CONSUMER");
    }

    @Test
    void interruptUnsubscribesAndExitsZero() throws Exception {
        Path output = Files.createTempFile("dess-consume-out", ".txt");
        Path error = Files.createTempFile("dess-consume-err", ".txt");
        try (ScriptedBroker broker = ScriptedBroker.start()) {
            Process process = CliProcess.start(null, output, error, List.of(
                    "consume", "--topic", "sales", "--topic", "billing",
                    "--port", Integer.toString(broker.port()), "--heartbeat-ms", "10000"));
            try {
                assertTrue(broker.await(Command.SUBSCRIBE, "billing", Duration.ofSeconds(2)));
                Process kill = new ProcessBuilder("kill", "-INT", Long.toString(process.pid())).start();
                assertEquals(0, kill.waitFor());
                assertTrue(process.waitFor(5, TimeUnit.SECONDS), Files.readString(error));
                assertEquals(0, process.exitValue(), Files.readString(error));
                assertTrue(broker.await(Command.UNSUBSCRIBE, "billing", Duration.ofSeconds(2)));
                assertEquals(List.of("sales", "billing"), broker.topics(Command.UNSUBSCRIBE));
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                }
                Files.deleteIfExists(output);
                Files.deleteIfExists(error);
            }
        }
    }

    @Test
    void refusedConnectExitsOneAndNamesHostPort() throws Exception {
        CliProcess.Result result = CliProcess.run(null, List.of(
                "consume", "--topic", "sales", "--host", "127.0.0.1", "--port", "1"));
        assertEquals(1, result.exitCode(), result.stderr());
        assertTrue(result.stderr().contains("127.0.0.1:1"), result.stderr());
    }

    @Test
    void fromEarliestPrintsPayloadsInOrder() throws Exception {
        Path error = Files.createTempFile("dess-consume-err", ".txt");
        try (ScriptedBroker broker = ScriptedBroker.start()) {
            Process process = CliProcess.start(null, null, error, List.of(
                    "consume", "--topic", "sales", "--from", "earliest",
                    "--port", Integer.toString(broker.port()), "--heartbeat-ms", "10000"));
            Stdout stdout = new Stdout(process);
            try {
                assertTrue(broker.await(Command.SUBSCRIBE, "sales", Duration.ofSeconds(2)));
                assertEquals(StartPosition.earliest(), broker.startPosition("sales"));
                broker.deliver("sales", "one".getBytes(StandardCharsets.UTF_8));
                broker.deliver("sales", "two".getBytes(StandardCharsets.UTF_8));
                broker.deliver("sales", "three".getBytes(StandardCharsets.UTF_8));
                String text = stdout.await("sales three");
                int first = text.indexOf("sales one");
                int second = text.indexOf("sales two");
                int third = text.indexOf("sales three");
                assertTrue(first >= 0 && first < second && second < third, text);
            } finally {
                stop(process);
                Files.deleteIfExists(error);
            }
        }
    }

    @Test
    void resumeContinuesAfterTheAcknowledgedOffset() throws Exception {
        Path directory = Files.createTempDirectory("dess-offsets");
        Path offsets = directory.resolve("offsets");
        Path error = Files.createTempFile("dess-consume-err", ".txt");
        try (ScriptedBroker broker = ScriptedBroker.start()) {
            Process process = CliProcess.start(null, null, error, List.of(
                    "consume", "--topic", "sales", "--resume", "--resume-file", offsets.toString(),
                    "--port", Integer.toString(broker.port()), "--heartbeat-ms", "10000"));
            Stdout stdout = new Stdout(process);
            try {
                assertTrue(broker.await(Command.SUBSCRIBE, "sales", Duration.ofSeconds(2)));
                assertEquals(StartPosition.latest(), broker.startPosition("sales"));
                broker.deliver("sales", 0, "old".getBytes(StandardCharsets.UTF_8));
                broker.deliver("sales", 1, "kept".getBytes(StandardCharsets.UTF_8));
                broker.deliver("sales", 2, "last".getBytes(StandardCharsets.UTF_8));
                String printed = stdout.await("sales last");
                assertTrue(printed.contains("sales old"), printed);
                assertTrue(printed.contains("sales last"), printed);
                awaitOffset(offsets, "sales", 2, Duration.ofSeconds(2));
            } finally {
                stop(process);
            }
        }
        try (ScriptedBroker broker = ScriptedBroker.start()) {
            Process process = CliProcess.start(null, null, error, List.of(
                    "consume", "--topic", "sales", "--resume", "--resume-file", offsets.toString(),
                    "--port", Integer.toString(broker.port()), "--heartbeat-ms", "10000"));
            Stdout stdout = new Stdout(process);
            try {
                assertTrue(broker.await(Command.SUBSCRIBE, "sales", Duration.ofSeconds(2)));
                assertEquals(StartPosition.at(3), broker.startPosition("sales"));
                broker.deliver("sales", 3, "next".getBytes(StandardCharsets.UTF_8));
                String text = stdout.await("sales next");
                assertTrue(text.contains("sales next"), text);
                assertFalse(text.contains("sales old"), text);
                assertFalse(text.contains("sales kept"), text);
                assertFalse(text.contains("sales last"), text);
            } finally {
                stop(process);
                Files.deleteIfExists(error);
                delete(directory);
            }
        }
    }

    @Test
    void resumeWithFromExitsTwo() throws Exception {
        CliProcess.Result result = CliProcess.run(null, List.of(
                "consume", "--topic", "sales", "--resume", "--from", "earliest"));
        assertEquals(2, result.exitCode(), result.stderr());
    }

    @Test
    void tenRestartsDeliverEachPayloadOnce() throws Exception {
        Path directory = Files.createTempDirectory("dess-resume-logs");
        Path logs = directory.resolve("logs");
        Files.createDirectories(logs);
        Path offsets = directory.resolve("offsets");
        Path brokerError = directory.resolve("broker-err.txt");
        Process broker = startResumeBroker(logs, brokerError);
        BrokerStdout brokerOut = new BrokerStdout(broker);
        List<String> received = new ArrayList<>();
        BrokerSession publisher = null;
        try {
            int port = portFrom(brokerOut.await("tcpPort=", Duration.ofSeconds(10)));
            publisher = BrokerSession.connect("127.0.0.1", port, 30_000,
                    ClientTls.context(TestPki.producerStore(), TestPki.trustStore(), TestPki.passwordFile()));
            int batch = 1_000;
            for (int restart = 0; restart < 10; restart++) {
                Path output = directory.resolve("out-" + restart + ".txt");
                Path error = directory.resolve("err-" + restart + ".txt");
                int subscribed = brokerOut.count("event=subscription.added");
                Process consumer = CliProcess.start(null, output, error, List.of(
                        "consume", "--topic", "sales", "--resume", "--resume-file", offsets.toString(),
                        "--port", Integer.toString(port), "--heartbeat-ms", "60000", "--log-level", "WARNING"));
                try {
                    assertTrue(brokerOut.awaitCount("event=subscription.added", subscribed + 1, Duration.ofSeconds(10)), brokerOut.text());
                    int from = restart * batch;
                    for (int index = 0; index < batch; index++) {
                        publisher.publish("sales", payload(from + index));
                        Frame ack = publisher.readFrame();
                        assertEquals(Command.ACK, ack.command());
                    }
                    awaitOffset(offsets, "sales", from + batch - 1, Duration.ofSeconds(60));
                } finally {
                    consumer.destroyForcibly();
                    consumer.waitFor(5, TimeUnit.SECONDS);
                }
                String text = Files.readString(output);
                for (String line : text.split("\\R")) {
                    if (line.startsWith("sales ")) {
                        received.add(line.substring("sales ".length()));
                    }
                }
            }
            assertEquals(10_000, received.size());
            for (int index = 0; index < received.size(); index++) {
                assertEquals(payloadText(index), received.get(index), "index " + index);
            }
        } finally {
            if (publisher != null) {
                publisher.close();
            }
            broker.destroyForcibly();
            broker.waitFor(5, TimeUnit.SECONDS);
            delete(directory);
        }
    }

    private static byte[] payload(int index) {
        return payloadText(index).getBytes(StandardCharsets.UTF_8);
    }

    private static String payloadText(int index) {
        return String.format("m%05d", index);
    }

    private static Process startResumeBroker(Path directory, Path error) throws IOException {
        Path root = repositoryRoot();
        String classpath = root.resolve("dess-broker/target/test-classes")
                + File.pathSeparator
                + root.resolve("dess-broker/target/classes")
                + File.pathSeparator
                + root.resolve("dess-common/target/classes");
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(classpath);
        command.add("dev.dess.broker.ResumeTrial");
        command.add(directory.toString());
        return new ProcessBuilder(command).redirectError(error.toFile()).start();
    }

    private static Path repositoryRoot() {
        Path current = Path.of("").toAbsolutePath().normalize();
        if (current.getFileName() != null && "dess-client".equals(current.getFileName().toString())) {
            return current.getParent();
        }
        if (Files.isDirectory(current.resolve("dess-client"))) {
            return current;
        }
        throw new IllegalStateException("Could not find the repository root from " + current);
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

    private static void awaitOffset(Path file, String topic, long offset, Duration timeout) throws Exception {
        String expected = topic + " " + offset;
        Path directory = file.getParent() == null ? Path.of(".") : file.getParent();
        try (WatchService watch = FileSystems.getDefault().newWatchService()) {
            directory.register(watch, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
            long deadline = System.nanoTime() + timeout.toNanos();
            while (true) {
                if (containsOffset(file, expected)) {
                    return;
                }
                long wait = deadline - System.nanoTime();
                if (wait <= 0) {
                    throw new AssertionError(Files.exists(file) ? Files.readString(file) : "missing " + file);
                }
                WatchKey key = watch.poll(Math.min(wait, Duration.ofMillis(200).toNanos()), TimeUnit.NANOSECONDS);
                if (key == null) {
                    continue;
                }
                key.pollEvents();
                key.reset();
            }
        }
    }

    private static boolean containsOffset(Path file, String expected) throws IOException {
        if (Files.notExists(file)) {
            return false;
        }
        for (String line : Files.readAllLines(file)) {
            if (line.equals(expected)) {
                return true;
            }
        }
        return false;
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

    private static final class BrokerStdout {

        private final StringBuilder text = new StringBuilder();
        private final LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<>();

        private BrokerStdout(Process process) {
            Thread reader = new Thread(() -> pump(process), "dess-resume-broker-stdout");
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
                // The broker closed its output.
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

        private boolean awaitCount(String needle, int count, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (count(needle) < count) {
                long wait = deadline - System.nanoTime();
                if (wait <= 0) {
                    return false;
                }
                if (lines.poll(wait, TimeUnit.NANOSECONDS) == null && count(needle) < count) {
                    return false;
                }
            }
            return true;
        }

        private int count(String needle) {
            int found = 0;
            String current = text();
            int at = 0;
            while (true) {
                int next = current.indexOf(needle, at);
                if (next < 0) {
                    return found;
                }
                found++;
                at = next + needle.length();
            }
        }

        private String text() {
            synchronized (text) {
                return text.toString();
            }
        }
    }

    private static final class Stdout {

        private final StringBuilder text = new StringBuilder();
        private final LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<>();

        private Stdout(Process process) {
            Thread reader = new Thread(() -> pump(process), "dess-client-stdout");
            reader.setDaemon(true);
            reader.start();
        }

        private void pump(Process process) {
            try (BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = input.readLine()) != null) {
                    synchronized (text) {
                        text.append(line).append(System.lineSeparator());
                    }
                    lines.put(line);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            } catch (IOException ex) {
                // The client process closed its output.
            }
        }

        private String await(String needle) throws InterruptedException {
            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
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

    private static void assertBrokerErrorExitsOne(String code) throws Exception {
        try (ScriptedBroker broker = ScriptedBroker.start()) {
            broker.failSubscribes(code);
            CliProcess.Result result = CliProcess.run(null, List.of(
                    "consume", "--topic", "sales", "--port", Integer.toString(broker.port())));
            assertEquals(1, result.exitCode(), result.stderr());
            assertTrue(result.stderr().contains(code), result.stderr());
        }
    }

    private static ClientAck acknowledgement(ScriptedBroker broker, String topic) {
        for (Frame frame : broker.frames()) {
            if (frame.command() == Command.ACK && topic.equals(FrameCodec.leadingTopic(frame.header()))) {
                return FrameCodec.clientAck(frame.header());
            }
        }
        throw new AssertionError(broker.frames());
    }

    private static void stop(Process process) throws InterruptedException, IOException {
        if (!process.isAlive()) {
            return;
        }
        Process kill = new ProcessBuilder("kill", "-INT", Long.toString(process.pid())).start();
        kill.waitFor();
        if (!process.waitFor(3, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(2, TimeUnit.SECONDS);
        }
    }
}
