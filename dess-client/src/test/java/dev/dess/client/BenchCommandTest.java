package dev.dess.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
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
class BenchCommandTest {

    @Test
    void shortRunPrintsThroughputAndLatency() throws Exception {
        Path brokerError = Files.createTempFile("dess-bench-broker-err", ".txt");
        Path logs = Files.createTempDirectory("dess-bench-logs");
        Process broker = startBroker(logs, brokerError);
        BrokerStdout stdout = new BrokerStdout(broker);
        try {
            int port = portFrom(stdout.await("tcpPort=", Duration.ofSeconds(10)));
            Result result = run(
                    "bench", "--topic", "bench", "--payload-bytes", "1024",
                    "--producers", "1", "--consumers", "1", "--messages", "100",
                    "--host", "127.0.0.1", "--port", Integer.toString(port),
                    "--identity", TestPki.producerStore().toString(),
                    "--trust-store", TestPki.trustStore().toString(),
                    "--password-file", TestPki.passwordFile().toString());
            assertEquals(0, result.exitCode(), result.stdout() + result.stderr());
            assertTrue(result.stdout().contains("payloadBytes "), result.stdout());
            assertTrue(result.stdout().contains("producers "), result.stdout());
            assertTrue(result.stdout().contains("consumers "), result.stdout());
            assertTrue(result.stdout().contains("messages "), result.stdout());
            assertTrue(result.stdout().contains("messagesPerSecond "), result.stdout());
            assertTrue(result.stdout().contains("latencyP50Ms "), result.stdout());
            assertTrue(result.stdout().contains("latencyP99Ms "), result.stdout());
            assertTrue(result.stdout().contains("os "), result.stdout());
            assertTrue(result.stdout().contains("processors "), result.stdout());
            assertTrue(result.stdout().contains("maxMemoryBytes "), result.stdout());
        } finally {
            broker.destroyForcibly();
            broker.waitFor(5, TimeUnit.SECONDS);
            Files.deleteIfExists(brokerError);
            delete(logs);
        }
    }

    @Test
    void missingTopicOrMessagesOrPortWithoutHostExitsTwo() throws Exception {
        Result missingTopic = run(
                "bench", "--messages", "10", "--host", "127.0.0.1", "--port", "55123");
        assertEquals(2, missingTopic.exitCode(), missingTopic.stderr());

        Result missingMessages = run(
                "bench", "--topic", "bench", "--host", "127.0.0.1", "--port", "55123");
        assertEquals(2, missingMessages.exitCode(), missingMessages.stderr());

        Result portWithoutHost = run(
                "bench", "--topic", "bench", "--messages", "10", "--port", "55123");
        assertEquals(2, portWithoutHost.exitCode(), portWithoutHost.stderr());
    }

    @Test
    void refusedConnectExitsOne() throws Exception {
        Result result = run(
                "bench", "--topic", "bench", "--messages", "1",
                "--host", "127.0.0.1", "--port", "1",
                "--identity", TestPki.producerStore().toString(),
                "--trust-store", TestPki.trustStore().toString(),
                "--password-file", TestPki.passwordFile().toString());
        assertEquals(1, result.exitCode(), result.stderr());
    }

    private static Result run(String... args) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add("dev.dess.client.ClientMain");
        command.addAll(List.of(args));
        Path output = Files.createTempFile("dess-bench-out", ".txt");
        Path error = Files.createTempFile("dess-bench-err", ".txt");
        Process process = new ProcessBuilder(command).redirectOutput(output.toFile()).redirectError(error.toFile()).start();
        try {
            if (!process.waitFor(30, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(2, TimeUnit.SECONDS);
            }
            return new Result(process.exitValue(), Files.readString(output), Files.readString(error));
        } finally {
            Files.deleteIfExists(output);
            Files.deleteIfExists(error);
        }
    }

    private static Process startBroker(Path directory, Path error) throws IOException {
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

    private record Result(int exitCode, String stdout, String stderr) {
    }

    private static final class BrokerStdout {

        private final StringBuilder text = new StringBuilder();
        private final LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<>();

        private BrokerStdout(Process process) {
            Thread reader = new Thread(() -> pump(process), "dess-bench-broker-stdout");
            reader.setDaemon(true);
            reader.start();
        }

        private void pump(Process process) {
            try (BufferedReader input = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
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
                    throw new AssertionError(text());
                }
                String line = lines.poll(wait, TimeUnit.NANOSECONDS);
                if (line == null) {
                    throw new AssertionError(text());
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
