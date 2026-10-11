package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class BrokerLifecycleTest {

    @Test
    void startOnAFreePortAcceptsAndLogsStarted() throws Exception {
        try (LogCapture logs = LogCapture.open();
                Broker broker = Broker.start(TestConfigs.of(0, 8, BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, 30_000))) {
            long started = System.nanoTime();
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), broker.port()), 2_000);
            }
            assertTrue(Duration.ofNanos(System.nanoTime() - started).compareTo(Duration.ofSeconds(2)) < 0);
            assertEquals(1, logs.count("event=broker.started", "version=3", "tcpPort=" + broker.port()));
        }
    }

    @Test
    void takenPortNamesThePortAndDoesNotLogStarted() throws Exception {
        try (LogCapture logs = LogCapture.open();
                ServerSocket taken = new ServerSocket(0)) {
            int port = taken.getLocalPort();
            IOException failure = assertThrows(IOException.class, () -> Broker.start(TestConfigs.of(
                    port, 8, BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, 30_000)));
            assertTrue(failure.getMessage() != null && failure.getMessage().contains(Integer.toString(port)), failure.getMessage());
            assertEquals(0, logs.count("event=broker.started"));
        }
    }

    @Test
    void closeWithTwentyClientsStopsWithinFiveSeconds() throws Exception {
        List<ProtocolClient> clients = new ArrayList<>();
        LogCapture logs = LogCapture.open();
        Broker broker = Broker.start(TestConfigs.of(0, 32, BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, 120_000));
        try {
            for (int index = 0; index < 20; index++) {
                ProtocolClient client = ProtocolClient.connect(broker.port());
                clients.add(client);
                client.hello();
            }
            long started = System.nanoTime();
            broker.close();
            Duration elapsed = Duration.ofNanos(System.nanoTime() - started);
            assertTrue(elapsed.compareTo(Duration.ofSeconds(5)) < 0, elapsed::toString);
            assertEquals(1, logs.count("event=broker.stopped", "reason=CLOSE", "uptimeSeconds="));
            assertEquals(20, logs.count("event=client.disconnected", "reason=SHUTDOWN"));
        } finally {
            for (ProtocolClient client : clients) {
                client.close();
            }
            broker.close();
            logs.close();
        }
    }

    @Test
    void sigtermLogsSignalAndExitsZero() throws Exception {
        Path error = Files.createTempFile("dess-broker-err", ".txt");
        Process process = startBroker(null, error, "--port", "0");
        Stdout stdout = new Stdout(process);
        ProtocolClient client = null;
        try {
            String text = stdout.await("event=broker.started");
            assertTrue(text.contains("event=broker.started"), text + Files.readString(error));
            int port = portFrom(text);
            client = ProtocolClient.connect(port);
            client.hello();
            long started = System.nanoTime();
            Process kill = new ProcessBuilder("kill", "-TERM", Long.toString(process.pid())).start();
            assertEquals(0, kill.waitFor());
            assertTrue(process.waitFor(5, TimeUnit.SECONDS), stdout.text() + Files.readString(error));
            assertTrue(Duration.ofNanos(System.nanoTime() - started).compareTo(Duration.ofSeconds(5)) < 0);
            assertEquals(0, process.exitValue());
            String stopped = stdout.await("reason=SIGNAL");
            assertTrue(stopped.contains("event=broker.stopped"), stopped);
            assertTrue(stopped.contains("reason=SIGNAL"), stopped);
            assertTrue(stopped.contains("uptimeSeconds="), stopped);
        } finally {
            if (client != null) {
                client.close();
            }
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(2, TimeUnit.SECONDS);
            }
            Files.deleteIfExists(error);
        }
    }

    @Test
    void bindFailureExitsOneAndNamesThePort() throws Exception {
        try (ServerSocket taken = new ServerSocket(0)) {
            int port = taken.getLocalPort();
            Path output = Files.createTempFile("dess-broker-out", ".txt");
            Path error = Files.createTempFile("dess-broker-err", ".txt");
            Process process = startBroker(output, error, "--port", Integer.toString(port));
            try {
                assertTrue(process.waitFor(5, TimeUnit.SECONDS));
                String combined = Files.readString(output) + Files.readString(error);
                assertEquals(1, process.exitValue(), combined);
                assertTrue(combined.contains(Integer.toString(port)), combined);
                assertFalse(combined.contains("event=broker.started"), combined);
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                    process.waitFor(2, TimeUnit.SECONDS);
                }
                Files.deleteIfExists(output);
                Files.deleteIfExists(error);
            }
        }
    }

    private static Process startBroker(Path output, Path error, String... args) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add("dev.dess.broker.BrokerMain");
        command.addAll(List.of(args));
        ProcessBuilder builder = new ProcessBuilder(command).redirectError(error.toFile());
        if (output != null) {
            builder.redirectOutput(output.toFile());
        }
        return builder.start();
    }

    private static final class Stdout {

        private final StringBuilder text = new StringBuilder();
        private final LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<>();

        private Stdout(Process process) {
            Thread reader = new Thread(() -> pump(process), "dess-broker-stdout");
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
                // The broker process closed its output.
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
}
