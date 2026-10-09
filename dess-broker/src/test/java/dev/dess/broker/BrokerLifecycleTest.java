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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class BrokerLifecycleTest {

    @Test
    void startOnAFreePortAcceptsAndLogsStarted() throws Exception {
        List<String> lines = new CopyOnWriteArrayList<>();
        Logger logger = Logger.getLogger("dev.dess.broker");
        Handler handler = capture(lines);
        logger.setLevel(Level.INFO);
        logger.addHandler(handler);
        try (Broker broker = Broker.start(new BrokerConfig(0, 8, BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, 30_000))) {
            long started = System.nanoTime();
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), broker.port()), 2_000);
            }
            assertTrue(Duration.ofNanos(System.nanoTime() - started).compareTo(Duration.ofSeconds(2)) < 0);
            assertEquals(1, count(lines, "event=broker.started", "version=1", "tcpPort=" + broker.port()));
        } finally {
            logger.removeHandler(handler);
        }
    }

    @Test
    void takenPortNamesThePortAndDoesNotLogStarted() throws Exception {
        List<String> lines = new CopyOnWriteArrayList<>();
        Logger logger = Logger.getLogger("dev.dess.broker");
        Handler handler = capture(lines);
        logger.setLevel(Level.INFO);
        logger.addHandler(handler);
        try (ServerSocket taken = new ServerSocket(0)) {
            int port = taken.getLocalPort();
            IOException failure = assertThrows(IOException.class, () -> Broker.start(new BrokerConfig(
                    port, 8, BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, 30_000)));
            assertTrue(failure.getMessage() != null && failure.getMessage().contains(Integer.toString(port)), failure.getMessage());
            assertEquals(0, count(lines, "event=broker.started"));
        } finally {
            logger.removeHandler(handler);
        }
    }

    @Test
    void closeWithTwentyClientsStopsWithinFiveSeconds() throws Exception {
        List<String> lines = new CopyOnWriteArrayList<>();
        Logger logger = Logger.getLogger("dev.dess.broker");
        Handler handler = capture(lines);
        logger.setLevel(Level.INFO);
        logger.addHandler(handler);
        List<ProtocolClient> clients = new ArrayList<>();
        Broker broker = Broker.start(new BrokerConfig(0, 32, BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, 120_000));
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
            assertEquals(1, count(lines, "event=broker.stopped", "reason=CLOSE", "uptimeSeconds="));
            assertEquals(20, count(lines, "event=client.disconnected", "reason=SHUTDOWN"));
        } finally {
            for (ProtocolClient client : clients) {
                client.close();
            }
            broker.close();
            logger.removeHandler(handler);
        }
    }

    @Test
    void sigtermLogsSignalAndExitsZero() throws Exception {
        Path output = Files.createTempFile("dess-broker-out", ".txt");
        Path error = Files.createTempFile("dess-broker-err", ".txt");
        Process process = startBroker(output, error, "--port", "0");
        ProtocolClient client = null;
        try {
            String text = awaitText(process, output, "event=broker.started");
            assertTrue(text.contains("event=broker.started"), text + Files.readString(error));
            int port = portFrom(text);
            client = ProtocolClient.connect(port);
            client.hello();
            long started = System.nanoTime();
            Process kill = new ProcessBuilder("kill", "-TERM", Long.toString(process.pid())).start();
            assertEquals(0, kill.waitFor());
            assertTrue(process.waitFor(5, TimeUnit.SECONDS), Files.readString(output) + Files.readString(error));
            assertTrue(Duration.ofNanos(System.nanoTime() - started).compareTo(Duration.ofSeconds(5)) < 0);
            assertEquals(0, process.exitValue());
            String stopped = awaitText(process, output, "reason=SIGNAL");
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
            Files.deleteIfExists(output);
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
        return new ProcessBuilder(command).redirectOutput(output.toFile()).redirectError(error.toFile()).start();
    }

    private static String awaitText(Process process, Path output, String needle) throws IOException {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        String text = "";
        while (System.nanoTime() < deadline) {
            text = Files.readString(output);
            if (text.contains(needle)) {
                return text;
            }
        }
        return text;
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

    private static long count(List<String> lines, String... parts) {
        return lines.stream().filter(line -> containsAll(line, parts)).count();
    }

    private static boolean containsAll(String line, String... parts) {
        for (String part : parts) {
            if (!line.contains(part)) {
                return false;
            }
        }
        return true;
    }

    private static Handler capture(List<String> lines) {
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage() != null) {
                    lines.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        handler.setLevel(Level.ALL);
        return handler;
    }
}
