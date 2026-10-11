package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class ConfigLoadTest {

    private static final List<String> SETTINGS = List.of(
            "port",
            "maxConnections",
            "maxPayloadBytes",
            "idleTimeoutMs",
            "maxInFlight",
            "topicQueueCapacity",
            "outboundCapacity",
            "slowConsumerGraceMs",
            "publishWaitMs",
            "ackTimeoutMs",
            "threadKeepAliveMs",
            "shutdownTimeoutMs",
            "logDirectory",
            "durability",
            "syncIntervalMs",
            "autoCreate",
            "level",
            "keyStore",
            "trustStore",
            "passwordFile");

    @Test
    void fiveIndependentProblemsExitOnceWithoutStarting() throws Exception {
        Path directory = Files.createTempDirectory("dess-config");
        Path logs = directory.resolve("logs");
        Path file = directory.resolve("broker.xml");
        Files.writeString(file, """
                <broker>
                    <network>
                        <port>70000</port>
                        <maxConnections>0</maxConnections>
                    </network>
                    <engine>
                        <topicQueueCapacity>10</topicQueueCapacity>
                        <outboundCapacity>11</outboundCapacity>
                    </engine>
                    <storage>
                        <logDirectory>%s</logDirectory>
                    </storage>
                    <topics>
                        <topic>Sales</topic>
                    </topics>
                    <discoveryPort>1</discoveryPort>
                </broker>
                """.formatted(logs.toAbsolutePath()));
        try {
            Result result = run(directory, "--config", file.toString());
            assertEquals(1, result.exitCode(), result.stderr());
            List<String> lines = lines(result.stderr());
            assertEquals(5, lines.size(), result.stderr());
            String absolute = file.toAbsolutePath().toString();
            for (String line : lines) {
                assertTrue(line.contains(absolute), line);
            }
            assertTrue(result.stderr().contains("discoveryPort"), result.stderr());
            assertTrue(result.stderr().contains("port must be between 0 and 65535"), result.stderr());
            assertTrue(result.stderr().contains("maxConnections must be at least 1"), result.stderr());
            assertTrue(result.stderr().contains("invalid topic name Sales"), result.stderr());
            assertTrue(result.stderr().contains("outboundCapacity must not exceed topicQueueCapacity"), result.stderr());
            assertFalse(result.stdout().contains("event=broker.started"), result.stdout());
            assertFalse(result.stderr().contains("Usage:"), result.stderr());
            assertFalse(Files.exists(logs));
        } finally {
            delete(directory);
        }
    }

    @Test
    void missingFileNamesTheExpectedPath() throws Exception {
        Path directory = Files.createTempDirectory("dess-config");
        Path missing = directory.resolve("absent.xml");
        try {
            Result named = run(directory, "--config", missing.toString());
            assertEquals(1, named.exitCode(), named.stderr());
            assertTrue(named.stderr().contains(missing.toAbsolutePath().toString()), named.stderr());
            assertFalse(named.stderr().contains("Usage:"), named.stderr());

            Result omitted = run(directory);
            assertEquals(1, omitted.exitCode(), omitted.stderr());
            assertTrue(omitted.stderr().contains("config.xml"), omitted.stderr());
            assertFalse(omitted.stderr().contains("Usage:"), omitted.stderr());
        } finally {
            delete(directory);
        }
    }

    @Test
    void omittedElementsTakeTheBundledDefaults() throws Exception {
        String defaults = resource("/defaults.xml");
        for (String setting : SETTINGS) {
            assertTrue(defaults.contains(setting), setting);
        }
        assertFalse(defaults.contains("<topic>"));
        Map<String, String> table = readmeDefaults();
        for (String setting : SETTINGS) {
            assertTrue(table.containsKey(setting), setting);
        }
        assertEquals("", table.get("topics"));

        Path directory = Files.createTempDirectory("dess-config");
        Path file = directory.resolve("empty.xml");
        Files.writeString(file, "<broker/>");
        try {
            ConfigLoader.Result loaded = ConfigLoader.load(file);
            assertEquals(List.of(), loaded.problems(), loaded.problems()::toString);
            BrokerConfig config = loaded.config();
            assertEquals(Integer.parseInt(table.get("port")), config.port());
            assertEquals(Integer.parseInt(table.get("maxConnections")), config.maxConnections());
            assertEquals(Integer.parseInt(table.get("maxPayloadBytes")), config.maxPayloadBytes());
            assertEquals(Integer.parseInt(table.get("idleTimeoutMs")), config.idleTimeoutMillis());
            assertEquals(Integer.parseInt(table.get("maxInFlight")), config.maxInFlight());
            assertEquals(Integer.parseInt(table.get("topicQueueCapacity")), config.topicQueueCapacity());
            assertEquals(Integer.parseInt(table.get("outboundCapacity")), config.outboundCapacity());
            assertEquals(Integer.parseInt(table.get("slowConsumerGraceMs")), config.slowConsumerGraceMillis());
            assertEquals(Integer.parseInt(table.get("publishWaitMs")), config.publishWaitMillis());
            assertEquals(Integer.parseInt(table.get("ackTimeoutMs")), config.ackTimeoutMillis());
            assertEquals(Integer.parseInt(table.get("threadKeepAliveMs")), config.threadKeepAliveMillis());
            assertEquals(Integer.parseInt(table.get("shutdownTimeoutMs")), config.shutdownTimeoutMillis());
            assertEquals(Boolean.parseBoolean(table.get("autoCreate")), config.autoCreateTopics());
            assertEquals(List.of(), config.topics());
            assertEquals(Path.of(table.get("logDirectory")).toAbsolutePath().normalize(), config.logDirectory());
            assertEquals(table.get("durability"), config.durability().name());
            assertEquals(Integer.parseInt(table.get("syncIntervalMs")), config.syncIntervalMillis());
            assertEquals(table.get("level"), config.logLevel());
            assertEquals(Path.of(table.get("keyStore")).toAbsolutePath().normalize(), config.keyStore());
            assertEquals(Path.of(table.get("trustStore")).toAbsolutePath().normalize(), config.trustStore());
            assertEquals(Path.of(table.get("passwordFile")).toAbsolutePath().normalize(), config.passwordFile());
            assertTrue(config.discoveryEnabled());
            assertEquals(55124, config.discoveryPort());
            assertFalse(config.presenceEnabled());
            assertEquals(file.toAbsolutePath(), config.configPath());
        } finally {
            delete(directory);
        }
    }

    @Test
    void forkedBrokerLogsTheResolvedConfiguration() throws Exception {
        Path directory = Files.createTempDirectory("dess-config");
        Path logs = directory.resolve("logs");
        Path file = directory.resolve("broker.xml");
        Files.writeString(file, """
                <broker>
                    <network>
                        <port>0</port>
                        <maxConnections>4</maxConnections>
                    </network>
                    <storage>
                        <logDirectory>%s</logDirectory>
                    </storage>
                    <topics>
                        <topic>sales</topic>
                    </topics>
                </broker>
                """.formatted(logs.toAbsolutePath()));
        Process process = start(directory, "--config", file.toString());
        Stdout stdout = new Stdout(process);
        try {
            String text = stdout.await("event=broker.started");
            assertTrue(text.contains("event=config.effective"), text);
            assertTrue(text.contains("port=0"), text);
            assertTrue(text.contains("maxConnections=4"), text);
            assertTrue(text.contains("idleTimeoutMillis=30000"), text);
            assertTrue(text.contains("maxPayloadBytes=1048576"), text);
            assertTrue(text.contains("durability=ALWAYS"), text);
            assertTrue(text.contains("logLevel=INFO"), text);
            assertTrue(text.contains("keyStore="), text);
            assertTrue(text.contains("trustStore="), text);
            assertTrue(text.contains("passwordFile="), text);
            assertTrue(text.contains("discoveryEnabled=true"), text);
            assertTrue(text.contains("discoveryPort=55124"), text);
            assertTrue(text.contains("presenceEnabled=false"), text);
            assertTrue(text.contains("autoCreateTopics=true"), text);
            assertTrue(text.contains("syncIntervalMillis=1000"), text);
            assertTrue(text.contains("topics=sales"), text);
            assertTrue(text.contains("logDirectory=" + logs.toAbsolutePath()), text);
            assertTrue(text.contains("version=4"), text);
            assertTrue(text.contains("configPath=" + file.toAbsolutePath()), text);
            assertTrue(before(text, "event=config.effective", "event=broker.started"), text);
        } finally {
            process.destroyForcibly();
            process.waitFor(2, TimeUnit.SECONDS);
            delete(directory);
        }
    }

    @Test
    void logDirectoryWhoseParentIsAFileExitsOne() throws Exception {
        Path directory = Files.createTempDirectory("dess-config");
        Path parent = directory.resolve("not-a-directory");
        Files.writeString(parent, "file");
        Path logs = parent.resolve("logs");
        Path file = directory.resolve("broker.xml");
        Files.writeString(file, """
                <broker>
                    <network><port>0</port></network>
                    <storage><logDirectory>%s</logDirectory></storage>
                </broker>
                """.formatted(logs.toAbsolutePath()));
        try {
            Result result = run(directory, "--config", file.toString());
            assertEquals(1, result.exitCode(), result.stderr());
            assertTrue(result.stderr().contains(logs.toAbsolutePath().toString()), result.stderr());
            assertFalse((result.stdout() + result.stderr()).contains("event=broker.started"));
        } finally {
            delete(directory);
        }
    }

    @Test
    void unknownFlagExitsTwoWithUsage() throws Exception {
        Path directory = Files.createTempDirectory("dess-config");
        try {
            Result result = run(directory, "--bogus");
            assertEquals(2, result.exitCode(), result.stderr());
            assertEquals("", result.stdout());
            assertTrue(result.stderr().contains(BrokerMain.USAGE), result.stderr());
        } finally {
            delete(directory);
        }
    }

    private static Map<String, String> readmeDefaults() throws IOException {
        Path readme = readme();
        Map<String, String> table = new LinkedHashMap<>();
        for (String line : Files.readAllLines(readme)) {
            if (!line.startsWith("|")) {
                continue;
            }
            String[] cells = line.split("\\|", -1);
            if (cells.length < 3) {
                continue;
            }
            String name = cells[1].trim();
            String value = cells[2].trim();
            if (name.isEmpty() || "Setting".equals(name) || name.contains("-")) {
                continue;
            }
            table.put(name, value);
        }
        return table;
    }

    private static Path readme() {
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("README.md");
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("README.md was not found");
    }

    private static String resource(String name) throws IOException {
        try (InputStream input = ConfigLoadTest.class.getResourceAsStream(name)) {
            if (input == null) {
                throw new IOException("Missing " + name);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static boolean before(String text, String earlier, String later) {
        int first = text.indexOf(earlier);
        int second = text.indexOf(later);
        return first >= 0 && second > first;
    }

    private static List<String> lines(String text) {
        List<String> lines = new ArrayList<>();
        for (String line : text.split("\\R")) {
            if (!line.isBlank()) {
                lines.add(line);
            }
        }
        return lines;
    }

    private static Result run(Path directory, String... args) throws Exception {
        Path out = Files.createTempFile("dess-config-out", ".txt");
        Path err = Files.createTempFile("dess-config-err", ".txt");
        Process process = start(directory, out, err, args);
        try {
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(2, TimeUnit.SECONDS);
            }
            return new Result(process.exitValue(), Files.readString(out), Files.readString(err));
        } finally {
            Files.deleteIfExists(out);
            Files.deleteIfExists(err);
        }
    }

    private static Process start(Path directory, String... args) throws IOException {
        return start(directory, null, null, args);
    }

    private static Process start(Path directory, Path output, Path error, String... args) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add("dev.dess.broker.BrokerMain");
        command.addAll(List.of(args));
        ProcessBuilder builder = new ProcessBuilder(command).directory(directory.toFile());
        if (output != null) {
            builder.redirectOutput(output.toFile());
        }
        if (error != null) {
            builder.redirectError(error.toFile());
        }
        return builder.start();
    }

    private static void delete(Path root) throws IOException {
        if (root == null || Files.notExists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            List<Path> paths = walk.sorted(Comparator.reverseOrder()).toList();
            for (Path path : paths) {
                Files.deleteIfExists(path);
            }
        }
    }

    private record Result(int exitCode, String stdout, String stderr) {
    }

    private static final class Stdout {

        private final StringBuilder text = new StringBuilder();

        private final LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<>();

        private Stdout(Process process) {
            Thread reader = new Thread(() -> pump(process), "dess-config-stdout");
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
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
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
