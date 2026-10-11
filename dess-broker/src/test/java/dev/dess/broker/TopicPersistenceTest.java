package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import dev.dess.common.protocol.StartPosition;
import java.io.BufferedReader;
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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class TopicPersistenceTest {

    @Test
    void runtimeTopicSurvivesSigtermAndReplays() throws Exception {
        Path directory = Files.createTempDirectory("dess-topics");
        Path logs = directory.resolve("logs");
        Path config = directory.resolve("config.xml");
        writeConfig(config, logs, Durability.ALWAYS, 1_000);
        Path error = directory.resolve("err.txt");
        Process process = startBroker(config, error);
        Stdout stdout = new Stdout(process);
        ProtocolClient client = null;
        try {
            int port = portFrom(stdout.await("event=broker.started", Duration.ofSeconds(10)), error);
            client = ProtocolClient.connect(port);
            client.setReadTimeoutMillis(5_000);
            client.hello();
            client.publish("orders", "kept".getBytes(StandardCharsets.UTF_8));
            assertEquals(0L, readAck(client));
            assertTrue(Files.isRegularFile(logs.resolve("orders.log")));
            assertEquals(List.of("orders"), ConfigLoader.load(config).config().topics());
        } finally {
            if (client != null) {
                client.close();
            }
            signal(process, "-TERM");
        }
        ConfigLoader.Result loaded = ConfigLoader.load(config);
        assertTrue(loaded.problems().isEmpty(), loaded.problems()::toString);
        try (LogCapture capture = LogCapture.open();
                Broker broker = Broker.start(loaded.config());
                ProtocolClient subscriber = ProtocolClient.connect(broker.port())) {
            assertTrue(capture.await(Duration.ofSeconds(2), "event=topic.created", "topic=orders"), capture.lines()::toString);
            assertTrue(hasSource(capture.lines(), "orders"), capture.lines()::toString);
            subscriber.setReadTimeoutMillis(5_000);
            subscriber.hello();
            subscriber.subscribe("orders", StartPosition.earliest());
            Frame delivered = readDeliver(subscriber);
            assertEquals(0L, FrameCodec.delivery(delivered.header()).offset());
            assertEquals("kept", new String(delivered.payload(), StandardCharsets.UTF_8));
        } finally {
            delete(directory);
        }
    }

    @Test
    void temporarySiblingIsNotConfiguration() throws Exception {
        Path directory = Files.createTempDirectory("dess-topics");
        Path logs = directory.resolve("logs");
        Files.createDirectories(logs);
        Path config = directory.resolve("config.xml");
        writeConfig(config, logs, Durability.INTERVAL, 60_000, "sales", "billing");
        Files.writeString(Path.of(config.toString() + ".tmp"), "<broker><topics autoCreate=\"true\"><topic>ghost</topic></topics></broker>");
        Files.writeString(logs.resolve("notes.tmp"), "partial");
        Files.writeString(logs.resolve("sales.log.tmp"), "partial");
        ConfigLoader.Result loaded = ConfigLoader.load(config);
        assertTrue(loaded.problems().isEmpty(), loaded.problems()::toString);
        try (LogCapture capture = LogCapture.open();
                Broker broker = Broker.start(loaded.config())) {
            assertEquals(List.of("sales", "billing"), brokerConfigTopics(config));
            assertFalse(capture.lines().stream().anyMatch(line -> line.contains("topic=ghost")), capture.lines()::toString);
            assertFalse(Files.exists(logs.resolve("ghost.log")));
            assertFalse(Files.exists(logs.resolve("notes.tmp")));
            assertFalse(Files.exists(logs.resolve("sales.log.tmp")));
            assertTrue(broker.port() > 0);
        } finally {
            delete(directory);
        }
    }

    @Test
    void subscribedTopicWithoutAPublishSurvivesRestart() throws Exception {
        Path directory = Files.createTempDirectory("dess-topics");
        Path logs = directory.resolve("logs");
        Path config = directory.resolve("config.xml");
        writeConfig(config, logs, Durability.INTERVAL, 60_000);
        ConfigLoader.Result loaded = ConfigLoader.load(config);
        try (Broker broker = Broker.start(loaded.config());
                ProtocolClient client = ProtocolClient.connect(broker.port())) {
            client.setReadTimeoutMillis(5_000);
            client.hello();
            client.subscribe("quiet");
            awaitTopics(config, Duration.ofSeconds(2), "quiet");
            assertTrue(Files.isRegularFile(logs.resolve("quiet.log")));
        }
        assertEquals(List.of("quiet"), brokerConfigTopics(config));
        ConfigLoader.Result again = ConfigLoader.load(config);
        try (LogCapture capture = LogCapture.open();
                Broker broker = Broker.start(again.config())) {
            assertTrue(capture.await(Duration.ofSeconds(2), "event=topic.created", "topic=quiet"), capture.lines()::toString);
            assertTrue(hasSource(capture.lines(), "quiet"), capture.lines()::toString);
            assertTrue(Files.isRegularFile(logs.resolve("quiet.log")));
            assertTrue(broker.port() > 0);
        } finally {
            delete(directory);
        }
    }

    @Test
    void unsubscribeKeepsTheTopicAndTheLog() throws Exception {
        Path directory = Files.createTempDirectory("dess-topics");
        Path logs = directory.resolve("logs");
        Path config = directory.resolve("config.xml");
        writeConfig(config, logs, Durability.INTERVAL, 60_000);
        ConfigLoader.Result loaded = ConfigLoader.load(config);
        try (Broker broker = Broker.start(loaded.config());
                ProtocolClient client = ProtocolClient.connect(broker.port())) {
            client.setReadTimeoutMillis(5_000);
            client.hello();
            client.subscribe("sales");
            awaitTopics(config, Duration.ofSeconds(2), "sales");
            assertTrue(Files.isRegularFile(logs.resolve("sales.log")));
            client.unsubscribe("sales");
            assertTrue(Files.isRegularFile(logs.resolve("sales.log")));
            assertEquals(List.of("sales"), brokerConfigTopics(config));
        }
        ConfigLoader.Result again = ConfigLoader.load(config);
        try (Broker broker = Broker.start(again.config())) {
            assertEquals(List.of("sales"), brokerConfigTopics(config));
            assertTrue(Files.isRegularFile(logs.resolve("sales.log")));
            assertTrue(broker.port() > 0);
        } finally {
            delete(directory);
        }
    }

    @Test
    void killedRewriteLeavesAValidDocumentNamingAckedTopics() throws Exception {
        for (int trial = 0; trial < 20; trial++) {
            Path directory = Files.createTempDirectory("dess-topics");
            try {
                Path logs = directory.resolve("logs");
                Path config = directory.resolve("config.xml");
                writeConfig(config, logs, Durability.INTERVAL, 60_000);
                Path error = directory.resolve("err.txt");
                Process process = startBroker(config, error);
                Stdout stdout = new Stdout(process);
                ProtocolClient client = null;
                int acknowledgements = trial + 1;
                List<String> acked = new ArrayList<>();
                try {
                    int port = portFrom(stdout.await("event=broker.started", Duration.ofSeconds(10)), error);
                    client = ProtocolClient.connect(port);
                    client.setReadTimeoutMillis(10_000);
                    client.hello();
                    for (int index = 0; index < acknowledgements; index++) {
                        String topic = "t" + index;
                        client.publish(topic, new byte[] {(byte) index});
                        assertEquals(0L, readAck(client), topic);
                        acked.add(topic);
                    }
                } finally {
                    if (client != null) {
                        client.close();
                    }
                    signal(process, "-9");
                }
                ConfigLoader.Result loaded = ConfigLoader.load(config);
                int attempt = trial;
                assertTrue(loaded.problems().isEmpty(), () -> "trial " + attempt + " " + loaded.problems());
                assertTrue(loaded.config().topics().containsAll(acked), () -> "trial " + attempt + " file " + loaded.config().topics() + " acked " + acked);
            } finally {
                delete(directory);
            }
        }
    }

    private static boolean hasSource(List<String> lines, String topic) {
        for (String line : lines) {
            if (line.contains("event=topic.created") && line.contains("topic=" + topic)
                    && (line.contains("source=CONFIG") || line.contains("source=RECOVERED"))) {
                return true;
            }
        }
        return false;
    }

    private static List<String> brokerConfigTopics(Path config) throws IOException {
        ConfigLoader.Result loaded = ConfigLoader.load(config);
        assertTrue(loaded.problems().isEmpty(), loaded.problems()::toString);
        return loaded.config().topics();
    }

    private static void writeConfig(Path file, Path logs, Durability durability, int syncIntervalMillis, String... topics) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append("<broker>");
        builder.append("<network><port>0</port></network>");
        builder.append("<storage><logDirectory>").append(logs.toAbsolutePath()).append("</logDirectory>");
        builder.append("<durability>").append(durability.name()).append("</durability>");
        builder.append("<syncIntervalMs>").append(syncIntervalMillis).append("</syncIntervalMs></storage>");
        builder.append("<topics autoCreate=\"true\">");
        for (String topic : topics) {
            builder.append("<topic>").append(topic).append("</topic>");
        }
        builder.append("</topics>");
        builder.append(TestConfigs.securityXml());
        builder.append("</broker>");
        Files.writeString(file, builder.toString());
    }

    private static Process startBroker(Path config, Path error) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add("dev.dess.broker.BrokerMain");
        command.add("--config");
        command.add(config.toString());
        return new ProcessBuilder(command).redirectError(error.toFile()).start();
    }

    private static long readAck(ProtocolClient client) throws IOException {
        Frame frame = client.readFrame();
        assertEquals(Command.ACK, frame.command(), frame.command()::name);
        return client.brokerAck(frame).offset();
    }

    private static Frame readDeliver(ProtocolClient client) throws IOException {
        while (true) {
            Frame frame = client.readFrame();
            if (frame.command() == Command.DELIVER) {
                return frame;
            }
        }
    }

    private static void signal(Process process, String signal) throws Exception {
        if (!process.isAlive()) {
            return;
        }
        Process kill = new ProcessBuilder("kill", signal, Long.toString(process.pid())).start();
        assertEquals(0, kill.waitFor());
        assertTrue(process.waitFor(10, TimeUnit.SECONDS));
    }

    private static int portFrom(String text, Path error) throws IOException {
        String marker = "tcpPort=";
        int at = text.indexOf(marker);
        if (at < 0) {
            throw new AssertionError(text + Files.readString(error));
        }
        int start = at + marker.length();
        int end = start;
        while (end < text.length() && Character.isDigit(text.charAt(end))) {
            end++;
        }
        return Integer.parseInt(text.substring(start, end));
    }

    private static void awaitTopics(Path config, Duration timeout, String... names) throws Exception {
        List<String> expected = List.of(names);
        Path directory = config.getParent();
        try (WatchService watch = FileSystems.getDefault().newWatchService()) {
            directory.register(watch, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
            long deadline = System.nanoTime() + timeout.toNanos();
            while (!expected.equals(topicsOrEmpty(config))) {
                long wait = deadline - System.nanoTime();
                if (wait <= 0) {
                    throw new AssertionError(topicsOrEmpty(config).toString());
                }
                WatchKey key = watch.poll(Math.min(wait, Duration.ofMillis(200).toNanos()), TimeUnit.NANOSECONDS);
                if (key != null) {
                    key.pollEvents();
                    key.reset();
                }
            }
        }
    }

    private static List<String> topicsOrEmpty(Path config) {
        try {
            ConfigLoader.Result loaded = ConfigLoader.load(config);
            if (!loaded.problems().isEmpty() || loaded.config() == null) {
                return List.of();
            }
            return loaded.config().topics();
        } catch (IOException ex) {
            return List.of();
        }
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

    private static final class Stdout {

        private final StringBuilder text = new StringBuilder();

        private final LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<>();

        private Stdout(Process process) {
            Thread reader = new Thread(() -> pump(process), "dess-topic-stdout");
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

        private String text() {
            synchronized (text) {
                return text.toString();
            }
        }
    }

}
