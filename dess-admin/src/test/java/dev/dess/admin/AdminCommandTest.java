package dev.dess.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.json.Json;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class AdminCommandTest {

    private static final String TOPICS_HEADER = "NAME                 RETAINED  SUBSCRIBERS  QUEUE";

    @Test
    void commandsCallTheLiveApiAndPrintTableOrJson() throws Exception {
        Path error = Files.createTempFile("dess-admin-broker-err", ".txt");
        Process broker = startBroker(error);
        BrokerStdout stdout = new BrokerStdout(broker);
        try {
            String ready;
            try {
                ready = stdout.await("admin.ready", Duration.ofSeconds(15));
            } catch (AssertionError failure) {
                throw new AssertionError(failure.getMessage() + Files.readString(error), failure);
            }
            int adminPort = portFrom(ready, "adminPort=");
            String origin = "http://127.0.0.1:" + adminPort;

            Result health = run("--url", origin, "health");
            assertEquals(0, health.exitCode(), health.stderr());
            assertEquals("STATUS UP" + System.lineSeparator(), health.stdout());

            Result healthJson = run("--url", origin, "--json", "health");
            assertEquals(0, healthJson.exitCode(), healthJson.stderr());
            assertEquals(get(origin, "/health").body(), healthJson.stdout());

            Result created = run("--url", origin, "topics", "create", "ops");
            assertEquals(0, created.exitCode(), created.stderr());
            assertEquals(TOPICS_HEADER, created.stdout().lines().findFirst().orElse(""));
            assertTrue(created.stdout().contains("ops"), created.stdout());

            Result createdJson = run("--url", origin, "--json", "topics", "create", "sales");
            assertEquals(0, createdJson.exitCode(), createdJson.stderr());
            Map<String, Object> createdBody = Json.object(createdJson.stdout());
            assertEquals("sales", createdBody.get("name"));
            assertEquals(0L, ((Number) createdBody.get("retainedCount")).longValue());

            Result listed = run("--url", origin, "topics", "list");
            assertEquals(0, listed.exitCode(), listed.stderr());
            assertEquals(TOPICS_HEADER, listed.stdout().lines().findFirst().orElse(""));
            assertTrue(listed.stdout().contains("ops"), listed.stdout());
            assertTrue(listed.stdout().contains("sales"), listed.stdout());

            Result listedJson = run("--url", origin, "--json", "topics", "list");
            assertEquals(0, listedJson.exitCode(), listedJson.stderr());
            assertEquals(get(origin, "/topics").body(), listedJson.stdout());
            Map<String, Object> listBody = Json.object(listedJson.stdout());
            assertTrue(listBody.get("topics") instanceof List<?>);

            Result metrics = run("--url", origin, "metrics");
            assertEquals(0, metrics.exitCode(), metrics.stderr());
            assertTrue(metrics.stdout().contains("takenAt "), metrics.stdout());
            assertTrue(metrics.stdout().contains("messagesInPerSecond "), metrics.stdout());
            assertTrue(metrics.stdout().contains("messagesOutPerSecond "), metrics.stdout());
            assertTrue(metrics.stdout().contains("activeConnections "), metrics.stdout());
            assertTrue(metrics.stdout().contains("activeThreads "), metrics.stdout());
            assertTrue(metrics.stdout().contains("heapUsedBytes "), metrics.stdout());
            assertTrue(metrics.stdout().contains("heapMaxBytes "), metrics.stdout());
            assertTrue(metrics.stdout().contains("inFlight "), metrics.stdout());
            assertTrue(metrics.stdout().contains("uptimeSeconds "), metrics.stdout());
            assertTrue(metrics.stdout().contains("QUEUE ops "), metrics.stdout());
            assertTrue(metrics.stdout().contains("QUEUE sales "), metrics.stdout());

            Result metricsJson = run("--url", origin, "--json", "metrics");
            assertEquals(0, metricsJson.exitCode(), metricsJson.stderr());
            Map<String, Object> metricsBody = Json.object(metricsJson.stdout());
            assertTrue(metricsBody.containsKey("takenAt"));
            assertTrue(metricsBody.containsKey("messagesInPerSecond"));
            assertTrue(metricsBody.get("topics") instanceof List<?>);

            Result deleted = run("--url", origin, "topics", "delete", "ops");
            assertEquals(0, deleted.exitCode(), deleted.stderr());
            assertEquals("", deleted.stdout());

            Result deletedJson = run("--url", origin, "--json", "topics", "delete", "sales");
            assertEquals(0, deletedJson.exitCode(), deletedJson.stderr());
            assertEquals("", deletedJson.stdout());

            Result missing = run("--url", origin, "topics", "delete", "missing");
            assertEquals(1, missing.exitCode(), missing.stderr());
            assertEquals("", missing.stdout());
            assertTrue(missing.stderr().contains("404"), missing.stderr());
            assertTrue(missing.stderr().contains("Unknown topic"), missing.stderr());
        } finally {
            broker.destroyForcibly();
            broker.waitFor(5, TimeUnit.SECONDS);
            Files.deleteIfExists(error);
        }
    }

    @Test
    void unknownCommandOrMissingNameExitsTwo() throws Exception {
        Result unknown = run("nope");
        assertEquals(2, unknown.exitCode());
        assertEquals("", unknown.stdout());
        assertUsage(unknown.stderr());

        Result missingSubcommand = run("topics");
        assertEquals(2, missingSubcommand.exitCode());
        assertUsage(missingSubcommand.stderr());

        Result missingCreateName = run("topics", "create");
        assertEquals(2, missingCreateName.exitCode());
        assertUsage(missingCreateName.stderr());

        Result missingDeleteName = run("topics", "delete");
        assertEquals(2, missingDeleteName.exitCode());
        assertUsage(missingDeleteName.stderr());
    }

    @Test
    void urlFlagWithoutValueExitsTwo() throws Exception {
        Result missing = run("--url");
        assertEquals(2, missing.exitCode());
        assertEquals("", missing.stdout());
        assertUsage(missing.stderr());
    }

    private static void assertUsage(String stderr) {
        String[] lines = stderr.split("\\R", -1);
        assertEquals(2, lines.length);
        assertEquals("", lines[1]);
        assertTrue(lines[0].matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{2}:\\d{2} INFO \\S+ \\S+ \\Q"
                + AdminMain.USAGE + "\\E"), lines[0]);
    }

    private static HttpResponse<String> get(String origin, String path) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(origin + path)).GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private static Result run(String... args) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add("dev.dess.admin.AdminMain");
        command.addAll(List.of(args));
        Path out = Files.createTempFile("dess-admin-out", ".txt");
        Path err = Files.createTempFile("dess-admin-err", ".txt");
        Process process = new ProcessBuilder(command).redirectOutput(out.toFile()).redirectError(err.toFile()).start();
        try {
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(2, TimeUnit.SECONDS);
            }
            return new Result(process.exitValue(), Files.readString(out), Files.readString(err));
        } finally {
            Files.deleteIfExists(out);
            Files.deleteIfExists(err);
        }
    }

    private static Process startBroker(Path error) throws IOException {
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
        command.add("dev.dess.broker.AdminTrial");
        return new ProcessBuilder(command).redirectError(error.toFile()).start();
    }

    private static Path repositoryRoot() {
        Path current = Path.of("").toAbsolutePath().normalize();
        if (current.getFileName() != null) {
            String name = current.getFileName().toString();
            if ("dess-admin".equals(name) || "dess-broker".equals(name) || "dess-client".equals(name)
                    || "dess-common".equals(name)) {
                return current.getParent();
            }
        }
        if (Files.isDirectory(current.resolve("dess-admin")) && Files.isDirectory(current.resolve("dess-broker"))) {
            return current;
        }
        throw new IllegalStateException("Could not find the repository root from " + current);
    }

    private static int portFrom(String text, String marker) {
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

    private record Result(int exitCode, String stdout, String stderr) {
    }

    private static final class BrokerStdout {

        private final StringBuilder text = new StringBuilder();
        private final LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<>();

        private BrokerStdout(Process process) {
            Thread reader = new Thread(() -> pump(process), "dess-admin-broker-stdout");
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
                // The broker process closed its output.
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
