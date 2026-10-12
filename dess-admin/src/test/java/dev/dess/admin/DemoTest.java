package dev.dess.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class DemoTest {

    @Test
    void scriptedRunCoversEveryStepAndLeavesNoChildren() throws Exception {
        Path root = repositoryRoot();
        Path pki = Files.createTempDirectory("dess-demo-pki");
        generateCerts(root, pki);
        int runs = Integer.getInteger("dess.demo.runs", 1);
        try {
            for (int run = 0; run < runs; run++) {
                Path work = Files.createTempDirectory("dess-demo-run");
                Path logs = work.resolve("logs");
                Path workdir = work.resolve("workdir");
                Files.createDirectories(logs);
                Files.createDirectories(workdir);
                Path config = writeConfig(work, logs, pki);
                Result result = runDemo(root, config, workdir, 3, TimeUnit.MINUTES);
                try {
                    assertEquals(0, result.exitCode(), result.output());
                    assertTrue(result.output().contains("event=broker.started"), result.output());
                    assertTrue(result.output().contains("INTEGRITY_FAILED"), result.output());
                    assertTrue(
                            result.output().contains("STATUS UP") || result.output().contains("\"status\""),
                            result.output());
                    assertTrue(result.output().contains("event=broker.stopped"), result.output());
                    assertTrue(result.output().contains("reason=SIGNAL"), result.output());
                    assertTrue(result.output().contains("event=log.recovered"), result.output());
                    assertTrue(result.output().contains("demo charlie"), result.output());
                    assertFalse(result.childrenAlive, result.output());
                } finally {
                    delete(work);
                }
            }
        } finally {
            delete(pki);
        }
    }

    @Test
    void firstFailedStepIsNamed() throws Exception {
        Path root = repositoryRoot();
        Path work = Files.createTempDirectory("dess-demo-fail");
        Path logs = work.resolve("logs");
        Path workdir = work.resolve("workdir");
        Files.createDirectories(logs);
        Files.createDirectories(workdir);
        Path brokerStore = work.resolve("broker.p12");
        Path trustStore = work.resolve("trust.p12");
        Path password = work.resolve("password");
        Files.writeString(brokerStore, "not-a-keystore");
        Files.writeString(trustStore, "not-a-keystore");
        Files.writeString(password, "secret");
        Files.writeString(work.resolve("producer.p12"), "not-a-keystore");
        Files.writeString(work.resolve("consumer.p12"), "not-a-keystore");
        Path config = work.resolve("config.xml");
        Files.writeString(config, """
                <broker>
                    <network><port>0</port></network>
                    <storage>
                        <logDirectory>%s</logDirectory>
                        <durability>ALWAYS</durability>
                    </storage>
                    <security>
                        <keyStore>%s</keyStore>
                        <trustStore>%s</trustStore>
                        <passwordFile>%s</passwordFile>
                    </security>
                    <discovery enabled="true" presence="false"><port>0</port></discovery>
                    <admin><bindAddress>127.0.0.1</bindAddress><port>0</port></admin>
                </broker>
                """.formatted(
                logs.toAbsolutePath(),
                brokerStore.toAbsolutePath(),
                trustStore.toAbsolutePath(),
                password.toAbsolutePath()));
        try {
            Result result = runDemo(root, config, workdir, 30, TimeUnit.SECONDS);
            assertEquals(1, result.exitCode(), result.output());
            assertTrue(result.output().contains("failed step: start-broker"), result.output());
        } finally {
            delete(work);
        }
    }

    private static Result runDemo(Path root, Path config, Path work, long timeout, TimeUnit unit)
            throws IOException, InterruptedException {
        Path script = root.resolve("scripts").resolve("demo.sh");
        Path output = work.resolve("demo.out");
        List<String> command = new ArrayList<>();
        command.add("bash");
        command.add(script.toString());
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.directory(root.toFile());
        builder.redirectErrorStream(true);
        builder.redirectOutput(output.toFile());
        builder.environment().put("DEMO_CONFIG", config.toAbsolutePath().toString());
        builder.environment().put("DEMO_WORKDIR", work.toAbsolutePath().toString());
        builder.environment().put("JAVA_HOME", System.getProperty("java.home"));
        Process process = builder.start();
        try {
            if (!process.waitFor(timeout, unit)) {
                destroyTree(process);
                process.waitFor(5, TimeUnit.SECONDS);
            }
            String text = Files.exists(output) ? Files.readString(output) : "";
            boolean childrenAlive = process.descendants().anyMatch(ProcessHandle::isAlive);
            return new Result(process.isAlive() ? -1 : process.exitValue(), text, childrenAlive);
        } finally {
            destroyTree(process);
        }
    }

    private static Path writeConfig(Path work, Path logs, Path pki) throws IOException {
        Path config = work.resolve("config.xml");
        Files.writeString(config, """
                <broker>
                    <network><port>0</port></network>
                    <storage>
                        <logDirectory>%s</logDirectory>
                        <durability>ALWAYS</durability>
                    </storage>
                    <topics autoCreate="true">
                        <topic>demo</topic>
                    </topics>
                    <security>
                        <keyStore>%s</keyStore>
                        <trustStore>%s</trustStore>
                        <passwordFile>%s</passwordFile>
                    </security>
                    <discovery enabled="true" presence="false"><port>0</port></discovery>
                    <admin>
                        <bindAddress>127.0.0.1</bindAddress>
                        <port>0</port>
                    </admin>
                </broker>
                """.formatted(
                logs.toAbsolutePath(),
                pki.resolve("broker.p12").toAbsolutePath(),
                pki.resolve("trust.p12").toAbsolutePath(),
                pki.resolve("password").toAbsolutePath()));
        return config;
    }

    private static void generateCerts(Path root, Path out) throws IOException, InterruptedException {
        Files.createDirectories(out);
        List<String> command = new ArrayList<>();
        command.add("bash");
        command.add(script(root).toString());
        command.add("--out");
        command.add(out.toString());
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.environment().put("JAVA_HOME", System.getProperty("java.home"));
        builder.redirectErrorStream(true);
        Process process = builder.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!process.waitFor(2, TimeUnit.MINUTES) || process.exitValue() != 0) {
            process.destroyForcibly();
            throw new IllegalStateException("generate-certs.sh failed:\n" + output);
        }
    }

    private static Path script(Path root) {
        Path nested = root.resolve("config").resolve("generate-certs.sh");
        if (Files.isRegularFile(nested)) {
            return nested;
        }
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("config").resolve("generate-certs.sh");
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("Could not find config/generate-certs.sh from " + Path.of("").toAbsolutePath());
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
        if (Files.isDirectory(current.resolve("dess-admin")) && Files.isDirectory(current.resolve("scripts"))) {
            return current;
        }
        throw new IllegalStateException("Could not find the repository root from " + current);
    }

    private static void destroyTree(Process process) {
        process.descendants().forEach(ProcessHandle::destroy);
        process.destroy();
        try {
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.descendants().forEach(ProcessHandle::destroyForcibly);
                process.destroyForcibly();
                process.waitFor(2, TimeUnit.SECONDS);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }

    private static void delete(Path root) throws IOException {
        if (root == null || Files.notExists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            List<Path> paths = walk.sorted(Comparator.reverseOrder()).toList();
            for (Path path : paths) {
                Files.deleteIfExists(path);
            }
        }
    }

    private record Result(int exitCode, String output, boolean childrenAlive) {
    }
}
