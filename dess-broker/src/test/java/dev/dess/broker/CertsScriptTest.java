package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class CertsScriptTest {

    private static final List<String> BASE_FILES = List.of(
            "password", "ca.p12", "trust.p12", "broker.p12", "producer.p12", "consumer.p12");

    @Test
    void cleanOutCreatesTheSixFilesAndASecondRunKeepsThem() throws Exception {
        Path out = Files.createTempDirectory("dess-certs");
        long started = System.nanoTime();
        Result first = run(out);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertEquals(0, first.exitCode(), first.output());
        assertTrue(elapsedMs < TimeUnit.MINUTES.toMillis(5), "first run took " + elapsedMs + " ms");
        Map<String, byte[]> created = snapshot(out);
        assertEquals(6, created.size());
        for (String name : BASE_FILES) {
            assertTrue(first.output().contains("created " + out.resolve(name)), first.output());
        }

        Result second = run(out);
        assertEquals(0, second.exitCode(), second.output());
        for (String name : BASE_FILES) {
            assertTrue(second.output().contains("kept " + out.resolve(name)), second.output());
            assertEquals(created.get(name).length, Files.size(out.resolve(name)));
            assertTrue(Arrays.equals(created.get(name), Files.readAllBytes(out.resolve(name))), name);
        }
    }

    @Test
    void forceRewritesTheSixBaseFiles() throws Exception {
        Path out = Files.createTempDirectory("dess-certs-force");
        Result first = run(out);
        assertEquals(0, first.exitCode(), first.output());
        Map<String, byte[]> before = snapshot(out);

        Result forced = run(out, "--force");
        assertEquals(0, forced.exitCode(), forced.output());
        for (String name : BASE_FILES) {
            assertTrue(forced.output().contains("created " + out.resolve(name)), forced.output());
            assertTrue(Files.isRegularFile(out.resolve(name)), name);
        }
        assertNotEquals(
                Arrays.hashCode(before.get("password")),
                Arrays.hashCode(Files.readAllBytes(out.resolve("password"))));
    }

    @Test
    void extraProducerClientIsSignedByTheSameCaAndCanPublish() throws Exception {
        Path alice = TestPki.client("alice", "producer");
        assertTrue(Files.isRegularFile(alice), alice.toString());
        X509Certificate aliceCert = leaf(alice, "alice");
        X509Certificate producerCert = leaf(TestPki.producerStore(), "producer");
        assertEquals(producerCert.getIssuerX500Principal(), aliceCert.getIssuerX500Principal());
        try (LogCapture logs = LogCapture.open()) {
            logs.quiet();
            try (BrokerHarness harness = BrokerHarness.start();
                    ProtocolClient client = ProtocolClient.connect(harness.port(), alice)) {
                client.setReadTimeoutMillis(2_000);
                client.hello();
                client.publish("sales", "from-alice".getBytes(StandardCharsets.UTF_8));
                assertEquals(Command.ACK, client.readFrame().command());
            }
        }
    }

    @Test
    void clientWithoutRoleExitsTwo() throws Exception {
        Result result = run(null, "--client", "alice");
        assertEquals(2, result.exitCode(), result.output());
        assertTrue(result.output().contains("Usage:"), result.output());
    }

    @Test
    void gitignoreCoversBrokerStoreAndPassword() throws Exception {
        Path repo = repositoryRoot();
        assertIgnored(repo, "config/pki/broker.p12");
        assertIgnored(repo, "config/pki/password");
    }

    private static void assertIgnored(Path repo, String path) throws IOException, InterruptedException {
        List<String> command = List.of("git", "-C", repo.toString(), "check-ignore", "-v", path);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(process.waitFor(5, TimeUnit.SECONDS), output);
        assertEquals(0, process.exitValue(), output);
        assertTrue(output.contains(path), output);
    }

    private static Map<String, byte[]> snapshot(Path out) throws IOException {
        Map<String, byte[]> bytes = new LinkedHashMap<>();
        for (String name : BASE_FILES) {
            Path file = out.resolve(name);
            assertTrue(Files.isRegularFile(file), file.toString());
            bytes.put(name, Files.readAllBytes(file));
        }
        return bytes;
    }

    private static X509Certificate leaf(Path store, String alias) throws Exception {
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, TestPki.password());
        }
        return (X509Certificate) keys.getCertificate(alias);
    }

    private static Result run(Path out, String... extra) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add("bash");
        command.add(script().toString());
        if (out != null) {
            command.add("--out");
            command.add(out.toString());
        }
        command.addAll(List.of(extra));
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.environment().put("JAVA_HOME", System.getProperty("java.home"));
        builder.redirectErrorStream(true);
        Process process = builder.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(process.waitFor(5, TimeUnit.MINUTES), output);
        return new Result(process.exitValue(), output);
    }

    private static Path script() {
        return repositoryRoot().resolve("config").resolve("generate-certs.sh");
    }

    private static Path repositoryRoot() {
        Path dir = Path.of("").toAbsolutePath();
        if (Files.isRegularFile(dir.resolve("config").resolve("generate-certs.sh"))) {
            return dir;
        }
        Path parent = dir.getParent();
        if (parent != null && Files.isRegularFile(parent.resolve("config").resolve("generate-certs.sh"))) {
            return parent;
        }
        throw new IllegalStateException("Could not find config/generate-certs.sh from " + dir);
    }

    private record Result(int exitCode, String output) {
    }
}
