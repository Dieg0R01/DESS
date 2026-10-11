package dev.dess.client;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public final class TestPki {

    private static final ReentrantLock LOCK = new ReentrantLock();

    private static Path directory;

    private TestPki() {
    }

    public static Path directory() {
        ensure();
        return directory;
    }

    public static Path passwordFile() {
        return directory().resolve("password");
    }

    public static Path brokerStore() {
        return directory().resolve("broker.p12");
    }

    public static Path trustStore() {
        return directory().resolve("trust.p12");
    }

    public static Path producerStore() {
        return directory().resolve("producer.p12");
    }

    public static Path consumerStore() {
        return directory().resolve("consumer.p12");
    }

    public static Path client(String name, String role) {
        ensure();
        runScript(directory, "--client", name, "--role", role);
        return directory.resolve(name + ".p12");
    }

    public static Path expiredClient() {
        ensure();
        Path store = directory.resolve("expired.p12");
        if (Files.isRegularFile(store)) {
            return store;
        }
        LOCK.lock();
        try {
            if (Files.isRegularFile(store)) {
                return store;
            }
            issueExpired(store);
            return store;
        } catch (IOException | InterruptedException ex) {
            throw new UncheckedIOException(ex instanceof IOException io ? io : new IOException(ex));
        } finally {
            LOCK.unlock();
        }
    }

    public static Path untrustedClient() {
        ensure();
        Path other = moduleRoot().resolve("target").resolve("test-pki-untrusted");
        runScript(other);
        return other.resolve("producer.p12");
    }

    private static void ensure() {
        if (directory != null) {
            return;
        }
        LOCK.lock();
        try {
            if (directory != null) {
                return;
            }
            Path out = moduleRoot().resolve("target").resolve("test-pki");
            runScript(out);
            directory = out;
        } finally {
            LOCK.unlock();
        }
    }

    private static void runScript(Path out, String... extra) {
        try {
            Files.createDirectories(out);
            List<String> command = new ArrayList<>();
            command.add("bash");
            command.add(script().toString());
            command.add("--out");
            command.add(out.toString());
            command.addAll(List.of(extra));
            ProcessBuilder builder = new ProcessBuilder(command);
            builder.environment().put("JAVA_HOME", System.getProperty("java.home"));
            builder.redirectErrorStream(true);
            Process process = builder.start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (process.waitFor() != 0) {
                throw new IllegalStateException("generate-certs.sh failed:\n" + output);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(ex);
        }
    }

    private static void issueExpired(Path store) throws IOException, InterruptedException {
        Path work = Files.createTempDirectory(directory, "expired");
        String password = Files.readString(passwordFile()).trim();
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        runKeytool(keytool, "-genkeypair", "-alias", "expired", "-keyalg", "EC", "-groupname", "secp256r1",
                "-validity", "1", "-dname", "CN=expired, OU=producer", "-keystore", store.toString(),
                "-storetype", "PKCS12", "-storepass", password, "-keypass", password);
        Path csr = work.resolve("expired.csr");
        Path crt = work.resolve("expired.crt");
        Path caCert = work.resolve("ca.crt");
        runKeytool(keytool, "-certreq", "-alias", "expired", "-keystore", store.toString(),
                "-storetype", "PKCS12", "-storepass", password, "-file", csr.toString());
        runKeytool(keytool, "-gencert", "-alias", "ca", "-keystore", directory.resolve("ca.p12").toString(),
                "-storetype", "PKCS12", "-storepass", password, "-infile", csr.toString(),
                "-outfile", crt.toString(), "-startdate", "-3d", "-validity", "1", "-rfc");
        runKeytool(keytool, "-exportcert", "-alias", "ca", "-keystore", directory.resolve("ca.p12").toString(),
                "-storetype", "PKCS12", "-storepass", password, "-rfc", "-file", caCert.toString());
        runKeytool(keytool, "-importcert", "-alias", "ca", "-file", caCert.toString(), "-keystore", store.toString(),
                "-storetype", "PKCS12", "-storepass", password, "-noprompt");
        runKeytool(keytool, "-importcert", "-alias", "expired", "-file", crt.toString(), "-keystore", store.toString(),
                "-storetype", "PKCS12", "-storepass", password, "-noprompt");
    }

    private static void runKeytool(String keytool, String... args) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(keytool);
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (process.waitFor() != 0) {
            throw new IOException("keytool failed:\n" + output);
        }
    }

    private static Path script() {
        return repositoryRoot().resolve("config").resolve("generate-certs.sh");
    }

    private static Path moduleRoot() {
        Path dir = Path.of("").toAbsolutePath();
        if (dir.getFileName() != null && "dess-client".equals(dir.getFileName().toString())) {
            return dir;
        }
        Path nested = dir.resolve("dess-client");
        if (Files.isDirectory(nested)) {
            return nested;
        }
        Path fromRepo = repositoryRoot().resolve("dess-client");
        if (Files.isDirectory(fromRepo)) {
            return fromRepo;
        }
        return dir;
    }

    private static Path repositoryRoot() {
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null) {
            if (Files.isRegularFile(dir.resolve("config").resolve("generate-certs.sh"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("Could not find config/generate-certs.sh from " + Path.of("").toAbsolutePath());
    }
}
