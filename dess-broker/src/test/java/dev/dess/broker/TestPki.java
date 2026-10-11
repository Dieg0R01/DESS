package dev.dess.broker;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

public final class TestPki {

    private static final ReentrantLock LOCK = new ReentrantLock();

    private static Path directory;

    private static Path untrustedDirectory;

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
        return untrustedDirectory().resolve("producer.p12");
    }

    public static Path untrustedTrustStore() {
        return untrustedDirectory().resolve("trust.p12");
    }

    public static Path untrustedPasswordFile() {
        return untrustedDirectory().resolve("password");
    }

    public static char[] password() {
        try {
            return TlsMaterial.firstLine(passwordFile()).toCharArray();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public static SSLContext clientContext(Path identity, Path trust, Path passwordFile) {
        try {
            char[] secret = TlsMaterial.firstLine(passwordFile).toCharArray();
            KeyStore keys = store(identity, secret);
            KeyStore trusted = store(trust, secret);
            KeyManagerFactory keysFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keysFactory.init(keys, secret);
            TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustFactory.init(trusted);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(new KeyManager[] {presentEvenIfExpired(keysFactory.getKeyManagers(), keys, secret)},
                    trustFactory.getTrustManagers(), null);
            return context;
        } catch (IOException | GeneralSecurityException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public static SSLSocket anonymousSocket(int port) throws IOException {
        try {
            KeyStore trusted = store(trustStore(), password());
            TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustFactory.init(trusted);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trustFactory.getTrustManagers(), null);
            SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket();
            socket.setTcpNoDelay(true);
            socket.setEnabledProtocols(TlsMaterial.PROTOCOLS);
            SSLParameters parameters = socket.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm(null);
            socket.setSSLParameters(parameters);
            socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 2_000);
            return socket;
        } catch (GeneralSecurityException ex) {
            throw new IOException(ex);
        }
    }

    public static SSLSocket clientSocket(int port) throws IOException {
        return clientSocket(port, producerStore(), trustStore(), passwordFile());
    }

    public static SSLSocket clientSocket(int port, Path identity, Path trust, Path passwordFile) throws IOException {
        SSLContext context = clientContext(identity, trust, passwordFile);
        SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket();
        socket.setTcpNoDelay(true);
        socket.setEnabledProtocols(TlsMaterial.PROTOCOLS);
        SSLParameters parameters = socket.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm(null);
        socket.setSSLParameters(parameters);
        socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 2_000);
        return socket;
    }

    private static Path untrustedDirectory() {
        if (untrustedDirectory != null) {
            return untrustedDirectory;
        }
        LOCK.lock();
        try {
            if (untrustedDirectory != null) {
                return untrustedDirectory;
            }
            Path other = moduleRoot().resolve("target").resolve("test-pki-untrusted");
            runScript(other);
            untrustedDirectory = other;
            return untrustedDirectory;
        } finally {
            LOCK.unlock();
        }
    }

    private static X509ExtendedKeyManager presentEvenIfExpired(KeyManager[] managers, KeyStore keys, char[] secret)
            throws GeneralSecurityException {
        X509ExtendedKeyManager delegate = null;
        for (KeyManager manager : managers) {
            if (manager instanceof X509ExtendedKeyManager extended) {
                delegate = extended;
                break;
            }
        }
        if (delegate == null) {
            throw new IllegalStateException("X509 key manager is required");
        }
        String alias = null;
        for (String candidate : Collections.list(keys.aliases())) {
            if (keys.isKeyEntry(candidate)) {
                alias = candidate;
                break;
            }
        }
        if (alias == null) {
            throw new IllegalStateException("identity store has no private key");
        }
        PrivateKey key = (PrivateKey) keys.getKey(alias, secret);
        java.security.cert.Certificate[] chain = keys.getCertificateChain(alias);
        X509Certificate[] x509 = new X509Certificate[chain.length];
        for (int index = 0; index < chain.length; index++) {
            x509[index] = (X509Certificate) chain[index];
        }
        return new ForcedClientKey(delegate, alias, key, x509);
    }

    private static KeyStore store(Path path, char[] secret) throws IOException, GeneralSecurityException {
        try (InputStream input = Files.newInputStream(path)) {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(input, secret);
            return store;
        }
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
            Path out = repositoryRoot().resolve("target").resolve("test-pki");
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
        if (dir.getFileName() != null && "dess-broker".equals(dir.getFileName().toString())) {
            return dir;
        }
        Path nested = dir.resolve("dess-broker");
        if (Files.isDirectory(nested)) {
            return nested;
        }
        Path fromRepo = repositoryRoot().resolve("dess-broker");
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

    private static final class ForcedClientKey extends X509ExtendedKeyManager {

        private final X509ExtendedKeyManager delegate;
        private final String alias;
        private final PrivateKey key;
        private final X509Certificate[] chain;

        private ForcedClientKey(X509ExtendedKeyManager delegate, String alias, PrivateKey key, X509Certificate[] chain) {
            this.delegate = delegate;
            this.alias = alias;
            this.key = key;
            this.chain = chain;
        }

        @Override
        public String[] getClientAliases(String keyType, Principal[] issuers) {
            return new String[] {alias};
        }

        @Override
        public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
            return alias;
        }

        @Override
        public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
            return alias;
        }

        @Override
        public String[] getServerAliases(String keyType, Principal[] issuers) {
            return delegate.getServerAliases(keyType, issuers);
        }

        @Override
        public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
            return delegate.chooseServerAlias(keyType, issuers, socket);
        }

        @Override
        public X509Certificate[] getCertificateChain(String chosen) {
            return alias.equals(chosen) ? chain : null;
        }

        @Override
        public PrivateKey getPrivateKey(String chosen) {
            return alias.equals(chosen) ? key : null;
        }
    }
}
