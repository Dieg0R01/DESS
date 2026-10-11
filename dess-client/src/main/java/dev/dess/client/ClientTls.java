package dev.dess.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.security.auth.x500.X500Principal;

final class ClientTls {

    static final String[] PROTOCOLS = {"TLSv1.3", "TLSv1.2"};

    private ClientTls() {
    }

    static SSLContext context(Path identity, Path trustStore, Path passwordFile) throws IOException {
        Path identityPath = identity.toAbsolutePath();
        Path trustPath = trustStore.toAbsolutePath();
        Path passwordPath = passwordFile.toAbsolutePath();
        if (!Files.isRegularFile(identityPath)) {
            throw new IOException(identityPath + ": missing file");
        }
        if (!Files.isRegularFile(trustPath)) {
            throw new IOException(trustPath + ": missing file");
        }
        if (!Files.isRegularFile(passwordPath)) {
            throw new IOException(passwordPath + ": missing file");
        }
        String password = firstLine(passwordPath);
        if (password.isEmpty()) {
            throw new IOException(passwordPath + ": empty password");
        }
        try {
            KeyStore keys = store(identityPath, password);
            KeyStore trust = store(trustPath, password);
            KeyManagerFactory keysFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keysFactory.init(keys, password.toCharArray());
            TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustFactory.init(trust);
            X509ExtendedTrustManager pkix = pkix(trustFactory.getTrustManagers());
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keysFactory.getKeyManagers(), new TrustManager[] {new BrokerTrust(pkix)}, null);
            return context;
        } catch (GeneralSecurityException ex) {
            throw new IOException(identityPath + ": cannot be opened with that password", ex);
        }
    }

    static boolean isHandshakeFailure(String message) {
        return "EXPIRED".equals(message)
                || "UNTRUSTED".equals(message)
                || "NOT_BROKER".equals(message)
                || "PROTOCOL_VERSION".equals(message);
    }

    static String reason(Throwable failure) {
        RejectedBroker rejected = find(failure);
        if (rejected != null) {
            return rejected.reason();
        }
        return failure.getMessage() == null ? failure.toString() : failure.getMessage();
    }

    private static KeyStore store(Path path, String password) throws IOException, GeneralSecurityException {
        try (InputStream input = Files.newInputStream(path)) {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(input, password.toCharArray());
            return store;
        }
    }

    private static X509ExtendedTrustManager pkix(TrustManager[] managers) {
        for (TrustManager manager : managers) {
            if (manager instanceof X509ExtendedTrustManager extended) {
                return extended;
            }
        }
        throw new IllegalStateException("PKIX trust manager is required");
    }

    private static String firstLine(Path file) throws IOException {
        String text = Files.readString(file);
        int end = text.indexOf('\n');
        if (end >= 0) {
            text = text.substring(0, end);
        }
        if (text.endsWith("\r")) {
            text = text.substring(0, text.length() - 1);
        }
        return text;
    }

    private static RejectedBroker find(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof RejectedBroker rejected) {
                return rejected;
            }
            current = current.getCause();
        }
        return null;
    }

    private static final class BrokerTrust extends X509ExtendedTrustManager {

        private final X509ExtendedTrustManager delegate;

        private BrokerTrust(X509ExtendedTrustManager delegate) {
            this.delegate = delegate;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            delegate.checkClientTrusted(chain, authType);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
            delegate.checkClientTrusted(chain, authType, socket);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
            delegate.checkClientTrusted(chain, authType, engine);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            inspect(chain);
            try {
                delegate.checkServerTrusted(chain, authType);
            } catch (CertificateException ex) {
                throw new RejectedBroker("UNTRUSTED", ex);
            }
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
            inspect(chain);
            try {
                delegate.checkServerTrusted(chain, authType, socket);
            } catch (CertificateException ex) {
                throw new RejectedBroker("UNTRUSTED", ex);
            }
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
            inspect(chain);
            try {
                delegate.checkServerTrusted(chain, authType, engine);
            } catch (CertificateException ex) {
                throw new RejectedBroker("UNTRUSTED", ex);
            }
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return delegate.getAcceptedIssuers();
        }

        private static void inspect(X509Certificate[] chain) throws CertificateException {
            if (chain == null || chain.length == 0) {
                throw new RejectedBroker("UNTRUSTED", null);
            }
            for (X509Certificate certificate : chain) {
                try {
                    certificate.checkValidity();
                } catch (CertificateExpiredException ex) {
                    throw new RejectedBroker("EXPIRED", ex);
                }
            }
            if (!hasOu(chain[0], "broker")) {
                throw new RejectedBroker("NOT_BROKER", null);
            }
        }

        private static boolean hasOu(X509Certificate certificate, String value) {
            try {
                LdapName name = new LdapName(certificate.getSubjectX500Principal().getName(X500Principal.RFC2253));
                for (Rdn rdn : name.getRdns()) {
                    if ("OU".equalsIgnoreCase(rdn.getType()) && value.equalsIgnoreCase(String.valueOf(rdn.getValue()))) {
                        return true;
                    }
                }
                return false;
            } catch (InvalidNameException ex) {
                return false;
            }
        }
    }

    private static final class RejectedBroker extends CertificateException {

        private final String reason;

        private RejectedBroker(String reason, Throwable cause) {
            super(reason, cause);
            this.reason = reason;
        }

        private String reason() {
            return reason;
        }
    }
}
