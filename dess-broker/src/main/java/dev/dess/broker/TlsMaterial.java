package dev.dess.broker;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.security.auth.x500.X500Principal;

final class TlsMaterial {

    static final String[] PROTOCOLS = {"TLSv1.3", "TLSv1.2"};

    private final SSLContext context;

    private TlsMaterial(SSLContext context) {
        this.context = context;
    }

    SSLSocketFactory factory() {
        return context.getSocketFactory();
    }

    static Result load(BrokerConfig config) {
        List<String> problems = new ArrayList<>();
        Path keyStorePath = config.keyStore().toAbsolutePath();
        Path trustStorePath = config.trustStore().toAbsolutePath();
        Path passwordPath = config.passwordFile().toAbsolutePath();
        if (!Files.isRegularFile(keyStorePath)) {
            problems.add(keyStorePath + ": missing file");
        }
        if (!Files.isRegularFile(trustStorePath)) {
            problems.add(trustStorePath + ": missing file");
        }
        if (!Files.isRegularFile(passwordPath)) {
            problems.add(passwordPath + ": missing file");
        }
        String password = null;
        if (Files.isRegularFile(passwordPath)) {
            try {
                password = firstLine(passwordPath);
            } catch (IOException ex) {
                problems.add(passwordPath + ": missing file");
            }
            if (password != null && password.isEmpty()) {
                problems.add(passwordPath + ": empty password");
                password = null;
            }
        }
        KeyStore keys = null;
        if (password != null && Files.isRegularFile(keyStorePath)) {
            keys = openStore(keyStorePath, password, problems);
            if (keys != null) {
                checkKeyStore(keys, keyStorePath, problems);
            }
        }
        KeyStore trust = null;
        if (password != null && Files.isRegularFile(trustStorePath)) {
            trust = openStore(trustStorePath, password, problems);
            if (trust != null) {
                checkTrustStore(trust, trustStorePath, problems);
            }
        }
        if (!problems.isEmpty() || keys == null || trust == null || password == null) {
            return new Result(null, problems);
        }
        try {
            KeyManagerFactory keysFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keysFactory.init(keys, password.toCharArray());
            TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustFactory.init(trust);
            X509ExtendedTrustManager pkix = pkix(trustFactory.getTrustManagers());
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keysFactory.getKeyManagers(), new TrustManager[] {new CertificateCheck(pkix)}, null);
            return new Result(new TlsMaterial(context), List.of());
        } catch (GeneralSecurityException ex) {
            problems.add(keyStorePath + ": cannot be opened with that password");
            return new Result(null, problems);
        }
    }

    private static KeyStore openStore(Path path, String password, List<String> problems) {
        try (InputStream input = Files.newInputStream(path)) {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(input, password.toCharArray());
            return store;
        } catch (IOException | GeneralSecurityException ex) {
            problems.add(path + ": cannot be opened with that password");
            return null;
        }
    }

    private static void checkKeyStore(KeyStore store, Path path, List<String> problems) {
        try {
            List<String> aliases = Collections.list(store.aliases());
            String keyAlias = null;
            int keys = 0;
            for (String alias : aliases) {
                if (store.isKeyEntry(alias)) {
                    keys++;
                    keyAlias = alias;
                }
            }
            if (keys != 1) {
                problems.add(path + ": does not contain exactly one private key entry");
                return;
            }
            Certificate certificate = store.getCertificate(keyAlias);
            if (!(certificate instanceof X509Certificate x509)) {
                problems.add(path + ": broker certificate has no broker role");
                return;
            }
            if (!hasOu(x509, "broker")) {
                problems.add(path + ": broker certificate has no broker role");
            }
            try {
                x509.checkValidity();
            } catch (CertificateExpiredException ex) {
                problems.add(path + ": broker certificate has expired");
            }
        } catch (GeneralSecurityException ex) {
            problems.add(path + ": cannot be opened with that password");
        }
    }

    private static void checkTrustStore(KeyStore store, Path path, List<String> problems) {
        try {
            boolean found = false;
            for (String alias : Collections.list(store.aliases())) {
                if (store.getCertificate(alias) != null) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                problems.add(path + ": trust store has no certificate");
            }
        } catch (GeneralSecurityException ex) {
            problems.add(path + ": cannot be opened with that password");
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

    static boolean hasOu(X509Certificate certificate, String value) {
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

    static String firstLine(Path file) throws IOException {
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

    record Result(TlsMaterial material, List<String> problems) {
    }
}
