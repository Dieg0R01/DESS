package dev.dess.broker;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.security.auth.x500.X500Principal;

final class CertificateCheck extends X509ExtendedTrustManager {

    private final X509ExtendedTrustManager delegate;

    CertificateCheck(X509ExtendedTrustManager delegate) {
        this.delegate = delegate;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        inspect(chain);
        try {
            delegate.checkClientTrusted(chain, authType);
        } catch (CertificateException ex) {
            throw new RejectedCertificate(HandshakeReason.UNTRUSTED, ex);
        }
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        inspect(chain);
        try {
            delegate.checkClientTrusted(chain, authType, socket);
        } catch (CertificateException ex) {
            throw new RejectedCertificate(HandshakeReason.UNTRUSTED, ex);
        }
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        inspect(chain);
        try {
            delegate.checkClientTrusted(chain, authType, engine);
        } catch (CertificateException ex) {
            throw new RejectedCertificate(HandshakeReason.UNTRUSTED, ex);
        }
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        delegate.checkServerTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        delegate.checkServerTrusted(chain, authType, socket);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        delegate.checkServerTrusted(chain, authType, engine);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return delegate.getAcceptedIssuers();
    }

    private static void inspect(X509Certificate[] chain) throws CertificateException {
        if (chain == null || chain.length == 0) {
            throw new RejectedCertificate(HandshakeReason.UNTRUSTED, null);
        }
        for (X509Certificate certificate : chain) {
            try {
                certificate.checkValidity();
            } catch (CertificateExpiredException ex) {
                throw new RejectedCertificate(HandshakeReason.EXPIRED, ex);
            }
        }
        if (!hasCn(chain[0])) {
            throw new RejectedCertificate(HandshakeReason.UNTRUSTED, null);
        }
    }

    private static boolean hasCn(X509Certificate certificate) {
        try {
            LdapName name = new LdapName(certificate.getSubjectX500Principal().getName(X500Principal.RFC2253));
            for (Rdn rdn : name.getRdns()) {
                if ("CN".equalsIgnoreCase(rdn.getType()) && !String.valueOf(rdn.getValue()).isBlank()) {
                    return true;
                }
            }
            return false;
        } catch (InvalidNameException ex) {
            return false;
        }
    }
}
