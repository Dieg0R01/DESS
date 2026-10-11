package dev.dess.broker;

import java.security.cert.CertificateException;

final class RejectedCertificate extends CertificateException {

    private final HandshakeReason reason;

    RejectedCertificate(HandshakeReason reason, Throwable cause) {
        super(reason.name(), cause);
        this.reason = reason;
    }

    HandshakeReason reason() {
        return reason;
    }
}
