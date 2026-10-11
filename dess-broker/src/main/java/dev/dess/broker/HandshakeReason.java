package dev.dess.broker;

enum HandshakeReason {
    PLAINTEXT,
    PROTOCOL_VERSION,
    NO_CERTIFICATE,
    EXPIRED,
    UNTRUSTED,
    TIMEOUT,
    CLOSED,
    FAILED
}
