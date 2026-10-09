package dev.dess.common.protocol;

public enum ErrorCode {
    MALFORMED_FRAME,
    UNKNOWN_COMMAND,
    UNSUPPORTED_VERSION,
    HANDSHAKE_REQUIRED,
    PAYLOAD_TOO_LARGE,
    INVALID_TOPIC_NAME,
    BROKER_BUSY
}
