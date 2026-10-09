package dev.dess.broker;

public record BrokerConfig(int port, int maxConnections, int maxPayloadBytes, int idleTimeoutMillis) {

    public static final int DEFAULT_PORT = 55123;

    public static final int DEFAULT_MAX_CONNECTIONS = 128;

    public static final int DEFAULT_MAX_PAYLOAD_BYTES = 1048576;

    public static final int DEFAULT_IDLE_TIMEOUT_MILLIS = 30000;

    public BrokerConfig {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
        if (maxConnections < 1) {
            throw new IllegalArgumentException("maxConnections must be at least 1");
        }
        if (maxPayloadBytes < 1) {
            throw new IllegalArgumentException("maxPayloadBytes must be at least 1");
        }
        if (idleTimeoutMillis < 1) {
            throw new IllegalArgumentException("idleTimeoutMillis must be at least 1");
        }
    }

    public static BrokerConfig defaults() {
        return new BrokerConfig(DEFAULT_PORT, DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_PAYLOAD_BYTES, DEFAULT_IDLE_TIMEOUT_MILLIS);
    }
}
