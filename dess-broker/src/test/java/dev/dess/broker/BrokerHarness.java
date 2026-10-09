package dev.dess.broker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;

public final class BrokerHarness implements AutoCloseable {

    private static final Duration START_TIMEOUT = Duration.ofSeconds(2);

    private final Broker broker;

    private BrokerHarness(Broker broker) {
        this.broker = broker;
    }

    public static BrokerHarness start() {
        return start(new BrokerConfig(0, 128, BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES, 120_000));
    }

    public static BrokerHarness start(BrokerConfig config) {
        Broker broker = open(config);
        long deadline = System.nanoTime() + START_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            try (Socket probe = new Socket()) {
                probe.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), broker.port()), 200);
                return new BrokerHarness(broker);
            } catch (IOException ex) {
                // The accept thread may not be blocked in accept() yet.
            }
        }
        broker.close();
        throw new IllegalStateException("Broker did not accept a connection within 2 seconds");
    }

    public int port() {
        return broker.port();
    }

    @Override
    public void close() {
        broker.close();
    }

    private static Broker open(BrokerConfig config) {
        try {
            return Broker.start(config);
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not bind the test broker", ex);
        }
    }
}
