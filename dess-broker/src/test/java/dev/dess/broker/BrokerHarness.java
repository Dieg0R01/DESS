package dev.dess.broker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;
import javax.net.ssl.SSLSocket;

public final class BrokerHarness implements AutoCloseable {

    private final Broker broker;

    private BrokerHarness(Broker broker) {
        this.broker = broker;
    }

    public static BrokerHarness start() {
        return start(TestConfigs.of(0, 128, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 120_000));
    }

    public static BrokerHarness start(BrokerConfig config) {
        Broker broker = open(config);
        try (SSLSocket probe = TestPki.clientSocket(broker.port())) {
            probe.startHandshake();
            return new BrokerHarness(broker);
        } catch (IOException ex) {
            broker.close();
            throw new IllegalStateException("Broker did not accept a connection within 2 seconds", ex);
        }
    }

    public boolean hasTopic(String name) {
        return broker.hasTopic(name);
    }

    public Set<Long> subscribers(String name) {
        return broker.subscribers(name);
    }

    public int port() {
        return broker.port();
    }

    public int discoveryPort() {
        return broker.discoveryPort();
    }

    public int presenceCount() {
        return broker.presenceCount();
    }

    public boolean hasPresence(long instanceId) {
        return broker.hasPresence(instanceId);
    }

    public boolean presenceRunning() {
        return broker.presenceRunning();
    }

    public int inFlight() {
        return broker.inFlight();
    }

    public int inFlightPeak() {
        return broker.inFlightPeak();
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
