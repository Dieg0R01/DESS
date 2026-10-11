package dev.dess.broker;

import dev.dess.common.logging.Logging;
import dev.dess.common.protocol.DiscoveryCodec;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

final class DiscoveryResponder implements AutoCloseable {

    private final DatagramSocket socket;
    private final Thread thread;
    private final long brokerId;
    private final int tcpPort;
    private final boolean discoveryEnabled;
    private final PresenceTracker presence;
    private volatile boolean running = true;

    private DiscoveryResponder(DatagramSocket socket, int tcpPort, boolean discoveryEnabled, PresenceTracker presence) {
        this.socket = socket;
        this.tcpPort = tcpPort;
        this.discoveryEnabled = discoveryEnabled;
        this.presence = presence;
        this.brokerId = ThreadLocalRandom.current().nextLong();
        this.thread = new Thread(this::loop, "dess-discovery");
        this.thread.setDaemon(false);
        this.thread.start();
    }

    static DiscoveryResponder open(
            int port, int tcpPort, boolean discoveryEnabled, int idleTimeoutMillis, PresenceTracker presence)
            throws IOException {
        DatagramSocket socket = new DatagramSocket(null);
        socket.setReuseAddress(true);
        socket.setBroadcast(true);
        try {
            socket.bind(new InetSocketAddress(port));
        } catch (IOException ex) {
            socket.close();
            throw new IOException("Could not bind discovery port " + port, ex);
        }
        try {
            socket.setSoTimeout(idleTimeoutMillis);
        } catch (SocketException ex) {
            socket.close();
            throw new IOException("Could not bind discovery port " + port, ex);
        }
        return new DiscoveryResponder(socket, tcpPort, discoveryEnabled, presence);
    }

    int port() {
        return socket.getLocalPort();
    }

    long brokerId() {
        return brokerId;
    }

    void close(long timeoutMillis) {
        running = false;
        socket.close();
        try {
            thread.join(timeoutMillis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping discovery", ex);
        }
        if (thread.isAlive()) {
            throw new IllegalStateException("Discovery thread did not stop");
        }
    }

    @Override
    public void close() {
        close(2_000);
    }

    private void loop() {
        byte[] buffer = new byte[2048];
        while (running) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
            } catch (SocketTimeoutException ex) {
                continue;
            } catch (IOException ex) {
                if (!running) {
                    return;
                }
                continue;
            }
            byte[] encoded = Arrays.copyOf(packet.getData(), packet.getLength());
            Object decoded = DiscoveryCodec.tryDecode(encoded);
            if (decoded instanceof DiscoveryCodec.Presence announcement) {
                if (presence != null) {
                    presence.announce(
                            announcement.instanceId(),
                            announcement.name(),
                            packet.getAddress().getHostAddress() + ":" + packet.getPort(),
                            announcement.intervalMillis());
                }
                continue;
            }
            if (!(decoded instanceof DiscoveryCodec.Request request)) {
                continue;
            }
            boolean respond = discoveryEnabled;
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put("remoteAddress", packet.getAddress().getHostAddress() + ":" + packet.getPort());
            properties.put("responded", Boolean.toString(respond));
            Logging.event("dev.dess.broker", "discovery.request", properties);
            if (!respond) {
                continue;
            }
            try {
                byte[] address = reach(packet.getAddress(), packet.getPort());
                byte[] reply = DiscoveryCodec.reply(request.nonce(), brokerId, tcpPort, address);
                socket.send(new DatagramPacket(reply, reply.length, packet.getAddress(), packet.getPort()));
            } catch (IOException ex) {
                if (!running) {
                    return;
                }
            }
        }
    }

    private static byte[] reach(InetAddress remote, int port) throws IOException {
        try (DatagramSocket probe = new DatagramSocket()) {
            probe.connect(remote, port);
            InetAddress local = probe.getLocalAddress();
            if (local == null || local.isAnyLocalAddress()) {
                return InetAddress.getByAddress(new byte[] {127, 0, 0, 1}).getAddress();
            }
            return local.getAddress();
        }
    }
}
