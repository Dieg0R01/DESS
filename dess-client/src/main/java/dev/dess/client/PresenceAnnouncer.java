package dev.dess.client;

import dev.dess.common.protocol.DiscoveryCodec;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;

final class PresenceAnnouncer implements AutoCloseable {

    private final DatagramSocket socket;
    private final Timer timer;

    private PresenceAnnouncer(DatagramSocket socket, Timer timer) {
        this.socket = socket;
        this.timer = timer;
    }

    static PresenceAnnouncer startIfRequested(ClientRequest request, Discovery.Endpoint endpoint) throws IOException {
        if (request.presenceMs() < 1) {
            return null;
        }
        String name = ClientTls.commonName(request.identity(), request.passwordFile());
        return start(endpoint.host(), request.discoveryPort(), request.presenceMs(), name);
    }

    static PresenceAnnouncer start(String host, int discoveryPort, int intervalMillis, String name) throws IOException {
        DatagramSocket socket = new DatagramSocket();
        long instanceId = ThreadLocalRandom.current().nextLong();
        byte[] encoded = DiscoveryCodec.presence(instanceId, intervalMillis, name);
        InetSocketAddress target = new InetSocketAddress(host, discoveryPort);
        Timer timer = new Timer("dess-presence-client", true);
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                try {
                    socket.send(new DatagramPacket(encoded, encoded.length, target));
                } catch (IOException ex) {
                    // Presence is advisory; a dropped datagram is retried on the next interval.
                }
            }
        };
        timer.scheduleAtFixedRate(task, 0L, intervalMillis);
        return new PresenceAnnouncer(socket, timer);
    }

    @Override
    public void close() {
        timer.cancel();
        socket.close();
    }
}
