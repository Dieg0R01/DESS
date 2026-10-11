package dev.dess.client;

import dev.dess.common.logging.Logging;
import dev.dess.common.protocol.DiscoveryCodec;
import dev.dess.common.protocol.FrameCodec;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

final class Discovery {

    private static final int ATTEMPTS = 3;

    private static final int ATTEMPT_WAIT_MS = 1000;

    private static final int EXTRA_WAIT_MS = 200;

    private Discovery() {
    }

    record Endpoint(String host, int port) {
    }

    static Endpoint resolve(ClientRequest request) throws IOException {
        if (request.host() != null) {
            return new Endpoint(request.host(), request.port());
        }
        return locate(request.discoveryPort());
    }

    static Endpoint locate(int discoveryPort) throws IOException {
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setBroadcast(true);
            for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
                Endpoint found = attempt(socket, discoveryPort);
                if (found != null) {
                    return found;
                }
            }
            return null;
        }
    }

    private static Endpoint attempt(DatagramSocket socket, int discoveryPort) throws IOException {
        long nonce = ThreadLocalRandom.current().nextLong();
        byte[] request = DiscoveryCodec.request(nonce);
        sendAll(socket, request, discoveryPort);
        Endpoint first = null;
        long brokerId = 0L;
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ATTEMPT_WAIT_MS);
        byte[] buffer = new byte[2048];
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return first;
            }
            int waitMs = (int) Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining));
            socket.setSoTimeout(waitMs);
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
            } catch (SocketTimeoutException ex) {
                return first;
            }
            byte[] encoded = Arrays.copyOf(packet.getData(), packet.getLength());
            Object decoded = DiscoveryCodec.tryDecode(encoded);
            if (decoded instanceof DiscoveryCodec.Reply reply) {
                if (reply.nonce() != nonce) {
                    ignore("STALE_NONCE", packet);
                    continue;
                }
                if (reply.version() != FrameCodec.PROTOCOL_VERSION) {
                    ignore("VERSION", packet);
                    continue;
                }
                if (first == null) {
                    first = endpoint(reply);
                    brokerId = reply.brokerId();
                    deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(EXTRA_WAIT_MS);
                    continue;
                }
                if (reply.brokerId() != brokerId) {
                    ignore("ANOTHER_BROKER", packet);
                }
                continue;
            }
            String reason = ignoreReason(encoded, nonce);
            if (reason != null) {
                ignore(reason, packet);
            }
        }
    }

    private static void sendAll(DatagramSocket socket, byte[] request, int discoveryPort) {
        sendQuiet(socket, request, new InetSocketAddress("255.255.255.255", discoveryPort));
        try {
            for (NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!iface.isUp() || iface.isLoopback()) {
                    continue;
                }
                for (InterfaceAddress address : iface.getInterfaceAddresses()) {
                    InetAddress broadcast = address.getBroadcast();
                    if (broadcast != null) {
                        sendQuiet(socket, request, new InetSocketAddress(broadcast, discoveryPort));
                    }
                }
            }
        } catch (IOException ex) {
            // Directed broadcasts are best-effort; 127.0.0.1 still goes out.
        }
        sendQuiet(socket, request, new InetSocketAddress("127.0.0.1", discoveryPort));
    }

    private static void sendQuiet(DatagramSocket socket, byte[] request, InetSocketAddress target) {
        try {
            socket.send(new DatagramPacket(request, request.length, target));
        } catch (IOException ex) {
            // One destination can fail while others still reach the broker.
        }
    }

    private static Endpoint endpoint(DiscoveryCodec.Reply reply) throws IOException {
        String host = InetAddress.getByAddress(reply.address()).getHostAddress();
        return new Endpoint(host, reply.tcpPort());
    }

    private static void ignore(String reason, DatagramPacket packet) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("reason", reason);
        properties.put("remoteAddress", packet.getAddress().getHostAddress() + ":" + packet.getPort());
        Logging.event("dev.dess.client", "discovery.ignored", properties);
    }

    private static String ignoreReason(byte[] encoded, long nonce) {
        if (encoded.length < 24) {
            return null;
        }
        if (encoded[0] != 'D' || encoded[1] != 'E' || encoded[2] != 'S' || encoded[3] != 'S') {
            return null;
        }
        if ((encoded[4] & 0xff) != DiscoveryCodec.TYPE_REPLY) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        buffer.position(5);
        int version = buffer.getShort() & 0xffff;
        long replyNonce = buffer.getLong();
        buffer.getLong();
        int flags = buffer.get() & 0xff;
        if (replyNonce != nonce) {
            return "STALE_NONCE";
        }
        if (version != FrameCodec.PROTOCOL_VERSION) {
            return "VERSION";
        }
        if ((flags & DiscoveryCodec.FLAG_TLS) == 0) {
            return "TLS";
        }
        return null;
    }
}
