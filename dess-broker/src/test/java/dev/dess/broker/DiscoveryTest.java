package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.DiscoveryCodec;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class DiscoveryTest {

    @Test
    void requestIsAnsweredAndLoopbackMeetsSc004() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(TestConfigs.discoveryEnabled(0))) {
            DiscoveryCodec.Reply reply = probe(harness.discoveryPort());
            assertEquals(harness.port(), reply.tcpPort());
            assertTrue(reply.tls());
            try (ProtocolClient client = ProtocolClient.connect(reply.tcpPort())) {
                client.hello();
            }
            assertTrue(logs.await(Duration.ofSeconds(2), "event=discovery.request", "responded=true"),
                    logs.lines()::toString);
            logs.quiet();
            int successes = 0;
            for (int attempt = 0; attempt < 20; attempt++) {
                long started = System.nanoTime();
                DiscoveryCodec.Reply next = probe(harness.discoveryPort());
                try (ProtocolClient client = ProtocolClient.connect(next.tcpPort())) {
                    client.hello();
                }
                if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) <= 2_000) {
                    successes++;
                }
            }
            assertTrue(successes >= 19, "loopback discoveries within 2s: " + successes + "/20");
        }
    }

    @Test
    void disabledDiscoveryLogsFalseAndSendsNothing() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start(TestConfigs.discoveryDisabled(0));
                DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(200);
            sendRequest(socket, harness.discoveryPort(), ThreadLocalRandom.current().nextLong());
            assertTrue(logs.await(Duration.ofSeconds(2), "event=discovery.request", "responded=false"),
                    logs.lines()::toString);
            DatagramPacket incoming = new DatagramPacket(new byte[128], 128);
            assertThrows(SocketTimeoutException.class, () -> socket.receive(incoming));
        }
    }

    @Test
    void malformedDatagramIsIgnoredAndALaterRequestIsAnswered() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start(TestConfigs.discoveryEnabled(0));
                DatagramSocket socket = new DatagramSocket()) {
            byte[] junk = "hello".getBytes();
            socket.send(new DatagramPacket(junk, junk.length, InetAddress.getByName("127.0.0.1"), harness.discoveryPort()));
            DiscoveryCodec.Reply reply = probe(socket, harness.discoveryPort());
            assertEquals(harness.port(), reply.tcpPort());
        }
    }

    @Test
    void loopbackRequestReceivesLoopbackAddress() throws Exception {
        try (BrokerHarness harness = BrokerHarness.start(TestConfigs.discoveryEnabled(0))) {
            DiscoveryCodec.Reply reply = probe(harness.discoveryPort());
            assertArrayEquals(new byte[] {127, 0, 0, 1}, reply.address());
        }
    }

    @Test
    void occupiedDiscoveryPortFailsStartupNamingThePort() throws Exception {
        try (DatagramSocket taken = new DatagramSocket(null)) {
            taken.setReuseAddress(false);
            taken.bind(new InetSocketAddress(0));
            int port = taken.getLocalPort();
            IOException failure = assertThrows(IOException.class, () -> Broker.start(TestConfigs.discoveryEnabled(port)));
            assertTrue(failure.getMessage().contains(Integer.toString(port)), failure.getMessage());
        }
    }

    private static DiscoveryCodec.Reply probe(int discoveryPort) throws IOException {
        try (DatagramSocket socket = new DatagramSocket()) {
            return probe(socket, discoveryPort);
        }
    }

    private static DiscoveryCodec.Reply probe(DatagramSocket socket, int discoveryPort) throws IOException {
        socket.setSoTimeout(2_000);
        long nonce = ThreadLocalRandom.current().nextLong();
        sendRequest(socket, discoveryPort, nonce);
        DatagramPacket incoming = new DatagramPacket(new byte[128], 128);
        socket.receive(incoming);
        Object decoded = DiscoveryCodec.decode(Arrays.copyOf(incoming.getData(), incoming.getLength()));
        DiscoveryCodec.Reply reply = (DiscoveryCodec.Reply) decoded;
        assertEquals(nonce, reply.nonce());
        return reply;
    }

    private static void sendRequest(DatagramSocket socket, int discoveryPort, long nonce) throws IOException {
        byte[] request = DiscoveryCodec.request(nonce);
        socket.send(new DatagramPacket(request, request.length, InetAddress.getByName("127.0.0.1"), discoveryPort));
    }
}
