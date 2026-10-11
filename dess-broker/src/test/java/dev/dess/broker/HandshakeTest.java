package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class HandshakeTest {

    @Test
    void plaintextHelloIsClosedWithoutConnecting() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), harness.port()), 2_000);
            String remote = socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort();
            socket.getOutputStream().write(FrameCodec.encode(new Frame(
                    Command.HELLO, FrameCodec.helloHeader(FrameCodec.PROTOCOL_VERSION), new byte[0])));
            socket.getOutputStream().flush();
            assertTrue(logs.await(Duration.ofSeconds(2), "event=tls.handshake.failed", "reason=PLAINTEXT",
                    "remoteAddress=" + remote), logs.lines()::toString);
            assertEquals(0, logs.count("event=client.connected"));
        }
    }

    @Test
    void tls11ClientHelloIsProtocolVersion() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), harness.port()), 2_000);
            String remote = socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort();
            socket.getOutputStream().write(tls11Prefix());
            socket.getOutputStream().flush();
            assertTrue(logs.await(Duration.ofSeconds(2), "event=tls.handshake.failed", "reason=PROTOCOL_VERSION",
                    "remoteAddress=" + remote), logs.lines()::toString);
            assertEquals(0, logs.count("event=client.connected"));
        }
    }

    @Test
    void clientThatTrustsAnotherCaFails() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start()) {
            IOException failure = connectAndFail(harness.port(), TestPki.untrustedClient(),
                    TestPki.untrustedTrustStore(), TestPki.untrustedPasswordFile());
            assertTrue(failure != null, "untrusted client connected");
            assertTrue(logs.await(Duration.ofSeconds(2), "event=tls.handshake.failed", "reason=FAILED"),
                    logs.lines()::toString);
            assertEquals(0, logs.count("event=client.connected"));
        }
    }

    @Test
    void expiredClientCertificateIsExpired() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start()) {
            connectAndFail(harness.port(), TestPki.expiredClient(),
                    TestPki.trustStore(), TestPki.passwordFile());
            assertTrue(logs.await(Duration.ofSeconds(2), "event=tls.handshake.failed", "reason=EXPIRED"),
                    logs.lines()::toString);
            assertEquals(0, logs.count("event=client.connected"));
        }
    }

    @Test
    void handshakeWithoutAClientCertificateIsRejected() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                SSLSocket socket = TestPki.anonymousSocket(harness.port())) {
            String remote = socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort();
            try {
                socket.startHandshake();
            } catch (Exception ex) {
                // TLS 1.3 may finish on the client before the broker closes.
            }
            assertTrue(logs.await(Duration.ofSeconds(2), "event=tls.handshake.failed", "reason=NO_CERTIFICATE",
                    "remoteAddress=" + remote), logs.lines()::toString);
            assertEquals(0, logs.count("event=client.connected"));
        }
    }

    @Test
    void producerHandshakeConnectsAndPublishes() throws Exception {
        try (LogCapture logs = LogCapture.open();
                BrokerHarness harness = BrokerHarness.start();
                ProtocolClient client = ProtocolClient.connect(harness.port())) {
            client.setReadTimeoutMillis(2_000);
            client.hello();
            assertTrue(logs.await(Duration.ofSeconds(2), "event=client.connected"), logs.lines()::toString);
            client.publish("sales", "ok".getBytes(StandardCharsets.UTF_8));
            Frame ack = client.readFrame();
            assertEquals(Command.ACK, ack.command());
        }
    }

    @Test
    void startedLogCarriesTlsAndVersionFour() throws Exception {
        try (LogCapture logs = LogCapture.open();
                Broker broker = Broker.start(TestConfigs.of(0, 8, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 30_000))) {
            assertEquals(1, logs.count("event=broker.started", "tls=true", "version=4", "tcpPort=" + broker.port()));
        }
    }

    private static IOException connectAndFail(int port, Path identity, Path trust, Path password) {
        try {
            SSLSocket socket = TestPki.clientSocket(port, identity, trust, password);
            SSLParameters parameters = socket.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm(null);
            socket.setSSLParameters(parameters);
            socket.startHandshake();
            socket.close();
            return null;
        } catch (IOException ex) {
            return ex;
        }
    }

    private static byte[] tls11Prefix() {
        return new byte[] {
            0x16, 0x03, 0x01, 0x00, 0x20,
            0x01, 0x00, 0x00, 0x1c,
            0x03, 0x02
        };
    }
}
