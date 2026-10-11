package dev.dess.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.logging.Logging;
import dev.dess.common.protocol.DiscoveryCodec;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

class DiscoveryCommandTest {

    @Test
    void noBrokerPrintsOneLineAndExitsOne() throws Exception {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        try (DatagramSocket unused = new DatagramSocket(0)) {
            int code = ClientMain.execute(
                    withIdentity("produce", "--topic", "sales", "--message", "hello",
                            "--discovery-port", Integer.toString(unused.getLocalPort())),
                    InputStream.nullInputStream(),
                    new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8),
                    new PrintStream(err, true, StandardCharsets.UTF_8));
            assertEquals(1, code);
            String text = err.toString(StandardCharsets.UTF_8);
            assertTrue(text.contains("no broker found"), text);
            assertEquals(1, text.strip().lines().count(), text);
        }
    }

    @Test
    void portWithoutHostExitsTwo() {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = ClientMain.execute(
                new String[] {"produce", "--topic", "sales", "--message", "hello", "--port", "55123"},
                InputStream.nullInputStream(),
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8));
        assertEquals(2, code);
        assertTrue(err.toString(StandardCharsets.UTF_8).contains("Usage:"), err.toString(StandardCharsets.UTF_8));
    }

    @Test
    void explicitHostSendsNoDatagram() throws Exception {
        try (ScriptedBroker broker = ScriptedBroker.start();
                DatagramSocket sniffer = new DatagramSocket(0)) {
            sniffer.setSoTimeout(200);
            int code = ClientMain.execute(
                    withIdentity("produce", "--topic", "sales", "--message", "hello",
                            "--host", "127.0.0.1",
                            "--port", Integer.toString(broker.port()),
                            "--discovery-port", Integer.toString(sniffer.getLocalPort())),
                    InputStream.nullInputStream(),
                    new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8),
                    new PrintStream(System.err, true, StandardCharsets.UTF_8));
            assertEquals(0, code);
            DatagramPacket incoming = new DatagramPacket(new byte[64], 64);
            assertThrows(SocketTimeoutException.class, () -> sniffer.receive(incoming));
        }
    }

    @Test
    void secondBrokerIdIsIgnoredAndFirstReplyIsUsed() throws Exception {
        Logging.configure("INFO");
        Logger logger = Logger.getLogger("dev.dess.client");
        List<String> lines = new CopyOnWriteArrayList<>();
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage() != null) {
                    lines.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        handler.setLevel(Level.ALL);
        logger.addHandler(handler);
        logger.setLevel(Level.INFO);
        try (DatagramSocket responder = new DatagramSocket(0)) {
            responder.setSoTimeout(2_000);
            Thread replies = new Thread(() -> replyTwice(responder), "dess-two-replies");
            replies.start();
            Discovery.Endpoint found = Discovery.locate(responder.getLocalPort());
            replies.join(2_000);
            assertNotNull(found);
            assertEquals("127.0.0.1", found.host());
            assertEquals(55123, found.port());
            assertTrue(lines.stream().anyMatch(line -> line.contains("reason=ANOTHER_BROKER")), lines::toString);
        } finally {
            logger.removeHandler(handler);
        }
    }

    private static void replyTwice(DatagramSocket responder) {
        try {
            DatagramPacket incoming = new DatagramPacket(new byte[64], 64);
            responder.receive(incoming);
            DiscoveryCodec.Request request = (DiscoveryCodec.Request) DiscoveryCodec.decode(
                    Arrays.copyOf(incoming.getData(), incoming.getLength()));
            byte[] address = {127, 0, 0, 1};
            byte[] first = DiscoveryCodec.reply(request.nonce(), 1L, 55123, address);
            byte[] second = DiscoveryCodec.reply(request.nonce(), 2L, 55123, address);
            responder.send(new DatagramPacket(first, first.length, incoming.getAddress(), incoming.getPort()));
            responder.send(new DatagramPacket(second, second.length, incoming.getAddress(), incoming.getPort()));
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static String[] withIdentity(String... args) {
        List<String> command = new ArrayList<>(List.of(args));
        command.add("--identity");
        command.add(TestPki.producerStore().toString());
        command.add("--trust-store");
        command.add(TestPki.trustStore().toString());
        command.add("--password-file");
        command.add(TestPki.passwordFile().toString());
        return command.toArray(String[]::new);
    }
}
