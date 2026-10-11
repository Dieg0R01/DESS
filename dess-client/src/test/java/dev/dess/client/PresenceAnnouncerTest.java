package dev.dess.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.DiscoveryCodec;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class PresenceAnnouncerTest {

    @Test
    void announcerSendsTypeThreeWithCertificateCommonName() throws Exception {
        try (DatagramSocket sniffer = new DatagramSocket(0);
                PresenceAnnouncer announcer = PresenceAnnouncer.start(
                        "127.0.0.1", sniffer.getLocalPort(), 200, ClientTls.commonName(
                                TestPki.producerStore(), TestPki.passwordFile()))) {
            sniffer.setSoTimeout(2_000);
            DatagramPacket incoming = new DatagramPacket(new byte[256], 256);
            sniffer.receive(incoming);
            DiscoveryCodec.Presence presence = (DiscoveryCodec.Presence) DiscoveryCodec.decode(
                    Arrays.copyOf(incoming.getData(), incoming.getLength()));
            assertEquals("producer", presence.name());
            assertEquals(200, presence.intervalMillis());
            assertEquals(4, presence.version());
        }
    }

    @Test
    void presenceMsZeroExitsTwo() {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = ClientMain.execute(
                new String[] {"produce", "--topic", "sales", "--message", "x", "--presence-ms", "0"},
                InputStream.nullInputStream(),
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8));
        assertEquals(2, code);
        assertTrue(err.toString(StandardCharsets.UTF_8).contains("Usage:"), err.toString(StandardCharsets.UTF_8));
    }
}
