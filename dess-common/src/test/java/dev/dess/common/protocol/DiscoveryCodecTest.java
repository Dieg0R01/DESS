package dev.dess.common.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;

class DiscoveryCodecTest {

    @Test
    void requestIsFifteenBytes() {
        byte[] encoded = DiscoveryCodec.request(0x0102030405060708L);
        assertEquals(15, encoded.length);
        assertEquals('D', encoded[0]);
        assertEquals('E', encoded[1]);
        assertEquals('S', encoded[2]);
        assertEquals('S', encoded[3]);
        assertEquals(DiscoveryCodec.TYPE_REQUEST, encoded[4] & 0xff);
        ByteBuffer view = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        view.position(5);
        assertEquals(4, view.getShort());
        assertEquals(0x0102030405060708L, view.getLong());
        DiscoveryCodec.Request decoded = (DiscoveryCodec.Request) DiscoveryCodec.decode(encoded);
        assertEquals(4, decoded.version());
        assertEquals(0x0102030405060708L, decoded.nonce());
    }

    @Test
    void replyRoundTripsIpv4() {
        byte[] address = {127, 0, 0, 1};
        byte[] encoded = DiscoveryCodec.reply(9L, 99L, 55123, address);
        DiscoveryCodec.Reply decoded = (DiscoveryCodec.Reply) DiscoveryCodec.decode(encoded);
        assertEquals(9L, decoded.nonce());
        assertEquals(99L, decoded.brokerId());
        assertTrue(decoded.tls());
        assertEquals(55123, decoded.tcpPort());
        assertArrayEquals(address, decoded.address());
    }

    @Test
    void replyRoundTripsIpv6() {
        byte[] address = new byte[16];
        address[15] = 1;
        byte[] encoded = DiscoveryCodec.reply(1L, 2L, 443, address);
        DiscoveryCodec.Reply decoded = (DiscoveryCodec.Reply) DiscoveryCodec.decode(encoded);
        assertEquals(1L, decoded.nonce());
        assertEquals(2L, decoded.brokerId());
        assertEquals(443, decoded.tcpPort());
        assertArrayEquals(address, decoded.address());
    }

    @Test
    void presenceRoundTripsNameAndInterval() {
        byte[] encoded = DiscoveryCodec.presence(7L, 200, "alice");
        DiscoveryCodec.Presence decoded = (DiscoveryCodec.Presence) DiscoveryCodec.decode(encoded);
        assertEquals(7L, decoded.instanceId());
        assertEquals(200, decoded.intervalMillis());
        assertEquals("alice", decoded.name());
        assertEquals(4, decoded.version());
    }

    @Test
    void truncatedBufferFails() {
        assertThrows(IllegalArgumentException.class, () -> DiscoveryCodec.decode(new byte[] {'D', 'E', 'S'}));
    }

    @Test
    void badMagicFails() {
        byte[] encoded = DiscoveryCodec.request(1L);
        encoded[0] = 'X';
        assertThrows(IllegalArgumentException.class, () -> DiscoveryCodec.decode(encoded));
    }

    @Test
    void unknownTypeFails() {
        byte[] encoded = DiscoveryCodec.request(1L);
        encoded[4] = 9;
        assertThrows(IllegalArgumentException.class, () -> DiscoveryCodec.decode(encoded));
    }

    @Test
    void replyWithoutTlsFlagFails() {
        byte[] address = {127, 0, 0, 1};
        byte[] encoded = DiscoveryCodec.reply(1L, 2L, 55123, address);
        encoded[4 + 1 + 2 + 8 + 8] = 0;
        assertThrows(IllegalArgumentException.class, () -> DiscoveryCodec.decode(encoded));
    }

    @Test
    void presenceNameIsUtf8() {
        byte[] encoded = DiscoveryCodec.presence(1L, 50, "café");
        DiscoveryCodec.Presence decoded = (DiscoveryCodec.Presence) DiscoveryCodec.decode(encoded);
        assertEquals("café", decoded.name());
    }
}
