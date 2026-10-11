package dev.dess.common.security;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class DigestsTest {

    @Test
    void hashesTopicLengthThenTopicThenPayload() {
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] digest = Digests.of("sales", payload);
        assertEquals(32, digest.length);
        assertTrue(MessageDigest.isEqual(digest, Digests.of("sales", payload)));
        assertTrue(Digests.matches("sales", payload, digest));
        assertFalse(Digests.matches("sales", payload, new byte[32]));

        byte[] abC = Digests.of("ab", new byte[] {'c'});
        byte[] aBc = Digests.of("a", new byte[] {'b', 'c'});
        assertNotEquals(bytes(abC), bytes(aBc));

        byte[] sales = Digests.of("sales", payload);
        byte[] stock = Digests.of("stock", payload);
        assertNotEquals(bytes(sales), bytes(stock));
        assertArrayEquals(digest, sales);
    }

    private static String bytes(byte[] value) {
        return HexFormat.of().formatHex(value);
    }
}
