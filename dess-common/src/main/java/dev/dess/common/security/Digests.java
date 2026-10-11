package dev.dess.common.security;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

public final class Digests {

    public static final int LENGTH = 32;

    private Digests() {
    }

    public static byte[] of(String topic, byte[] payload) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(payload, "payload");
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        if (topicBytes.length > 65535) {
            throw new IllegalArgumentException("topic is too long");
        }
        ByteBuffer buffer = ByteBuffer.allocate(2 + topicBytes.length + payload.length);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.put(payload);
        try {
            return MessageDigest.getInstance("SHA-256").digest(buffer.array());
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is required", ex);
        }
    }

    public static boolean matches(String topic, byte[] payload, byte[] digest) {
        if (digest == null || digest.length != LENGTH) {
            return false;
        }
        return MessageDigest.isEqual(of(topic, payload), digest);
    }
}
