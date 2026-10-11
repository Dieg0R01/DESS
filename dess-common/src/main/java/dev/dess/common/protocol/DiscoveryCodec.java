package dev.dess.common.protocol;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class DiscoveryCodec {

    public static final int TYPE_REQUEST = 1;

    public static final int TYPE_REPLY = 2;

    public static final int TYPE_PRESENCE = 3;

    public static final int FLAG_TLS = 1;

    public static final int FAMILY_IPV4 = 4;

    public static final int FAMILY_IPV6 = 6;

    private static final byte[] MAGIC = {'D', 'E', 'S', 'S'};

    private DiscoveryCodec() {
    }

    public static byte[] request(long nonce) {
        ByteBuffer buffer = bytes(15);
        header(buffer, TYPE_REQUEST);
        buffer.putLong(nonce);
        return buffer.array();
    }

    public static byte[] reply(long nonce, long brokerId, int tcpPort, byte[] address) {
        int family = family(address);
        ByteBuffer buffer = bytes(4 + 1 + 2 + 8 + 8 + 1 + 2 + 1 + address.length);
        header(buffer, TYPE_REPLY);
        buffer.putLong(nonce);
        buffer.putLong(brokerId);
        buffer.put((byte) FLAG_TLS);
        buffer.putShort((short) tcpPort);
        buffer.put((byte) family);
        buffer.put(address);
        return buffer.array();
    }

    public static byte[] presence(long instanceId, int intervalMillis, String name) {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        if (nameBytes.length > 65535) {
            throw new IllegalArgumentException("presence name is too long");
        }
        ByteBuffer buffer = bytes(4 + 1 + 2 + 8 + 4 + 2 + nameBytes.length);
        header(buffer, TYPE_PRESENCE);
        buffer.putLong(instanceId);
        buffer.putInt(intervalMillis);
        buffer.putShort((short) nameBytes.length);
        buffer.put(nameBytes);
        return buffer.array();
    }

    public static Object decode(byte[] encoded) {
        if (encoded == null || encoded.length < 7) {
            throw new IllegalArgumentException("truncated discovery datagram");
        }
        if (encoded[0] != 'D' || encoded[1] != 'E' || encoded[2] != 'S' || encoded[3] != 'S') {
            throw new IllegalArgumentException("bad discovery magic");
        }
        ByteBuffer buffer = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        buffer.position(4);
        int type = buffer.get() & 0xff;
        int version = buffer.getShort() & 0xffff;
        return switch (type) {
            case TYPE_REQUEST -> requestFrom(buffer, version, encoded.length);
            case TYPE_REPLY -> replyFrom(buffer, version, encoded.length);
            case TYPE_PRESENCE -> presenceFrom(buffer, version, encoded.length);
            default -> throw new IllegalArgumentException("unknown discovery type");
        };
    }

    public static Object tryDecode(byte[] encoded) {
        try {
            return decode(encoded);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    private static Request requestFrom(ByteBuffer buffer, int version, int length) {
        if (length != 15) {
            throw new IllegalArgumentException("truncated discovery datagram");
        }
        return new Request(version, buffer.getLong());
    }

    private static Reply replyFrom(ByteBuffer buffer, int version, int length) {
        if (length < 4 + 1 + 2 + 8 + 8 + 1 + 2 + 1 + 4) {
            throw new IllegalArgumentException("truncated discovery datagram");
        }
        long nonce = buffer.getLong();
        long brokerId = buffer.getLong();
        int flags = buffer.get() & 0xff;
        if ((flags & FLAG_TLS) == 0) {
            throw new IllegalArgumentException("discovery reply requires TLS");
        }
        int tcpPort = buffer.getShort() & 0xffff;
        int family = buffer.get() & 0xff;
        int addressLength = switch (family) {
            case FAMILY_IPV4 -> 4;
            case FAMILY_IPV6 -> 16;
            default -> throw new IllegalArgumentException("unknown address family");
        };
        if (buffer.remaining() != addressLength) {
            throw new IllegalArgumentException("truncated discovery datagram");
        }
        byte[] address = new byte[addressLength];
        buffer.get(address);
        return new Reply(version, nonce, brokerId, true, tcpPort, address);
    }

    private static Presence presenceFrom(ByteBuffer buffer, int version, int length) {
        if (length < 4 + 1 + 2 + 8 + 4 + 2) {
            throw new IllegalArgumentException("truncated discovery datagram");
        }
        long instanceId = buffer.getLong();
        int intervalMillis = buffer.getInt();
        int nameLength = buffer.getShort() & 0xffff;
        if (buffer.remaining() != nameLength) {
            throw new IllegalArgumentException("truncated discovery datagram");
        }
        byte[] name = new byte[nameLength];
        buffer.get(name);
        return new Presence(version, instanceId, intervalMillis, new String(name, StandardCharsets.UTF_8));
    }

    private static void header(ByteBuffer buffer, int type) {
        buffer.put(MAGIC);
        buffer.put((byte) type);
        buffer.putShort((short) FrameCodec.PROTOCOL_VERSION);
    }

    private static ByteBuffer bytes(int length) {
        return ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN);
    }

    private static int family(byte[] address) {
        if (address == null) {
            throw new IllegalArgumentException("address is required");
        }
        if (address.length == 4) {
            return FAMILY_IPV4;
        }
        if (address.length == 16) {
            return FAMILY_IPV6;
        }
        throw new IllegalArgumentException("address must be IPv4 or IPv6");
    }

    public record Request(int version, long nonce) {
    }

    public record Reply(int version, long nonce, long brokerId, boolean tls, int tcpPort, byte[] address) {
        public Reply {
            address = address == null ? new byte[0] : Arrays.copyOf(address, address.length);
        }
    }

    public record Presence(int version, long instanceId, int intervalMillis, String name) {
    }
}
