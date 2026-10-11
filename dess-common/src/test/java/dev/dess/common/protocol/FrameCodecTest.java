package dev.dess.common.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class FrameCodecTest {

    @Test
    void roundTripsEveryCommandAndKeepsTheFrameLength() {
        for (Command command : Command.values()) {
            byte[] header = headerFor(command);
            byte[] payload = payloadFor(command);
            Frame frame = new Frame(command, header, payload);
            byte[] encoded = FrameCodec.encode(frame);
            int frameLength = ((encoded[0] & 0xff) << 24)
                    | ((encoded[1] & 0xff) << 16)
                    | ((encoded[2] & 0xff) << 8)
                    | (encoded[3] & 0xff);
            assertEquals(7 + header.length + payload.length, frameLength);
            assertEquals(4 + frameLength, encoded.length);

            Frame decoded = FrameCodec.decode(encoded);
            assertEquals(command, decoded.command());
            assertArrayEquals(header, decoded.header());
            assertArrayEquals(payload, decoded.payload());
        }
    }

    @Test
    void roundTripsATopicHeaderAndAnErrorHeader() {
        byte[] topic = FrameCodec.topicHeader("sales");
        assertEquals("sales", FrameCodec.topic(topic));

        byte[] error = FrameCodec.errorHeader("PAYLOAD_TOO_LARGE", "too big");
        assertEquals("PAYLOAD_TOO_LARGE", FrameCodec.errorCode(error));
        assertEquals("too big", FrameCodec.errorReason(error));
    }

    @Test
    void versionTwoHeadersRoundTripAndRejectABadShape() {
        long offset = Integer.MAX_VALUE + 1L;

        byte[] publish = FrameCodec.publishHeader("sales", ConfirmationMode.ALL);
        Publication publication = FrameCodec.publication(publish);
        assertEquals("sales", publication.topic());
        assertEquals(ConfirmationMode.ALL, publication.mode());

        byte[] deliver = FrameCodec.deliverHeader("stock", offset);
        Delivery delivery = FrameCodec.delivery(deliver);
        assertEquals("stock", delivery.topic());
        assertEquals(offset, delivery.offset());

        byte[] ack = FrameCodec.brokerAckHeader("sales", offset, ConfirmationMode.ALL, 3);
        BrokerAck brokerAck = FrameCodec.brokerAck(ack);
        assertEquals("sales", brokerAck.topic());
        assertEquals(offset, brokerAck.offset());
        assertEquals(ConfirmationMode.ALL, brokerAck.mode());
        assertEquals(3, brokerAck.acknowledgements());

        byte[] clientAck = FrameCodec.clientAckHeader("sales", offset);
        ClientAck decodedAck = FrameCodec.clientAck(clientAck);
        assertEquals("sales", decodedAck.topic());
        assertEquals(offset, decodedAck.offset());

        byte[] timeout = FrameCodec.ackTimeoutHeader("ACK_TIMEOUT", "too late", "sales", offset, 3, 1);
        AckTimeout decodedTimeout = FrameCodec.ackTimeout(timeout);
        assertEquals("ACK_TIMEOUT", decodedTimeout.code());
        assertEquals("too late", decodedTimeout.reason());
        assertEquals("sales", decodedTimeout.topic());
        assertEquals(offset, decodedTimeout.offset());
        assertEquals(3, decodedTimeout.expected());
        assertEquals(1, decodedTimeout.received());

        byte[] trailing = Arrays.copyOf(publish, publish.length + 1);
        FrameException extra = assertThrows(FrameException.class, () -> FrameCodec.publication(trailing));
        assertEquals(ErrorCode.MALFORMED_FRAME, extra.code());

        FrameException shortHeader = assertThrows(FrameException.class, () -> FrameCodec.publication(new byte[] {0}));
        assertEquals(ErrorCode.MALFORMED_FRAME, shortHeader.code());

        byte[] unknownMode = Arrays.copyOf(publish, publish.length);
        unknownMode[unknownMode.length - 1] = 2;
        FrameException badMode = assertThrows(FrameException.class, () -> FrameCodec.publication(unknownMode));
        assertEquals(ErrorCode.MALFORMED_FRAME, badMode.code());
    }

    @Test
    void versionThreeSubscribeHeadersRoundTripAndRejectABadShape() {
        assertEquals(3, FrameCodec.PROTOCOL_VERSION);
        long offset = Integer.MAX_VALUE + 1L;

        byte[] latest = FrameCodec.subscribeHeader("sales", StartPosition.latest());
        SubscriptionRequest latestRequest = FrameCodec.subscription(latest);
        assertEquals("sales", latestRequest.topic());
        assertEquals(StartPosition.Kind.LATEST, latestRequest.position().kind());

        byte[] earliest = FrameCodec.subscribeHeader("stock", StartPosition.earliest());
        SubscriptionRequest earliestRequest = FrameCodec.subscription(earliest);
        assertEquals("stock", earliestRequest.topic());
        assertEquals(StartPosition.Kind.EARLIEST, earliestRequest.position().kind());

        byte[] at = FrameCodec.subscribeHeader("sales", StartPosition.at(offset));
        SubscriptionRequest atRequest = FrameCodec.subscription(at);
        assertEquals("sales", atRequest.topic());
        assertEquals(StartPosition.Kind.OFFSET, atRequest.position().kind());
        assertEquals(offset, atRequest.position().offset());

        byte[] trailing = Arrays.copyOf(latest, latest.length + 1);
        FrameException extra = assertThrows(FrameException.class, () -> FrameCodec.subscription(trailing));
        assertEquals(ErrorCode.MALFORMED_FRAME, extra.code());

        byte[] shortOffset = Arrays.copyOf(at, at.length - 1);
        FrameException truncated = assertThrows(FrameException.class, () -> FrameCodec.subscription(shortOffset));
        assertEquals(ErrorCode.MALFORMED_FRAME, truncated.code());

        byte[] unknownKind = Arrays.copyOf(latest, latest.length);
        unknownKind[unknownKind.length - 1] = 3;
        FrameException badKind = assertThrows(FrameException.class, () -> FrameCodec.subscription(unknownKind));
        assertEquals(ErrorCode.MALFORMED_FRAME, badKind.code());
    }

    @Test
    void rejectsAHeaderLongerThan1024Bytes() throws IOException {
        FrameException onEncode = assertThrows(FrameException.class, () -> FrameCodec.encode(new Frame(Command.PING, new byte[1025], new byte[0])));
        assertEquals(ErrorCode.MALFORMED_FRAME, onEncode.code());

        byte[] encoded = rawFrame(7 + 1025, Command.PING.code(), new byte[1025], 0, new byte[0]);
        FrameException onDecode = assertThrows(FrameException.class, () -> FrameCodec.decode(encoded));
        assertEquals(ErrorCode.MALFORMED_FRAME, onDecode.code());
    }

    @Test
    void rejectsLengthsThatDoNotAddUp() throws IOException {
        byte[] extra = rawFrame(8, Command.PING.code(), new byte[0], 0, new byte[] {0});
        FrameException disagreed = assertThrows(FrameException.class, () -> FrameCodec.decode(extra));
        assertEquals(ErrorCode.MALFORMED_FRAME, disagreed.code());

        byte[] shortPayload = rawFrame(7, Command.PUBLISH.code(), new byte[0], 1, new byte[0]);
        FrameException truncated = assertThrows(FrameException.class, () -> FrameCodec.decode(shortPayload));
        assertEquals(ErrorCode.MALFORMED_FRAME, truncated.code());
    }

    @Test
    void oversizePayloadIsDiscardedInChunksAndTheNextFrameRemains() throws Exception {
        byte[] header = FrameCodec.topicHeader("sales");
        byte[] payload = new byte[20_000];
        byte[] ping = FrameCodec.encode(new Frame(Command.PING, new byte[0], new byte[0]));
        byte[] encoded = rawFrame(7 + header.length + payload.length, Command.PUBLISH.code(), header, payload.length, payload);
        byte[] both = new byte[encoded.length + ping.length];
        System.arraycopy(encoded, 0, both, 0, encoded.length);
        System.arraycopy(ping, 0, both, encoded.length, ping.length);
        InputStream input = new ChunkLimitStream(both);
        FrameException ex = assertThrows(FrameException.class, () -> FrameCodec.read(input, 4));
        assertEquals(ErrorCode.PAYLOAD_TOO_LARGE, ex.code());
        assertFalse(ex.fatal());
        assertEquals("sales", ex.topic());
        Frame next = FrameCodec.read(input, 4);
        assertEquals(Command.PING, next.command());
    }

    @Test
    void frameAboveTheCeilingIsRejectedWithoutTheBody() throws Exception {
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        new DataOutputStream(raw).writeInt(FrameCodec.MAX_FRAME_LENGTH + 1);
        FrameException ex = assertThrows(FrameException.class, () -> FrameCodec.read(new ByteArrayInputStream(raw.toByteArray())));
        assertEquals(ErrorCode.PAYLOAD_TOO_LARGE, ex.code());
        assertTrue(ex.fatal());
    }

    private static byte[] headerFor(Command command) {
        return switch (command) {
            case HELLO -> FrameCodec.helloHeader(FrameCodec.PROTOCOL_VERSION);
            case PUBLISH -> FrameCodec.publishHeader("sales", ConfirmationMode.BROKER);
            case SUBSCRIBE -> FrameCodec.subscribeHeader("sales", StartPosition.latest());
            case UNSUBSCRIBE -> FrameCodec.topicHeader("sales");
            case DELIVER -> FrameCodec.deliverHeader("sales", 0);
            case ACK -> FrameCodec.brokerAckHeader("sales", 0, ConfirmationMode.BROKER, 0);
            case PING, PONG -> new byte[0];
            case ERROR -> FrameCodec.errorHeader("MALFORMED_FRAME", "bad");
        };
    }

    private static byte[] payloadFor(Command command) {
        if (command == Command.PUBLISH || command == Command.DELIVER) {
            return "hello\n".getBytes(StandardCharsets.UTF_8);
        }
        return new byte[0];
    }

    private static byte[] rawFrame(int frameLength, int command, byte[] header, int payloadLength, byte[] payload) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeInt(frameLength);
        out.writeByte(command);
        out.writeShort(header.length);
        out.write(header);
        out.writeInt(payloadLength);
        out.write(payload);
        return buffer.toByteArray();
    }

    private static final class ChunkLimitStream extends InputStream {

        private final byte[] data;

        private int offset;

        private ChunkLimitStream(byte[] data) {
            this.data = data;
        }

        @Override
        public int read() {
            if (offset >= data.length) {
                return -1;
            }
            return data[offset++] & 0xff;
        }

        @Override
        public int read(byte[] destination, int off, int length) {
            if (length > 8192) {
                throw new IllegalArgumentException("read of " + length + " exceeds 8192");
            }
            if (offset >= data.length) {
                return -1;
            }
            int count = Math.min(length, data.length - offset);
            System.arraycopy(data, offset, destination, off, count);
            offset += count;
            return count;
        }
    }
}
