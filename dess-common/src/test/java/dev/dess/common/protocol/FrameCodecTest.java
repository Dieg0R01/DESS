package dev.dess.common.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

    private static byte[] headerFor(Command command) {
        return switch (command) {
            case HELLO -> FrameCodec.helloHeader(1);
            case PUBLISH, SUBSCRIBE, UNSUBSCRIBE, DELIVER -> FrameCodec.topicHeader("sales");
            case ACK -> FrameCodec.topicHeader("sales");
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
}
