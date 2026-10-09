package dev.dess.common.protocol;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class FrameCodec {

    public static final int MAX_HEADER_LENGTH = 1024;

    public static final int MAX_FRAME_LENGTH = 16 * 1024 * 1024 + 1031;

    public static final int PROTOCOL_VERSION = 1;

    private static final int DISCARD_CHUNK = 8192;

    private FrameCodec() {
    }

    public static byte[] encode(Frame frame) {
        byte[] header = frame.header();
        byte[] payload = frame.payload();
        if (header.length > MAX_HEADER_LENGTH) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "header is longer than 1024 bytes");
        }
        long frameLength = 7L + header.length + payload.length;
        if (frameLength > MAX_FRAME_LENGTH) {
            throw new FrameException(ErrorCode.PAYLOAD_TOO_LARGE, "frame is longer than the ceiling");
        }
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream(4 + (int) frameLength);
            DataOutputStream out = new DataOutputStream(buffer);
            out.writeInt((int) frameLength);
            out.writeByte(frame.command().code());
            out.writeShort(header.length);
            out.write(header);
            out.writeInt(payload.length);
            out.write(payload);
            return buffer.toByteArray();
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public static Frame decode(byte[] encoded) {
        if (encoded == null || encoded.length < 4) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "frame is truncated");
        }
        int frameLength = readInt(encoded, 0);
        if (frameLength > MAX_FRAME_LENGTH) {
            throw new FrameException(ErrorCode.PAYLOAD_TOO_LARGE, "frame is longer than the ceiling");
        }
        if (frameLength < 7 || encoded.length != 4 + frameLength) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "frame length does not match");
        }
        return parse(encoded, 4, frameLength);
    }

    public static void write(OutputStream output, Frame frame) throws IOException {
        output.write(encode(frame));
        output.flush();
    }

    public static Frame read(InputStream input) throws IOException {
        return read(input, MAX_FRAME_LENGTH);
    }

    public static Frame read(InputStream input, int maxPayloadBytes) throws IOException {
        int first = input.read();
        if (first < 0) {
            throw new EOFException("end of stream");
        }
        byte[] prefix = new byte[4];
        prefix[0] = (byte) first;
        readFully(input, prefix, 1, 3);
        long declared = readInt(prefix, 0) & 0xffffffffL;
        if (declared > MAX_FRAME_LENGTH) {
            throw new FrameException(ErrorCode.PAYLOAD_TOO_LARGE, "frame is longer than the ceiling");
        }
        if (declared < 7) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "frame is too short");
        }
        int command = readByte(input);
        int headerLength = readUnsignedShort(input);
        if (headerLength > MAX_HEADER_LENGTH) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "header is longer than 1024 bytes");
        }
        if (3L + headerLength + 4 > declared) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "frame is truncated");
        }
        byte[] header = new byte[headerLength];
        readFully(input, header, 0, headerLength);
        int payloadLength = readStreamInt(input);
        if (payloadLength < 0 || 7L + headerLength + payloadLength != declared) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "frame length does not match");
        }
        if (payloadLength > maxPayloadBytes) {
            discard(input, payloadLength);
            throw new FrameException(ErrorCode.PAYLOAD_TOO_LARGE, "payload is too large", false, topicOrNull(header));
        }
        byte[] payload = new byte[payloadLength];
        readFully(input, payload, 0, payloadLength);
        return new Frame(Command.fromCode(command), header, payload);
    }

    public static byte[] helloHeader(int version) {
        if (version < 0 || version > 65535) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "version does not fit in 16 bits");
        }
        return new byte[] {(byte) (version >>> 8), (byte) version};
    }

    public static int helloVersion(byte[] header) {
        if (header == null || header.length != 2) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "hello header must be 2 bytes");
        }
        return ((header[0] & 0xff) << 8) | (header[1] & 0xff);
    }

    public static byte[] topicHeader(String topic) {
        byte[] text = utf8Bytes(topic);
        if (text.length > 65535) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "topic is too long");
        }
        byte[] header = new byte[2 + text.length];
        header[0] = (byte) (text.length >>> 8);
        header[1] = (byte) text.length;
        System.arraycopy(text, 0, header, 2, text.length);
        return header;
    }

    public static String topic(byte[] header) {
        if (header == null || header.length < 2) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "topic header is truncated");
        }
        int length = ((header[0] & 0xff) << 8) | (header[1] & 0xff);
        if (length != header.length - 2) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "topic length does not match");
        }
        return utf8(Arrays.copyOfRange(header, 2, header.length));
    }

    public static byte[] errorHeader(String code, String reason) {
        byte[] codeBytes = utf8Bytes(code);
        byte[] reasonBytes = utf8Bytes(reason == null ? "" : reason);
        if (codeBytes.length > 65535 || reasonBytes.length > 65535) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "error header is too long");
        }
        byte[] header = new byte[2 + codeBytes.length + 2 + reasonBytes.length];
        header[0] = (byte) (codeBytes.length >>> 8);
        header[1] = (byte) codeBytes.length;
        System.arraycopy(codeBytes, 0, header, 2, codeBytes.length);
        int reasonAt = 2 + codeBytes.length;
        header[reasonAt] = (byte) (reasonBytes.length >>> 8);
        header[reasonAt + 1] = (byte) reasonBytes.length;
        System.arraycopy(reasonBytes, 0, header, reasonAt + 2, reasonBytes.length);
        return header;
    }

    public static String errorCode(byte[] header) {
        return errorParts(header)[0];
    }

    public static String errorReason(byte[] header) {
        return errorParts(header)[1];
    }

    private static Frame parse(byte[] encoded, int offset, int frameLength) {
        int commandCode = encoded[offset] & 0xff;
        int headerLength = ((encoded[offset + 1] & 0xff) << 8) | (encoded[offset + 2] & 0xff);
        if (headerLength > MAX_HEADER_LENGTH) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "header is longer than 1024 bytes");
        }
        int headerAt = offset + 3;
        int payloadLengthAt = headerAt + headerLength;
        if (payloadLengthAt + 4 > encoded.length) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "frame is truncated");
        }
        byte[] header = Arrays.copyOfRange(encoded, headerAt, payloadLengthAt);
        int payloadLength = readInt(encoded, payloadLengthAt);
        int payloadAt = payloadLengthAt + 4;
        if (payloadLength < 0 || payloadAt + payloadLength != encoded.length) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "payload length does not match");
        }
        if (7 + headerLength + payloadLength != frameLength) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "frame length does not match");
        }
        byte[] payload = Arrays.copyOfRange(encoded, payloadAt, encoded.length);
        return new Frame(Command.fromCode(commandCode), header, payload);
    }

    private static String[] errorParts(byte[] header) {
        if (header == null || header.length < 4) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "error header is truncated");
        }
        int codeLength = ((header[0] & 0xff) << 8) | (header[1] & 0xff);
        int reasonAt = 2 + codeLength;
        if (codeLength < 0 || reasonAt + 2 > header.length) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "error code length does not match");
        }
        int reasonLength = ((header[reasonAt] & 0xff) << 8) | (header[reasonAt + 1] & 0xff);
        if (reasonLength < 0 || reasonAt + 2 + reasonLength != header.length) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "error reason length does not match");
        }
        String code = utf8(Arrays.copyOfRange(header, 2, reasonAt));
        String reason = utf8(Arrays.copyOfRange(header, reasonAt + 2, header.length));
        return new String[] {code, reason};
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24)
                | ((bytes[offset + 1] & 0xff) << 16)
                | ((bytes[offset + 2] & 0xff) << 8)
                | (bytes[offset + 3] & 0xff);
    }

    private static void discard(InputStream input, int length) throws IOException {
        byte[] chunk = new byte[DISCARD_CHUNK];
        int remaining = length;
        while (remaining > 0) {
            int count = input.read(chunk, 0, Math.min(chunk.length, remaining));
            if (count < 0) {
                throw new FrameException(ErrorCode.MALFORMED_FRAME, "frame is truncated");
            }
            remaining -= count;
        }
    }

    private static String topicOrNull(byte[] header) {
        try {
            return topic(header);
        } catch (FrameException ex) {
            return null;
        }
    }

    private static int readByte(InputStream input) throws IOException {
        int value = input.read();
        if (value < 0) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "frame is truncated");
        }
        return value;
    }

    private static int readUnsignedShort(InputStream input) throws IOException {
        return (readByte(input) << 8) | readByte(input);
    }

    private static int readStreamInt(InputStream input) throws IOException {
        return (readByte(input) << 24) | (readByte(input) << 16) | (readByte(input) << 8) | readByte(input);
    }

    private static void readFully(InputStream input, byte[] dest, int offset, int length) throws IOException {
        int read = 0;
        while (read < length) {
            int count = input.read(dest, offset + read, length - read);
            if (count < 0) {
                throw new FrameException(ErrorCode.MALFORMED_FRAME, "frame is truncated");
            }
            read += count;
        }
    }

    private static byte[] utf8Bytes(String text) {
        if (text == null) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "text is required");
        }
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String utf8(byte[] bytes) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return decoder.decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException ex) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "invalid UTF-8");
        }
    }
}
