package dev.dess.broker;

import dev.dess.common.logging.Logging;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.function.LongSupplier;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

final class TopicLog {

    private static final int VERSION = 1;

    private static final int FIXED_BODY = 25;

    /** The topic this file stores. */
    private final String topic;

    /** Guarded by {@link #appendLock}. Absolute path of the append-only file. */
    private final Path file;

    /** Guarded by {@link #appendLock}. */
    private final FileChannel channel;

    /** Held across the record write and, for {@link Durability#ALWAYS}, the force. */
    private final ReentrantLock appendLock = new ReentrantLock();

    private final Durability durability;

    /** Wall clock for {@code timestampMillis}. */
    private final LongSupplier clock;

    /** Guarded by {@link #appendLock}. Valid bytes. The next record starts here. */
    private long size;

    /** Guarded by {@link #appendLock}. Count of complete records. */
    private long nextOffset;

    /** Guarded by {@link #appendLock}. {@code positions[offset]} is the file position of that record. */
    private long[] positions;

    /** Torn tails removed while opening this file. Fixed at construction. */
    private final int truncatedRecords;

    private TopicLog(String topic, Path file, FileChannel channel, Durability durability, LongSupplier clock, long size, long nextOffset, long[] positions, int truncatedRecords) {
        this.topic = topic;
        this.file = file;
        this.channel = channel;
        this.durability = durability;
        this.clock = clock;
        this.size = size;
        this.nextOffset = nextOffset;
        this.positions = positions;
        this.truncatedRecords = truncatedRecords;
    }

    static TopicLog open(Path file, Durability durability) throws IOException {
        return open(file, durability, System::currentTimeMillis);
    }

    static TopicLog open(Path file, Durability durability, LongSupplier clock) throws IOException {
        FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            Scan scan = scan(file, channel);
            String fileName = file.getFileName().toString();
            String topic = fileName.substring(0, fileName.length() - 4);
            return new TopicLog(topic, file, channel, durability, clock, scan.size(), scan.nextOffset(), scan.positions(), scan.truncatedRecords());
        } catch (IOException ex) {
            channel.close();
            throw ex;
        }
    }

    String topic() {
        return topic;
    }

    int truncatedRecords() {
        return truncatedRecords;
    }

    long nextOffset() {
        appendLock.lock();
        try {
            return nextOffset;
        } finally {
            appendLock.unlock();
        }
    }

    Written append(byte[] payload) throws IOException {
        appendLock.lock();
        try {
            long offset = nextOffset;
            long timestamp = clock.getAsLong();
            long start = size;
            int written;
            try {
                written = writeRecord(offset, timestamp, payload, start);
                switch (durability) {
                    case ALWAYS -> channel.force(true);
                    case INTERVAL -> {
                    }
                    default -> {
                        Durability unexpected = durability;
                        throw new IllegalStateException("Unhandled durability " + unexpected);
                    }
                }
            } catch (IOException | RuntimeException ex) {
                try {
                    channel.truncate(start);
                } catch (IOException truncateEx) {
                    ex.addSuppressed(truncateEx);
                }
                if (ex instanceof IOException io) {
                    throw io;
                }
                throw (RuntimeException) ex;
            }
            remember(offset, start);
            size = start + written;
            nextOffset = offset + 1;
            return new Written(offset, timestamp);
        } finally {
            appendLock.unlock();
        }
    }

    void force() throws IOException {
        appendLock.lock();
        try {
            channel.force(true);
        } finally {
            appendLock.unlock();
        }
    }

    void close() throws IOException {
        appendLock.lock();
        try {
            channel.close();
        } finally {
            appendLock.unlock();
        }
    }

    private int writeRecord(long offset, long timestamp, byte[] payload, long position) throws IOException {
        int bodyLength = FIXED_BODY + payload.length;
        ByteBuffer record = ByteBuffer.allocate(4 + bodyLength);
        record.putInt(bodyLength);
        int crcStart = record.position();
        record.put((byte) VERSION);
        record.putLong(offset);
        record.putLong(timestamp);
        record.putInt(payload.length);
        record.put(payload);
        CRC32 crc = new CRC32();
        crc.update(record.array(), crcStart, record.position() - crcStart);
        record.putInt((int) crc.getValue());
        record.flip();
        long at = position;
        while (record.hasRemaining()) {
            int wrote = channel.write(record, at);
            if (wrote < 0) {
                throw new IOException("Could not append to " + file.toAbsolutePath());
            }
            at += wrote;
        }
        return record.capacity();
    }

    private void remember(long offset, long position) throws IOException {
        if (offset > Integer.MAX_VALUE) {
            throw new IOException("Too many records in " + file.toAbsolutePath());
        }
        int index = (int) offset;
        if (positions.length <= index) {
            int grown = positions.length == 0 ? 16 : positions.length;
            while (grown <= index) {
                grown *= 2;
            }
            positions = Arrays.copyOf(positions, grown);
        }
        positions[index] = position;
    }

    private static Scan scan(Path file, FileChannel channel) throws IOException {
        long fileSize = channel.size();
        long position = 0;
        long next = 0;
        long[] found = new long[0];
        int truncated = 0;
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        ByteBuffer body = ByteBuffer.allocate(256);
        while (position < fileSize) {
            if (fileSize - position < 4) {
                discardTail(file, channel, position);
                truncated = 1;
                break;
            }
            lengthBuffer.clear();
            readFully(channel, lengthBuffer, position);
            lengthBuffer.flip();
            int bodyLength = lengthBuffer.getInt();
            if (bodyLength < 0) {
                throw corrupt(file, position);
            }
            if (fileSize - position < 4L + bodyLength) {
                discardTail(file, channel, position);
                truncated = 1;
                break;
            }
            if (bodyLength < FIXED_BODY) {
                throw corrupt(file, position);
            }
            if (body.capacity() < bodyLength) {
                body = ByteBuffer.allocate(bodyLength);
            }
            body.clear();
            body.limit(bodyLength);
            readFully(channel, body, position + 4);
            body.flip();
            CRC32 crc = new CRC32();
            crc.update(body.array(), 0, bodyLength - 4);
            int version = body.get() & 0xff;
            long offset = body.getLong();
            body.getLong();
            int payloadLength = body.getInt();
            if (payloadLength < 0 || FIXED_BODY + (long) payloadLength != bodyLength) {
                throw corrupt(file, position);
            }
            body.position(body.position() + payloadLength);
            long actual = Integer.toUnsignedLong(body.getInt());
            if (version != VERSION || offset != next || actual != crc.getValue()) {
                throw corrupt(file, position);
            }
            if (next > Integer.MAX_VALUE) {
                throw new IOException("Too many records in " + file.toAbsolutePath());
            }
            if (found.length <= next) {
                found = Arrays.copyOf(found, found.length == 0 ? 16 : found.length * 2);
            }
            found[(int) next] = position;
            next++;
            position += 4L + bodyLength;
        }
        return new Scan(position, next, found, truncated);
    }

    private static void discardTail(Path file, FileChannel channel, long position) throws IOException {
        channel.truncate(position);
        channel.force(true);
        Logging.log("WARN", "dev.dess.broker", event("log.truncated", file, position));
    }

    private static IOException corrupt(Path file, long position) {
        Logging.log("ERROR", "dev.dess.broker", event("log.corrupt", file, position));
        return new IOException("Damaged record in " + file.toAbsolutePath() + " at " + position);
    }

    private static String event(String name, Path file, long position) {
        return "event=" + name + " file=" + quote(file.toAbsolutePath().toString()) + " position=" + position;
    }

    private static String quote(String value) {
        if (value.indexOf(' ') < 0 && value.indexOf('"') < 0 && value.indexOf('=') < 0) {
            return value;
        }
        return "\"" + value.replace("\"", "\\\"") + "\"";
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long at = position;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, at);
            if (read < 0) {
                throw new IOException("Short read at " + position);
            }
            at += read;
        }
    }

    record Written(long offset, long timestamp) {
    }

    private record Scan(long size, long nextOffset, long[] positions, int truncatedRecords) {
    }
}
