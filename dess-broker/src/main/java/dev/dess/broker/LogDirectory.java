package dev.dess.broker;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Exclusive ownership of one log directory. The lock is held until {@link #close()}.
 */
final class LogDirectory implements AutoCloseable {

    /** Released by {@link #close()}. Not used concurrently. */
    private final FileChannel channel;

    /** Released by {@link #close()}. Not used concurrently. */
    private final FileLock lock;

    private LogDirectory(FileChannel channel, FileLock lock) {
        this.channel = channel;
        this.lock = lock;
    }

    static LogDirectory acquire(Path directory) throws IOException {
        Files.createDirectories(directory);
        Path lockFile = directory.resolve("broker.lock");
        FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock lock;
        try {
            lock = channel.tryLock();
        } catch (OverlappingFileLockException ex) {
            IOException inUse = inUse(directory, ex);
            closeQuietly(channel, inUse);
            throw inUse;
        }
        if (lock == null) {
            IOException inUse = inUse(directory, null);
            closeQuietly(channel, inUse);
            throw inUse;
        }
        return new LogDirectory(channel, lock);
    }

    @Override
    public void close() throws IOException {
        try {
            lock.release();
        } finally {
            channel.close();
        }
    }

    private static IOException inUse(Path directory, Exception cause) {
        IOException failure = new IOException("Log directory " + directory.toAbsolutePath() + " is in use");
        if (cause != null) {
            failure.initCause(cause);
        }
        return failure;
    }

    private static void closeQuietly(FileChannel channel, IOException failure) {
        try {
            channel.close();
        } catch (IOException ex) {
            failure.addSuppressed(ex);
        }
    }
}
