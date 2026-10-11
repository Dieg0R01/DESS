package dev.dess.broker;

import dev.dess.common.protocol.TopicName;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

final class TopicLogs {

    private final Path directory;

    private final Durability durability;

    private final ConcurrentHashMap<String, TopicLog> logs = new ConcurrentHashMap<>();

    private final int files;

    private final long records;

    private final int truncatedRecords;

    private final long durationMs;

    private TopicLogs(Path directory, Durability durability, int files, long records, int truncatedRecords, long durationMs) {
        this.directory = directory;
        this.durability = durability;
        this.files = files;
        this.records = records;
        this.truncatedRecords = truncatedRecords;
        this.durationMs = durationMs;
    }

    static void deleteTemporary(Path directory) throws IOException {
        try (DirectoryStream<Path> children = Files.newDirectoryStream(directory)) {
            for (Path child : children) {
                String name = child.getFileName().toString();
                if (name.endsWith(".tmp") && Files.isRegularFile(child)) {
                    Files.deleteIfExists(child);
                }
            }
        }
    }

    static TopicLogs open(Path directory, Durability durability) throws IOException {
        long started = System.nanoTime();
        Files.createDirectories(directory);
        ConcurrentHashMap<String, TopicLog> opened = new ConcurrentHashMap<>();
        int files = 0;
        long records = 0;
        int truncatedRecords = 0;
        try (DirectoryStream<Path> children = Files.newDirectoryStream(directory)) {
            try {
                for (Path child : children) {
                    String name = child.getFileName().toString();
                    if ("broker.lock".equals(name) || name.endsWith(".tmp")) {
                        continue;
                    }
                    if (name.endsWith(".log") && Files.isDirectory(child)) {
                        continue;
                    }
                    if (!name.endsWith(".log") || !Files.isRegularFile(child)) {
                        throw new IOException("Unexpected log directory entry " + child.toAbsolutePath());
                    }
                    String topic = name.substring(0, name.length() - 4);
                    if (!TopicName.isValid(topic)) {
                        throw new IOException("Unexpected log directory entry " + child.toAbsolutePath());
                    }
                    TopicLog log = TopicLog.open(child, durability);
                    opened.put(topic, log);
                    files++;
                    records += log.nextOffset();
                    truncatedRecords += log.truncatedRecords();
                }
            } catch (IOException ex) {
                for (TopicLog log : opened.values()) {
                    try {
                        log.close();
                    } catch (IOException closeEx) {
                        ex.addSuppressed(closeEx);
                    }
                }
                throw ex;
            }
        }
        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        return new TopicLogs(directory, durability, files, records, truncatedRecords, durationMs, opened);
    }

    private TopicLogs(Path directory, Durability durability, int files, long records, int truncatedRecords, long durationMs, ConcurrentHashMap<String, TopicLog> opened) {
        this(directory, durability, files, records, truncatedRecords, durationMs);
        this.logs.putAll(opened);
    }

    int files() {
        return files;
    }

    long records() {
        return records;
    }

    int truncatedRecords() {
        return truncatedRecords;
    }

    long durationMs() {
        return durationMs;
    }

    List<TopicLog> recovered() {
        return List.copyOf(logs.values());
    }

    TopicLog openOrCreate(String topic) throws IOException {
        TopicLog existing = logs.get(topic);
        if (existing != null) {
            return existing;
        }
        synchronized (logs) {
            existing = logs.get(topic);
            if (existing != null) {
                return existing;
            }
            TopicLog created = TopicLog.open(directory.resolve(topic + ".log"), durability);
            logs.put(topic, created);
            return created;
        }
    }

    int forceAll() throws IOException {
        List<TopicLog> open = new ArrayList<>(logs.values());
        for (TopicLog log : open) {
            log.force();
        }
        return open.size();
    }

    void close() throws IOException {
        IOException failure = null;
        for (TopicLog log : logs.values()) {
            try {
                log.close();
            } catch (IOException ex) {
                if (failure == null) {
                    failure = ex;
                } else {
                    failure.addSuppressed(ex);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
