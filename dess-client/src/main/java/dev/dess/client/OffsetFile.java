package dev.dess.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;

final class OffsetFile {

    private final Path file;

    private final Map<String, Long> offsets = new LinkedHashMap<>();

    private OffsetFile(Path file) {
        this.file = file;
    }

    static OffsetFile open(Path file) throws IOException {
        OffsetFile offsets = new OffsetFile(file);
        offsets.load();
        return offsets;
    }

    Long stored(String topic) {
        return offsets.get(topic);
    }

    void acknowledge(String topic, long offset) throws IOException {
        offsets.put(topic, offset);
        store();
    }

    private void load() throws IOException {
        if (Files.notExists(file)) {
            return;
        }
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.isEmpty()) {
                continue;
            }
            int space = line.indexOf(' ');
            if (space <= 0 || space == line.length() - 1) {
                throw new IOException("offset file " + file.toAbsolutePath() + " is damaged");
            }
            String topic = line.substring(0, space);
            try {
                offsets.put(topic, Long.parseUnsignedLong(line.substring(space + 1)));
            } catch (NumberFormatException ex) {
                throw new IOException("offset file " + file.toAbsolutePath() + " is damaged", ex);
            }
        }
    }

    private void store() throws IOException {
        Path parent = file.getParent();
        if (parent == null) {
            parent = Path.of(".");
        }
        Path temporary = parent.resolve(file.getFileName().toString() + ".tmp");
        byte[] bytes = text().getBytes(StandardCharsets.UTF_8);
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                if (channel.write(buffer) < 0) {
                    throw new IOException("offset file " + temporary.toAbsolutePath() + " could not be written");
                }
            }
            channel.force(true);
        }
        Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private String text() {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Long> entry : offsets.entrySet()) {
            builder.append(entry.getKey()).append(' ').append(entry.getValue()).append('\n');
        }
        return builder.toString();
    }
}
