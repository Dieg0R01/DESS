package dev.dess.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

final class FaultBrokerProcess implements AutoCloseable {

    private final Process process;
    private final Path error;
    private final Path logDirectory;
    private final Stdout stdout;
    private final int port;

    private FaultBrokerProcess(Process process, Path error, Path logDirectory, Stdout stdout, int port) {
        this.process = process;
        this.error = error;
        this.logDirectory = logDirectory;
        this.stdout = stdout;
        this.port = port;
    }

    static FaultBrokerProcess start(Path workDirectory) throws Exception {
        Path logDirectory = workDirectory.resolve("logs");
        Files.createDirectories(logDirectory);
        Path config = workDirectory.resolve("config.xml");
        Path error = workDirectory.resolve("err.txt");
        writeConfig(config, logDirectory);
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add("dev.dess.broker.BrokerMain");
        command.add("--config");
        command.add(config.toString());
        Process process = new ProcessBuilder(command).redirectError(error.toFile()).start();
        Stdout stdout = new Stdout(process);
        try {
            String started = stdout.await("event=broker.started", Duration.ofSeconds(10));
            if (!started.contains("event=broker.started")) {
                process.destroyForcibly();
                process.waitFor(2, TimeUnit.SECONDS);
                throw new AssertionError(started + Files.readString(error));
            }
            return new FaultBrokerProcess(process, error, logDirectory, stdout, portFrom(started));
        } catch (Exception ex) {
            process.destroyForcibly();
            process.waitFor(2, TimeUnit.SECONDS);
            throw ex;
        }
    }

    int port() {
        return port;
    }

    Path logDirectory() {
        return logDirectory;
    }

    String output() {
        return stdout.text();
    }

    void destroyForcibly() throws InterruptedException {
        if (!process.isAlive()) {
            return;
        }
        process.destroyForcibly();
        if (!process.waitFor(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("broker process did not die: " + stdout.text());
        }
    }

    @Override
    public void close() {
        try {
            destroyForcibly();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
        try {
            Files.deleteIfExists(error);
        } catch (IOException ex) {
            // Temp file cleanup is best-effort.
        }
    }

    private static void writeConfig(Path file, Path logDirectory) throws IOException {
        String document = """
                <broker>
                    <network><port>0</port></network>
                    <storage>
                        <logDirectory>%s</logDirectory>
                        <durability>ALWAYS</durability>
                    </storage>
                    <topics autoCreate="true"/>
                    %s
                    <discovery enabled="false" presence="false"><port>0</port></discovery>
                    <admin><bindAddress>127.0.0.1</bindAddress><port>0</port></admin>
                </broker>
                """.formatted(logDirectory.toAbsolutePath(), TestConfigs.securityXml());
        Files.writeString(file, document);
    }

    private static int portFrom(String text) {
        String marker = "tcpPort=";
        int at = text.indexOf(marker);
        if (at < 0) {
            throw new AssertionError(text);
        }
        int start = at + marker.length();
        int end = start;
        while (end < text.length() && Character.isDigit(text.charAt(end))) {
            end++;
        }
        return Integer.parseInt(text.substring(start, end));
    }

    private static final class Stdout {

        private final StringBuilder text = new StringBuilder();
        private final LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<>();

        private Stdout(Process process) {
            Thread reader = new Thread(() -> pump(process), "dess-fault-broker-stdout");
            reader.setDaemon(true);
            reader.start();
        }

        private void pump(Process process) {
            try (BufferedReader input = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = input.readLine()) != null) {
                    synchronized (text) {
                        text.append(line).append('\n');
                    }
                    lines.put(line);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            } catch (IOException ex) {
                // The broker closed its output.
            }
        }

        private String await(String needle, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            String current = text();
            if (current.contains(needle)) {
                return current;
            }
            while (true) {
                long wait = deadline - System.nanoTime();
                if (wait <= 0) {
                    return text();
                }
                String line = lines.poll(wait, TimeUnit.NANOSECONDS);
                if (line == null) {
                    return text();
                }
                current = text();
                if (current.contains(needle)) {
                    return current;
                }
            }
        }

        private String text() {
            synchronized (text) {
                return text.toString();
            }
        }
    }
}
