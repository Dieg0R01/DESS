package dev.dess.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ConsumeCommandTest {

    @Test
    void deliveriesPrintTopicAndUtf8Payload() throws Exception {
        Path error = Files.createTempFile("dess-consume-err", ".txt");
        try (ScriptedBroker broker = ScriptedBroker.start()) {
            Process process = CliProcess.start(null, null, error, List.of(
                    "consume", "--topic", "sales", "--topic", "billing",
                    "--port", Integer.toString(broker.port()), "--heartbeat-ms", "200"));
            Stdout stdout = new Stdout(process);
            try {
                assertTrue(broker.await(Command.SUBSCRIBE, "sales", Duration.ofSeconds(2)));
                assertTrue(broker.await(Command.SUBSCRIBE, "billing", Duration.ofSeconds(2)));
                assertEquals(List.of("sales", "billing"), broker.topics(Command.SUBSCRIBE));
                assertTrue(broker.await(Command.PING, null, Duration.ofSeconds(2)));
                broker.deliver("sales", "hello".getBytes(StandardCharsets.UTF_8));
                broker.deliver("billing", new byte[] {(byte) 0xff});
                String text = stdout.await("billing");
                String newline = System.lineSeparator();
                assertTrue(text.contains("sales hello" + newline), text);
                assertTrue(text.contains("billing \uFFFD" + newline), text);
            } finally {
                stop(process);
                Files.deleteIfExists(error);
            }
        }
    }

    @Test
    void interruptUnsubscribesAndExitsZero() throws Exception {
        Path output = Files.createTempFile("dess-consume-out", ".txt");
        Path error = Files.createTempFile("dess-consume-err", ".txt");
        try (ScriptedBroker broker = ScriptedBroker.start()) {
            Process process = CliProcess.start(null, output, error, List.of(
                    "consume", "--topic", "sales", "--topic", "billing",
                    "--port", Integer.toString(broker.port()), "--heartbeat-ms", "10000"));
            try {
                assertTrue(broker.await(Command.SUBSCRIBE, "billing", Duration.ofSeconds(2)));
                Process kill = new ProcessBuilder("kill", "-INT", Long.toString(process.pid())).start();
                assertEquals(0, kill.waitFor());
                assertTrue(process.waitFor(5, TimeUnit.SECONDS), Files.readString(error));
                assertEquals(0, process.exitValue(), Files.readString(error));
                assertTrue(broker.await(Command.UNSUBSCRIBE, "billing", Duration.ofSeconds(2)));
                assertEquals(List.of("sales", "billing"), broker.topics(Command.UNSUBSCRIBE));
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                }
                Files.deleteIfExists(output);
                Files.deleteIfExists(error);
            }
        }
    }

    @Test
    void refusedConnectExitsOneAndNamesHostPort() throws Exception {
        CliProcess.Result result = CliProcess.run(null, List.of(
                "consume", "--topic", "sales", "--host", "127.0.0.1", "--port", "1"));
        assertEquals(1, result.exitCode(), result.stderr());
        assertTrue(result.stderr().contains("127.0.0.1:1"), result.stderr());
    }

    private static final class Stdout {

        private final StringBuilder text = new StringBuilder();
        private final LinkedBlockingQueue<String> lines = new LinkedBlockingQueue<>();

        private Stdout(Process process) {
            Thread reader = new Thread(() -> pump(process), "dess-client-stdout");
            reader.setDaemon(true);
            reader.start();
        }

        private void pump(Process process) {
            try (BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = input.readLine()) != null) {
                    synchronized (text) {
                        text.append(line).append(System.lineSeparator());
                    }
                    lines.put(line);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            } catch (IOException ex) {
                // The client process closed its output.
            }
        }

        private String await(String needle) throws InterruptedException {
            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
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

    private static void stop(Process process) throws InterruptedException, IOException {
        if (!process.isAlive()) {
            return;
        }
        Process kill = new ProcessBuilder("kill", "-INT", Long.toString(process.pid())).start();
        kill.waitFor();
        if (!process.waitFor(3, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(2, TimeUnit.SECONDS);
        }
    }
}
