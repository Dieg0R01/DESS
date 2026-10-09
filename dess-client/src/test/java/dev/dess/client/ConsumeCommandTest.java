package dev.dess.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ConsumeCommandTest {

    @Test
    void deliveriesPrintTopicAndUtf8Payload() throws Exception {
        Path output = Files.createTempFile("dess-consume-out", ".txt");
        Path error = Files.createTempFile("dess-consume-err", ".txt");
        try (ScriptedBroker broker = ScriptedBroker.start()) {
            Process process = CliProcess.start(null, output, error, List.of(
                    "consume", "--topic", "sales", "--topic", "billing",
                    "--port", Integer.toString(broker.port()), "--heartbeat-ms", "200"));
            try {
                assertTrue(broker.await(Command.SUBSCRIBE, "sales", Duration.ofSeconds(2)));
                assertTrue(broker.await(Command.SUBSCRIBE, "billing", Duration.ofSeconds(2)));
                assertEquals(List.of("sales", "billing"), broker.topics(Command.SUBSCRIBE));
                assertTrue(broker.await(Command.PING, null, Duration.ofSeconds(2)));
                broker.deliver("sales", "hello".getBytes(StandardCharsets.UTF_8));
                broker.deliver("billing", new byte[] {(byte) 0xff});
                String text = awaitText(output, "billing");
                String newline = System.lineSeparator();
                assertTrue(text.contains("sales hello" + newline), text);
                assertTrue(text.contains("billing \uFFFD" + newline), text);
            } finally {
                stop(process);
                Files.deleteIfExists(output);
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

    private static String awaitText(Path output, String needle) throws IOException {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        String text = "";
        while (System.nanoTime() < deadline) {
            text = Files.readString(output);
            if (text.contains(needle)) {
                return text;
            }
        }
        return text;
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
