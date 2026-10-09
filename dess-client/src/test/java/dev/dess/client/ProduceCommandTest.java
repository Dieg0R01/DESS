package dev.dess.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProduceCommandTest {

    @Test
    void oneMessageExitsZeroAfterAck() throws Exception {
        try (ScriptedBroker broker = ScriptedBroker.start()) {
            CliProcess.Result result = CliProcess.run(null, List.of(
                    "produce", "--topic", "sales", "--message", "hello", "--port", Integer.toString(broker.port())));
            assertEquals(0, result.exitCode(), result.stderr());
            assertEquals("", result.stdout());
            List<byte[]> payloads = payloads(broker);
            assertEquals(1, payloads.size());
            assertEquals("hello", new String(payloads.get(0), StandardCharsets.UTF_8));
            assertEquals(List.of("sales"), broker.topics(Command.PUBLISH));
        }
    }

    @Test
    void stdinLinesArePayloadsAndAnEmptyLineIsEmpty() throws Exception {
        Path input = Files.createTempFile("dess-produce", ".txt");
        try (ScriptedBroker broker = ScriptedBroker.start()) {
            Files.writeString(input, "alpha\n\nbeta\n");
            CliProcess.Result result = CliProcess.run(input, List.of(
                    "produce", "--topic", "sales", "--port", Integer.toString(broker.port())));
            assertEquals(0, result.exitCode(), result.stderr());
            List<byte[]> payloads = payloads(broker);
            assertEquals(3, payloads.size());
            assertEquals("alpha", new String(payloads.get(0), StandardCharsets.UTF_8));
            assertEquals(0, payloads.get(1).length);
            assertEquals("beta", new String(payloads.get(2), StandardCharsets.UTF_8));
        } finally {
            Files.deleteIfExists(input);
        }
    }

    @Test
    void errorFrameExitsOneAndNamesTheCode() throws Exception {
        try (ScriptedBroker broker = ScriptedBroker.start()) {
            broker.rejectPublishes("INVALID_TOPIC_NAME");
            CliProcess.Result result = CliProcess.run(null, List.of(
                    "produce", "--topic", "sales", "--message", "hello", "--port", Integer.toString(broker.port())));
            assertEquals(1, result.exitCode());
            assertTrue(result.stderr().contains("INVALID_TOPIC_NAME"), result.stderr());
        }
    }

    @Test
    void refusedConnectExitsOneAndNamesHostPort() throws Exception {
        CliProcess.Result result = CliProcess.run(null, List.of(
                "produce", "--topic", "sales", "--message", "hello", "--port", "1"));
        assertEquals(1, result.exitCode(), result.stderr());
        assertTrue(result.stderr().contains("127.0.0.1:1"), result.stderr());
    }

    private static List<byte[]> payloads(ScriptedBroker broker) {
        List<byte[]> payloads = new ArrayList<>();
        for (Frame frame : broker.frames()) {
            if (frame.command() == Command.PUBLISH) {
                payloads.add(frame.payload());
            }
        }
        return payloads;
    }
}
