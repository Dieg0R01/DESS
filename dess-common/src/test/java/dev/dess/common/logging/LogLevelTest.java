package dev.dess.common.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.cli.Help;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class LogLevelTest {

    @Test
    void unknownLevelWarnsAndStaysAtInfo() {
        String output = capture(() -> {
            Logging.configure("NOPE");
            Logging.log("DEBUG", "dev.dess.common.logging", "hidden");
            Logging.log("INFO", "dev.dess.common.logging", "visible");
        });
        String[] lines = output.split("\\R");
        assertEquals(2, lines.length);
        assertTrue(lines[0].contains(" WARN "));
        assertTrue(lines[0].contains("NOPE"));
        assertTrue(lines[1].contains(" INFO "));
        assertTrue(lines[1].endsWith(" visible"));
        assertFalse(output.contains("hidden"));
    }

    @Test
    void configurationNamesSelectTheSameLevels() {
        String fine = capture(() -> {
            Logging.configure("FINE");
            Logging.log("DEBUG", "dev.dess.common.logging", "shown");
        });
        assertTrue(fine.contains(" DEBUG "));
        assertTrue(fine.contains("shown"));
        assertFalse(fine.contains("Unknown log level"));

        String warning = capture(() -> {
            Logging.configure("WARNING");
            Logging.log("WARN", "dev.dess.common.logging", "kept");
            Logging.log("DEBUG", "dev.dess.common.logging", "hidden");
        });
        assertTrue(warning.contains(" WARN "));
        assertTrue(warning.contains("kept"));
        assertFalse(warning.contains("hidden"));
        assertFalse(warning.contains("Unknown log level"));

        String severe = capture(() -> {
            Logging.configure("SEVERE");
            Logging.log("ERROR", "dev.dess.common.logging", "loud");
            Logging.log("INFO", "dev.dess.common.logging", "quiet");
        });
        assertTrue(severe.contains(" ERROR "));
        assertTrue(severe.contains("loud"));
        assertFalse(severe.contains("quiet"));
        assertFalse(severe.contains("Unknown log level"));
    }

    @Test
    void debugFlagRecordsTheVersionLineAtDebug() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        capture(() -> {
            int code = Help.execute(
                    "dess-broker",
                    LogLevelTest.class,
                    new String[] {"--log-level", "DEBUG", "--help"},
                    new PrintStream(out, true, StandardCharsets.UTF_8),
                    new PrintStream(err, true, StandardCharsets.UTF_8));
            assertEquals(0, code);
        });
        assertEquals("", err.toString(StandardCharsets.UTF_8));
        String[] lines = out.toString(StandardCharsets.UTF_8).split("\\R");
        assertEquals(3, lines.length);
        assertTrue(lines[1].contains(" DEBUG "));
        assertTrue(lines[1].endsWith(" dev"));
    }

    private static String capture(Runnable action) {
        PrintStream original = System.out;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buffer, true, StandardCharsets.UTF_8));
        try {
            action.run();
            return buffer.toString(StandardCharsets.UTF_8);
        } finally {
            System.setOut(original);
        }
    }
}
