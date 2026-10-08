package dev.dess.common.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LogFormatTest {

    private static final String TIMESTAMP = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{2}:\\d{2}";

    @Test
    void plainAndEventLinesMatchTheContract() {
        String plain = capture(() -> Logging.log("INFO", "dev.dess.broker", "Broker ready"));
        assertLine(plain, "INFO", "dev.dess.broker", "Broker ready");

        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("version", "0.1.0");
        properties.put("tcpPort", "55123");
        String event = capture(() -> Logging.event("broker.started", properties));
        assertLine(event, "INFO", "dev.dess.common.logging", "event=broker.started version=0.1.0 tcpPort=55123");

        Map<String, String> quoted = new LinkedHashMap<>();
        quoted.put("note", "disk full");
        quoted.put("detail", "say \"hi\"");
        String special = capture(() -> Logging.event("broker.started", quoted));
        assertLine(special, "INFO", "dev.dess.common.logging", "event=broker.started note=\"disk full\" detail=\"say \\\"hi\\\"\"");
    }

    private static void assertLine(String stdout, String level, String component, String message) {
        String line = stdout.strip();
        assertTrue(line.matches(TIMESTAMP + " " + level + " \\S+ " + component + " " + java.util.regex.Pattern.quote(message)));
        assertEquals(message, line.split(" ", 5)[4]);
    }

    private static String capture(Runnable action) {
        PrintStream original = System.out;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        System.setOut(new PrintStream(buffer, true, StandardCharsets.UTF_8));
        try {
            Logging.configure("INFO");
            action.run();
            return buffer.toString(StandardCharsets.UTF_8);
        } finally {
            System.setOut(original);
        }
    }
}
