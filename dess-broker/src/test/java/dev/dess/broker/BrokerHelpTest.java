package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class BrokerHelpTest {

    @Test
    void helpFlagExitsZeroWithNameVersionAndUsage() throws Exception {
        Result help = run("--help");
        assertEquals(0, help.exitCode());
        assertEquals("", help.stderr());
        assertHelp(help.stdout(), "dess-broker");

        Result shortHelp = run("-h");
        assertEquals(0, shortHelp.exitCode());
        assertEquals("", shortHelp.stderr());
        assertHelp(shortHelp.stdout(), "dess-broker");

        Result helped = run("--port", "1", "--help");
        assertEquals(0, helped.exitCode());
        assertEquals("", helped.stderr());
        assertHelp(helped.stdout(), "dess-broker");
    }

    @Test
    void anyOtherInvocationExitsTwoWithUsageOnStderr() throws Exception {
        Result unknown = run("--bogus");
        assertEquals(2, unknown.exitCode());
        assertEquals("", unknown.stdout());
        assertUsage(unknown.stderr(), "dess-broker");

        Result missing = run("--config");
        assertEquals(2, missing.exitCode());
        assertEquals("", missing.stdout());
        assertUsage(missing.stderr(), "dess-broker");

        Result repeated = run("--config", "a.xml", "--config", "b.xml");
        assertEquals(2, repeated.exitCode());
        assertUsage(repeated.stderr(), "dess-broker");
    }

    private static void assertHelp(String stdout, String artifact) {
        String[] lines = stdout.split("\\R", -1);
        assertEquals(4, lines.length);
        assertEquals("", lines[3]);
        assertMessage(lines[0], "INFO", artifact);
        assertMessage(lines[1], "INFO", "dev");
        assertMessage(lines[2], "INFO", BrokerMain.USAGE);
    }

    private static void assertUsage(String stderr, String artifact) {
        String[] lines = stderr.split("\\R", -1);
        assertEquals(2, lines.length);
        assertEquals("", lines[1]);
        assertMessage(lines[0], "INFO", BrokerMain.USAGE);
    }

    private static void assertMessage(String line, String level, String message) {
        assertTrue(line.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{2}:\\d{2} " + level + " \\S+ \\S+ \\Q" + message + "\\E"));
    }

    private static Result run(String... args) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add("dev.dess.broker.BrokerMain");
        command.addAll(List.of(args));
        Path out = Files.createTempFile("dess-broker-out", ".txt");
        Path err = Files.createTempFile("dess-broker-err", ".txt");
        try {
            Process process = new ProcessBuilder(command).redirectOutput(out.toFile()).redirectError(err.toFile()).start();
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                if (!process.waitFor(2, TimeUnit.SECONDS)) {
                    throw new AssertionError("broker process did not exit");
                }
            }
            return new Result(process.exitValue(), Files.readString(out), Files.readString(err));
        } finally {
            Files.deleteIfExists(out);
            Files.deleteIfExists(err);
        }
    }

    private record Result(int exitCode, String stdout, String stderr) {
    }
}
