package dev.dess.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ClientHelpTest {

    @Test
    void helpFlagExitsZeroWithNameVersionAndUsage() throws Exception {
        Result help = run("--help");
        assertEquals(0, help.exitCode());
        assertEquals("", help.stderr());
        assertHelp(help.stdout(), "dess-client");

        Result shortHelp = run("-h");
        assertEquals(0, shortHelp.exitCode());
        assertEquals("", shortHelp.stderr());
        assertHelp(shortHelp.stdout(), "dess-client");
    }

    @Test
    void anyOtherInvocationExitsTwoWithUsageOnStderr() throws Exception {
        Result unknown = run("--bogus");
        assertEquals(2, unknown.exitCode());
        assertEquals("", unknown.stdout());
        assertUsage(unknown.stderr(), "dess-client");
    }

    private static void assertHelp(String stdout, String artifact) {
        String[] lines = stdout.split("\\R", -1);
        assertEquals(4, lines.length);
        assertEquals("", lines[3]);
        assertMessage(lines[0], "INFO", artifact);
        assertMessage(lines[1], "INFO", "dev");
        assertMessage(lines[2], "INFO", "Usage: " + artifact + " --help | -h | --log-level LEVEL");
    }

    private static void assertUsage(String stderr, String artifact) {
        String[] lines = stderr.split("\\R", -1);
        assertEquals(2, lines.length);
        assertEquals("", lines[1]);
        assertMessage(lines[0], "INFO", "Usage: " + artifact + " --help | -h | --log-level LEVEL");
    }

    private static void assertMessage(String line, String level, String message) {
        assertTrue(line.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{2}:\\d{2} " + level + " \\S+ \\S+ \\Q" + message + "\\E"));
    }

    private static Result run(String... args) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add("dev.dess.client.ClientMain");
        command.addAll(List.of(args));
        Path out = Files.createTempFile("dess-client-out", ".txt");
        Path err = Files.createTempFile("dess-client-err", ".txt");
        try {
            Process process = new ProcessBuilder(command).redirectOutput(out.toFile()).redirectError(err.toFile()).start();
            int code = process.waitFor();
            return new Result(code, Files.readString(out), Files.readString(err));
        } finally {
            Files.deleteIfExists(out);
            Files.deleteIfExists(err);
        }
    }

    private record Result(int exitCode, String stdout, String stderr) {
    }
}
