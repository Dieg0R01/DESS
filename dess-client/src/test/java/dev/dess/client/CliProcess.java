package dev.dess.client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

final class CliProcess {

    private CliProcess() {
    }

    static Process start(Path input, Path output, Path error, List<String> args) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add("dev.dess.client.ClientMain");
        command.addAll(args);
        ProcessBuilder builder = new ProcessBuilder(command);
        if (input != null) {
            builder.redirectInput(input.toFile());
        }
        if (output != null) {
            builder.redirectOutput(output.toFile());
        }
        if (error != null) {
            builder.redirectError(error.toFile());
        }
        return builder.start();
    }

    static Result run(Path input, List<String> args) throws IOException, InterruptedException {
        Path output = Files.createTempFile("dess-client-out", ".txt");
        Path error = Files.createTempFile("dess-client-err", ".txt");
        Process process = start(input, output, error, args);
        try {
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(2, TimeUnit.SECONDS);
            }
            return new Result(process.exitValue(), Files.readString(output), Files.readString(error));
        } finally {
            Files.deleteIfExists(output);
            Files.deleteIfExists(error);
        }
    }

    record Result(int exitCode, String stdout, String stderr) {
    }
}
