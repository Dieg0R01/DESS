package dev.dess.common;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class CommonModuleBoundaryTest {

    private static final Path SOURCE_ROOT = Path.of("src", "main", "java");

    private static final Pattern FORBIDDEN_IMPORT = Pattern.compile(
            "^import\\s+(?:static\\s+)?(java\\.net\\.|javax\\.net\\.|java\\.nio\\.channels\\.|java\\.util\\.concurrent\\.)");

    @Test
    void mainSourcesDoNotImportNetworkingOrThreading() throws IOException {
        if (!Files.isDirectory(SOURCE_ROOT)) {
            fail("Expected dess-common sources at " + SOURCE_ROOT.toAbsolutePath());
        }
        List<String> violations = new ArrayList<>();
        try (Stream<Path> files = Files.walk(SOURCE_ROOT)) {
            List<Path> sources = files.filter(path -> path.toString().endsWith(".java")).toList();
            for (Path source : sources) {
                for (String line : Files.readAllLines(source)) {
                    Matcher matcher = FORBIDDEN_IMPORT.matcher(line.trim());
                    if (matcher.find()) {
                        violations.add(source + ": " + line.trim());
                    }
                }
            }
        }
        if (!violations.isEmpty()) {
            fail("Forbidden import in dess-common:%n%s".formatted(String.join(System.lineSeparator(), violations)));
        }
    }
}
