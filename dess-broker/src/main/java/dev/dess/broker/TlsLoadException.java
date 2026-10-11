package dev.dess.broker;

import java.io.IOException;
import java.util.List;

final class TlsLoadException extends IOException {

    private final List<String> problems;

    TlsLoadException(List<String> problems) {
        super(String.join(System.lineSeparator(), problems));
        this.problems = List.copyOf(problems);
    }

    List<String> problems() {
        return problems;
    }
}
