package dev.dess.admin;

import java.util.ArrayList;
import java.util.List;

final class AdminRequest {

    enum Verb {
        LIST,
        CREATE,
        DELETE,
        METRICS,
        HEALTH
    }

    static final String DEFAULT_URL = "http://127.0.0.1:55125";

    private final String url;
    private final boolean json;
    private final String logLevel;
    private final Verb verb;
    private final String name;

    private AdminRequest(String url, boolean json, String logLevel, Verb verb, String name) {
        this.url = url;
        this.json = json;
        this.logLevel = logLevel;
        this.verb = verb;
        this.name = name;
    }

    static AdminRequest parse(String[] args) {
        if (args == null || args.length == 0) {
            return null;
        }
        String url = null;
        boolean json = false;
        String logLevel = null;
        ArrayList<String> rest = new ArrayList<>();
        for (int index = 0; index < args.length; index++) {
            switch (args[index]) {
                case "--url" -> {
                    if (url != null) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    if (value == null) {
                        return null;
                    }
                    url = value;
                    index++;
                }
                case "--json" -> {
                    if (json) {
                        return null;
                    }
                    json = true;
                }
                case "--log-level" -> {
                    if (logLevel != null) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    if (value == null) {
                        return null;
                    }
                    logLevel = value;
                    index++;
                }
                default -> {
                    if (args[index].startsWith("-")) {
                        return null;
                    }
                    rest.add(args[index]);
                }
            }
        }
        ParsedCommand command = command(rest);
        if (command == null) {
            return null;
        }
        return new AdminRequest(
                url == null ? DEFAULT_URL : url,
                json,
                logLevel == null ? "INFO" : logLevel,
                command.verb,
                command.name);
    }

    String url() {
        return url;
    }

    boolean json() {
        return json;
    }

    String logLevel() {
        return logLevel;
    }

    Verb verb() {
        return verb;
    }

    String name() {
        return name;
    }

    private static ParsedCommand command(List<String> rest) {
        if (rest.isEmpty()) {
            return null;
        }
        return switch (rest.get(0)) {
            case "metrics" -> rest.size() == 1 ? new ParsedCommand(Verb.METRICS, null) : null;
            case "health" -> rest.size() == 1 ? new ParsedCommand(Verb.HEALTH, null) : null;
            case "topics" -> topics(rest);
            default -> null;
        };
    }

    private static ParsedCommand topics(List<String> rest) {
        if (rest.size() < 2) {
            return null;
        }
        return switch (rest.get(1)) {
            case "list" -> rest.size() == 2 ? new ParsedCommand(Verb.LIST, null) : null;
            case "create" -> rest.size() == 3 ? new ParsedCommand(Verb.CREATE, rest.get(2)) : null;
            case "delete" -> rest.size() == 3 ? new ParsedCommand(Verb.DELETE, rest.get(2)) : null;
            default -> null;
        };
    }

    private static String value(String[] args, int index) {
        if (index >= args.length) {
            return null;
        }
        String value = args[index];
        if (value.isEmpty() || value.startsWith("-")) {
            return null;
        }
        return value;
    }

    private record ParsedCommand(Verb verb, String name) {
    }
}
