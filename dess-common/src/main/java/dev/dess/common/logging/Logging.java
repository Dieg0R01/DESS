package dev.dess.common.logging;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;

public final class Logging {

    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    private static final Logger DESS = Logger.getLogger("dev.dess");

    private static StreamHandler handler;

    private static volatile LogLevel effective = LogLevel.INFO;

    private Logging() {
    }

    public static synchronized void configure(String level) {
        Optional<LogLevel> parsed = LogLevel.parse(level);
        effective = parsed.orElse(LogLevel.INFO);
        if (handler != null) {
            DESS.removeHandler(handler);
        }
        DESS.setUseParentHandlers(false);
        DESS.setLevel(effective.jul);
        handler = new FlushingHandler();
        DESS.addHandler(handler);
        if (parsed.isEmpty()) {
            log(LogLevel.WARN, "dev.dess.common.logging", "Unknown log level " + level + "; using INFO");
        }
    }

    public static String level() {
        return effective.name();
    }

    public static void log(String component, String message) {
        log(effective, component, message);
    }

    public static void log(String level, String component, String message) {
        LogLevel parsed = LogLevel.parse(level).orElseThrow(() -> new IllegalArgumentException("Unknown log level " + level));
        log(parsed, component, message);
    }

    public static String format(String level, String component, String message) {
        LogLevel parsed = LogLevel.parse(level).orElse(LogLevel.INFO);
        return line(parsed, component, message, System.currentTimeMillis());
    }

    public static void event(String name, Map<String, String> properties) {
        StringBuilder message = new StringBuilder("event=").append(name);
        for (Map.Entry<String, String> property : properties.entrySet()) {
            message.append(' ').append(property.getKey()).append('=').append(quote(property.getValue()));
        }
        log(LogLevel.INFO, "dev.dess.common.logging", message.toString());
    }

    static String line(LogLevel level, String component, String message, long epochMillis) {
        String timestamp = TIMESTAMP.format(Instant.ofEpochMilli(epochMillis).atZone(ZoneId.systemDefault()));
        return timestamp + " " + level.name() + " " + Thread.currentThread().getName() + " " + component + " " + message;
    }

    private static void log(LogLevel level, String component, String message) {
        Logger.getLogger(component).log(level.jul, message);
    }

    private static String quote(String value) {
        if (value == null) {
            return "";
        }
        if (value.indexOf(' ') < 0 && value.indexOf('"') < 0 && value.indexOf('=') < 0) {
            return value;
        }
        return "\"" + value.replace("\"", "\\\"") + "\"";
    }

    enum LogLevel {
        ERROR(Level.SEVERE),
        WARN(Level.WARNING),
        INFO(Level.INFO),
        DEBUG(Level.FINE);

        private final Level jul;

        LogLevel(Level jul) {
            this.jul = jul;
        }

        static Optional<LogLevel> parse(String raw) {
            if (raw == null || raw.isBlank()) {
                return Optional.empty();
            }
            try {
                return Optional.of(LogLevel.valueOf(raw.toUpperCase(Locale.ROOT)));
            } catch (IllegalArgumentException ex) {
                return Optional.empty();
            }
        }
    }

    private static final class DessFormatter extends Formatter {

        @Override
        public String format(LogRecord record) {
            LogLevel level = displayLevel(record.getLevel());
            return line(level, record.getLoggerName(), record.getMessage(), record.getMillis()) + System.lineSeparator();
        }

        private static LogLevel displayLevel(Level level) {
            for (LogLevel candidate : LogLevel.values()) {
                if (candidate.jul.equals(level)) {
                    return candidate;
                }
            }
            throw new IllegalStateException("No log level for " + level.getName());
        }
    }

    private static final class FlushingHandler extends StreamHandler {

        private FlushingHandler() {
            super(System.out, new DessFormatter());
            setLevel(Level.ALL);
        }

        @Override
        public synchronized void publish(LogRecord record) {
            super.publish(record);
            flush();
        }

        @Override
        public synchronized void close() {
            flush();
        }
    }
}
