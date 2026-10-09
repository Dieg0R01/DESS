package dev.dess.broker;

import dev.dess.common.cli.Help;
import dev.dess.common.logging.Logging;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public final class BrokerMain {

    static final String USAGE = "Usage: dess-broker [--port N] [--max-connections N] [--max-payload-bytes N] [--idle-timeout-ms N] [--log-level LEVEL]";

    private BrokerMain() {
    }

    public static void main(String[] args) {
        System.exit(execute(args, System.out, System.err));
    }

    static int execute(String[] args, PrintStream out, PrintStream err) {
        String[] safeArgs = args == null ? new String[0] : args;
        if (asksForHelp(safeArgs)) {
            return Help.execute("dess-broker", BrokerMain.class, helpArgs(safeArgs), out, err, USAGE);
        }
        Request request = Request.parse(safeArgs);
        if (request == null) {
            return Help.execute("dess-broker", BrokerMain.class, safeArgs, out, err, USAGE);
        }
        Logging.configure(request.logLevel());
        Broker broker;
        try {
            broker = Broker.start(request.config());
        } catch (IOException ex) {
            Logging.log("ERROR", "dev.dess.broker", bindMessage(request.config().port(), ex));
            return 1;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            broker.close("SIGNAL");
            System.out.flush();
            Runtime.getRuntime().halt(0);
        }, "dess-broker-shutdown"));
        try {
            Thread.currentThread().join();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        return 0;
    }

    private static boolean asksForHelp(String[] args) {
        for (String arg : args) {
            if ("--help".equals(arg) || "-h".equals(arg)) {
                return true;
            }
        }
        return false;
    }

    private static String[] helpArgs(String[] args) {
        List<String> kept = new ArrayList<>();
        for (int index = 0; index < args.length; index++) {
            if ("--log-level".equals(args[index]) && index + 1 < args.length) {
                kept.add(args[index]);
                kept.add(args[++index]);
            }
        }
        kept.add("--help");
        return kept.toArray(String[]::new);
    }

    private static String bindMessage(int port, IOException failure) {
        String portText = Integer.toString(port);
        if (failure.getMessage() != null && failure.getMessage().contains(portText)) {
            return failure.getMessage();
        }
        return "Could not bind port " + portText;
    }

    private record Request(BrokerConfig config, String logLevel) {

        private static Request parse(String[] args) {
            Integer port = null;
            Integer maxConnections = null;
            Integer maxPayloadBytes = null;
            Integer idleTimeoutMillis = null;
            String logLevel = null;
            for (int index = 0; index < args.length; index++) {
                switch (args[index]) {
                    case "--port" -> {
                        if (port != null) {
                            return null;
                        }
                        port = integer(value(args, index + 1), 0, 65535);
                        if (port == null) {
                            return null;
                        }
                        index++;
                    }
                    case "--max-connections" -> {
                        if (maxConnections != null) {
                            return null;
                        }
                        maxConnections = integer(value(args, index + 1), 1, Integer.MAX_VALUE);
                        if (maxConnections == null) {
                            return null;
                        }
                        index++;
                    }
                    case "--max-payload-bytes" -> {
                        if (maxPayloadBytes != null) {
                            return null;
                        }
                        maxPayloadBytes = integer(value(args, index + 1), 1, Integer.MAX_VALUE);
                        if (maxPayloadBytes == null) {
                            return null;
                        }
                        index++;
                    }
                    case "--idle-timeout-ms" -> {
                        if (idleTimeoutMillis != null) {
                            return null;
                        }
                        idleTimeoutMillis = integer(value(args, index + 1), 1, Integer.MAX_VALUE);
                        if (idleTimeoutMillis == null) {
                            return null;
                        }
                        index++;
                    }
                    case "--log-level" -> {
                        if (logLevel != null) {
                            return null;
                        }
                        logLevel = value(args, index + 1);
                        if (logLevel == null) {
                            return null;
                        }
                        index++;
                    }
                    default -> {
                        return null;
                    }
                }
            }
            BrokerConfig config = new BrokerConfig(
                    port == null ? BrokerConfig.DEFAULT_PORT : port,
                    maxConnections == null ? BrokerConfig.DEFAULT_MAX_CONNECTIONS : maxConnections,
                    maxPayloadBytes == null ? BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES : maxPayloadBytes,
                    idleTimeoutMillis == null ? BrokerConfig.DEFAULT_IDLE_TIMEOUT_MILLIS : idleTimeoutMillis);
            return new Request(config, logLevel == null ? "INFO" : logLevel);
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

        private static Integer integer(String raw, int min, int max) {
            if (raw == null) {
                return null;
            }
            try {
                int value = Integer.parseInt(raw);
                if (value < min || value > max) {
                    return null;
                }
                return value;
            } catch (NumberFormatException ex) {
                return null;
            }
        }
    }
}
