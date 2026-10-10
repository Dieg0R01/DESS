package dev.dess.broker;

import dev.dess.common.cli.Help;
import dev.dess.common.logging.Logging;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public final class BrokerMain {

    static final String USAGE = "Usage: dess-broker [--port N] [--max-connections N] [--max-payload-bytes N] [--idle-timeout-ms N] [--max-in-flight N] [--topic-queue-capacity N] [--outbound-capacity N] [--slow-consumer-grace-ms N] [--publish-wait-ms N] [--ack-timeout-ms N] [--auto-create-topics true|false] [--topic NAME] [--log-level LEVEL]";

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
            Integer maxInFlight = null;
            Integer topicQueueCapacity = null;
            Integer outboundCapacity = null;
            Integer slowConsumerGraceMillis = null;
            Integer publishWaitMillis = null;
            Integer ackTimeoutMillis = null;
            Boolean autoCreateTopics = null;
            List<String> topics = new ArrayList<>();
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
                    case "--max-in-flight" -> {
                        if (maxInFlight != null) {
                            return null;
                        }
                        maxInFlight = integer(value(args, index + 1), 1, Integer.MAX_VALUE);
                        if (maxInFlight == null) {
                            return null;
                        }
                        index++;
                    }
                    case "--topic-queue-capacity" -> {
                        if (topicQueueCapacity != null) {
                            return null;
                        }
                        topicQueueCapacity = integer(value(args, index + 1), 1, Integer.MAX_VALUE);
                        if (topicQueueCapacity == null) {
                            return null;
                        }
                        index++;
                    }
                    case "--outbound-capacity" -> {
                        if (outboundCapacity != null) {
                            return null;
                        }
                        outboundCapacity = integer(value(args, index + 1), 1, Integer.MAX_VALUE);
                        if (outboundCapacity == null) {
                            return null;
                        }
                        index++;
                    }
                    case "--slow-consumer-grace-ms" -> {
                        if (slowConsumerGraceMillis != null) {
                            return null;
                        }
                        slowConsumerGraceMillis = integer(value(args, index + 1), 1, Integer.MAX_VALUE);
                        if (slowConsumerGraceMillis == null) {
                            return null;
                        }
                        index++;
                    }
                    case "--publish-wait-ms" -> {
                        if (publishWaitMillis != null) {
                            return null;
                        }
                        publishWaitMillis = integer(value(args, index + 1), 0, Integer.MAX_VALUE);
                        if (publishWaitMillis == null) {
                            return null;
                        }
                        index++;
                    }
                    case "--ack-timeout-ms" -> {
                        if (ackTimeoutMillis != null) {
                            return null;
                        }
                        ackTimeoutMillis = integer(value(args, index + 1), 1, Integer.MAX_VALUE);
                        if (ackTimeoutMillis == null) {
                            return null;
                        }
                        index++;
                    }
                    case "--auto-create-topics" -> {
                        if (autoCreateTopics != null) {
                            return null;
                        }
                        String raw = value(args, index + 1);
                        if ("true".equals(raw)) {
                            autoCreateTopics = true;
                        } else if ("false".equals(raw)) {
                            autoCreateTopics = false;
                        } else {
                            return null;
                        }
                        index++;
                    }
                    case "--topic" -> {
                        String name = value(args, index + 1);
                        if (name == null) {
                            return null;
                        }
                        topics.add(name);
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
            try {
                BrokerConfig config = new BrokerConfig(
                        port == null ? BrokerConfig.DEFAULT_PORT : port,
                        maxConnections == null ? BrokerConfig.DEFAULT_MAX_CONNECTIONS : maxConnections,
                        maxPayloadBytes == null ? BrokerConfig.DEFAULT_MAX_PAYLOAD_BYTES : maxPayloadBytes,
                        idleTimeoutMillis == null ? BrokerConfig.DEFAULT_IDLE_TIMEOUT_MILLIS : idleTimeoutMillis,
                        maxInFlight == null ? BrokerConfig.DEFAULT_MAX_IN_FLIGHT : maxInFlight,
                        topicQueueCapacity == null ? BrokerConfig.DEFAULT_TOPIC_QUEUE_CAPACITY : topicQueueCapacity,
                        outboundCapacity == null ? BrokerConfig.DEFAULT_OUTBOUND_CAPACITY : outboundCapacity,
                        slowConsumerGraceMillis == null ? BrokerConfig.DEFAULT_SLOW_CONSUMER_GRACE_MILLIS : slowConsumerGraceMillis,
                        publishWaitMillis == null ? BrokerConfig.DEFAULT_PUBLISH_WAIT_MILLIS : publishWaitMillis,
                        ackTimeoutMillis == null ? BrokerConfig.DEFAULT_ACK_TIMEOUT_MILLIS : ackTimeoutMillis,
                        autoCreateTopics == null || autoCreateTopics,
                        topics);
                return new Request(config, logLevel == null ? "INFO" : logLevel);
            } catch (IllegalArgumentException ex) {
                return null;
            }
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
