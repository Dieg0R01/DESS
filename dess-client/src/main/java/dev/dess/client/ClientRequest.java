package dev.dess.client;

import dev.dess.common.protocol.ConfirmationMode;
import dev.dess.common.protocol.StartPosition;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

final class ClientRequest {

    private final String command;
    private final List<String> topics;
    private final String message;
    private final String host;
    private final int port;
    private final int discoveryPort;
    private final int presenceMs;
    private final int heartbeatMs;
    private final String logLevel;
    private final ConfirmationMode confirm;
    private final StartPosition from;
    private final boolean resume;
    private final Path resumeFile;
    private final Path identity;
    private final Path trustStore;
    private final Path passwordFile;
    private final int payloadBytes;
    private final int producers;
    private final int consumers;
    private final int messages;

    private ClientRequest(String command, List<String> topics, String message, String host, int port, int discoveryPort, int presenceMs, int heartbeatMs, String logLevel, ConfirmationMode confirm, StartPosition from, boolean resume, Path resumeFile, Path identity, Path trustStore, Path passwordFile, int payloadBytes, int producers, int consumers, int messages) {
        this.command = command;
        this.topics = topics;
        this.message = message;
        this.host = host;
        this.port = port;
        this.discoveryPort = discoveryPort;
        this.presenceMs = presenceMs;
        this.heartbeatMs = heartbeatMs;
        this.logLevel = logLevel;
        this.confirm = confirm;
        this.from = from;
        this.resume = resume;
        this.resumeFile = resumeFile;
        this.identity = identity;
        this.trustStore = trustStore;
        this.passwordFile = passwordFile;
        this.payloadBytes = payloadBytes;
        this.producers = producers;
        this.consumers = consumers;
        this.messages = messages;
    }

    static ClientRequest parse(String[] args) {
        if (args == null || args.length == 0) {
            return null;
        }
        String command = args[0];
        if (!"produce".equals(command) && !"consume".equals(command) && !"bench".equals(command)) {
            return null;
        }
        List<String> topics = new ArrayList<>();
        String message = null;
        boolean sawMessage = false;
        String host = null;
        boolean sawHost = false;
        Integer port = null;
        Integer discoveryPort = null;
        Integer presenceMs = null;
        Integer heartbeatMs = null;
        String logLevel = null;
        ConfirmationMode confirm = null;
        StartPosition from = null;
        boolean resume = false;
        Path resumeFile = null;
        Path identity = null;
        Path trustStore = null;
        Path passwordFile = null;
        Integer payloadBytes = null;
        Integer producers = null;
        Integer consumers = null;
        Integer messages = null;
        for (int index = 1; index < args.length; index++) {
            switch (args[index]) {
                case "--topic" -> {
                    String value = value(args, index + 1);
                    if (value == null) {
                        return null;
                    }
                    topics.add(value);
                    index++;
                }
                case "--message" -> {
                    if (!"produce".equals(command) || sawMessage) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    if (value == null) {
                        return null;
                    }
                    message = value;
                    sawMessage = true;
                    index++;
                }
                case "--host" -> {
                    if (sawHost) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    if (value == null) {
                        return null;
                    }
                    host = value;
                    sawHost = true;
                    index++;
                }
                case "--port" -> {
                    if (port != null) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    port = integer(value, 1, 65535);
                    if (port == null) {
                        return null;
                    }
                    index++;
                }
                case "--discovery-port" -> {
                    if (discoveryPort != null) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    discoveryPort = integer(value, 1, 65535);
                    if (discoveryPort == null) {
                        return null;
                    }
                    index++;
                }
                case "--presence-ms" -> {
                    if (presenceMs != null) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    presenceMs = integer(value, 1, Integer.MAX_VALUE);
                    if (presenceMs == null) {
                        return null;
                    }
                    index++;
                }
                case "--heartbeat-ms" -> {
                    if (heartbeatMs != null) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    heartbeatMs = integer(value, 1, Integer.MAX_VALUE);
                    if (heartbeatMs == null) {
                        return null;
                    }
                    index++;
                }
                case "--confirm" -> {
                    if (!"produce".equals(command) || confirm != null) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    confirm = confirmation(value);
                    if (confirm == null) {
                        return null;
                    }
                    index++;
                }
                case "--from" -> {
                    if (!"consume".equals(command) || from != null) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    from = start(value);
                    if (from == null) {
                        return null;
                    }
                    index++;
                }
                case "--resume" -> {
                    if (!"consume".equals(command) || resume) {
                        return null;
                    }
                    resume = true;
                }
                case "--resume-file" -> {
                    if (!"consume".equals(command) || resumeFile != null) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    if (value == null) {
                        return null;
                    }
                    resumeFile = Path.of(value);
                    index++;
                }
                case "--identity" -> {
                    if (identity != null) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    if (value == null) {
                        return null;
                    }
                    identity = Path.of(value);
                    index++;
                }
                case "--trust-store" -> {
                    if (trustStore != null) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    if (value == null) {
                        return null;
                    }
                    trustStore = Path.of(value);
                    index++;
                }
                case "--password-file" -> {
                    if (passwordFile != null) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    if (value == null) {
                        return null;
                    }
                    passwordFile = Path.of(value);
                    index++;
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
                case "--payload-bytes" -> {
                    if (!"bench".equals(command) || payloadBytes != null) {
                        return null;
                    }
                    payloadBytes = integer(value(args, index + 1), 1, Integer.MAX_VALUE);
                    if (payloadBytes == null) {
                        return null;
                    }
                    index++;
                }
                case "--producers" -> {
                    if (!"bench".equals(command) || producers != null) {
                        return null;
                    }
                    producers = integer(value(args, index + 1), 1, Integer.MAX_VALUE);
                    if (producers == null) {
                        return null;
                    }
                    index++;
                }
                case "--consumers" -> {
                    if (!"bench".equals(command) || consumers != null) {
                        return null;
                    }
                    consumers = integer(value(args, index + 1), 1, Integer.MAX_VALUE);
                    if (consumers == null) {
                        return null;
                    }
                    index++;
                }
                case "--messages" -> {
                    if (!"bench".equals(command) || messages != null) {
                        return null;
                    }
                    messages = integer(value(args, index + 1), 1, Integer.MAX_VALUE);
                    if (messages == null) {
                        return null;
                    }
                    index++;
                }
                default -> {
                    return null;
                }
            }
        }
        if (topics.isEmpty() || (("produce".equals(command) || "bench".equals(command)) && topics.size() != 1)) {
            return null;
        }
        if ("bench".equals(command) && messages == null) {
            return null;
        }
        if (resume && from != null) {
            return null;
        }
        if (port != null && !sawHost) {
            return null;
        }
        int resolvedPort = port == null ? 55123 : port;
        int resolvedDiscovery = discoveryPort == null ? 55124 : discoveryPort;
        int resolvedPresence = presenceMs == null ? 0 : presenceMs;
        int resolvedHeartbeat = heartbeatMs == null ? 10_000 : heartbeatMs;
        String resolvedLevel = logLevel == null ? "INFO" : logLevel;
        ConfirmationMode resolvedConfirm = confirm == null ? ConfirmationMode.BROKER : confirm;
        Path resolvedIdentity = identity != null
                ? identity
                : Path.of("consume".equals(command) ? "./config/pki/consumer.p12" : "./config/pki/producer.p12");
        Path resolvedTrust = trustStore != null ? trustStore : Path.of("./config/pki/trust.p12");
        Path resolvedPassword = passwordFile != null ? passwordFile : Path.of("./config/pki/password");
        int resolvedPayload = payloadBytes == null ? 1024 : payloadBytes;
        int resolvedProducers = producers == null ? 1 : producers;
        int resolvedConsumers = consumers == null ? 1 : consumers;
        int resolvedMessages = messages == null ? 0 : messages;
        return new ClientRequest(command, List.copyOf(topics), message, host, resolvedPort, resolvedDiscovery, resolvedPresence, resolvedHeartbeat, resolvedLevel, resolvedConfirm, from, resume, resumeFile, resolvedIdentity, resolvedTrust, resolvedPassword, resolvedPayload, resolvedProducers, resolvedConsumers, resolvedMessages);
    }

    String command() {
        return command;
    }

    List<String> topics() {
        return topics;
    }

    String message() {
        return message;
    }

    String host() {
        return host;
    }

    int port() {
        return port;
    }

    int discoveryPort() {
        return discoveryPort;
    }

    int presenceMs() {
        return presenceMs;
    }

    int heartbeatMs() {
        return heartbeatMs;
    }

    String logLevel() {
        return logLevel;
    }

    ConfirmationMode confirm() {
        return confirm;
    }

    StartPosition from() {
        return from;
    }

    boolean resume() {
        return resume;
    }

    Path resumeFile() {
        return resumeFile == null ? Path.of("dess-offsets") : resumeFile;
    }

    Path identity() {
        return identity;
    }

    Path trustStore() {
        return trustStore;
    }

    Path passwordFile() {
        return passwordFile;
    }

    Path consumerIdentity() {
        Path parent = identity.getParent();
        if (parent != null) {
            return parent.resolve("consumer.p12");
        }
        return Path.of("./config/pki/consumer.p12");
    }

    int payloadBytes() {
        return payloadBytes;
    }

    int producers() {
        return producers;
    }

    int consumers() {
        return consumers;
    }

    int messages() {
        return messages;
    }

    private static StartPosition start(String value) {
        if (value == null) {
            return null;
        }
        if ("latest".equals(value)) {
            return StartPosition.latest();
        }
        if ("earliest".equals(value)) {
            return StartPosition.earliest();
        }
        try {
            long offset = Long.parseLong(value);
            if (offset < 0) {
                return null;
            }
            return StartPosition.at(offset);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static ConfirmationMode confirmation(String value) {
        if ("BROKER".equals(value)) {
            return ConfirmationMode.BROKER;
        }
        if ("ALL".equals(value)) {
            return ConfirmationMode.ALL;
        }
        return null;
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
