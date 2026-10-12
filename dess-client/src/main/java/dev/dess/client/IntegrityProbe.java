package dev.dess.client;

import dev.dess.common.cli.Help;
import dev.dess.common.logging.Logging;
import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.ConfirmationMode;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import dev.dess.common.protocol.FrameException;
import dev.dess.common.security.Digests;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import javax.net.ssl.SSLContext;

public final class IntegrityProbe {

    static final String USAGE =
            "Usage: IntegrityProbe --topic NAME [--host HOST] [--port N] [--identity PATH] [--trust-store PATH] [--password-file PATH] [--heartbeat-ms N] [--log-level LEVEL]";

    private IntegrityProbe() {
    }

    public static void main(String[] args) {
        System.exit(execute(args, System.out, System.err));
    }

    static int execute(String[] args, PrintStream out, PrintStream err) {
        String[] safeArgs = args == null ? new String[0] : args;
        Request request = parse(safeArgs);
        if (request == null) {
            return Help.execute("IntegrityProbe", IntegrityProbe.class, safeArgs, out, err, USAGE);
        }
        Logging.configure(request.logLevel);
        SSLContext tls;
        try {
            tls = ClientTls.context(request.identity, request.trustStore, request.passwordFile);
        } catch (IOException ex) {
            err.println(Logging.format("ERROR", "dev.dess.client", ex.getMessage()));
            return 1;
        }
        BrokerSession session;
        try {
            session = BrokerSession.connect(request.host, request.port, request.heartbeatMs, tls);
        } catch (IOException ex) {
            err.println(Logging.format("ERROR", "dev.dess.client", unreachable(request, ex)));
            return 1;
        }
        try {
            byte[] payload = "probe".getBytes(StandardCharsets.UTF_8);
            session.publish(request.topic, payload, ConfirmationMode.BROKER, new byte[Digests.LENGTH]);
            Frame reply = awaitReply(session);
            if (reply.command() != Command.ERROR) {
                err.println(Logging.format("ERROR", "dev.dess.client", "unexpected " + reply.command()));
                return 1;
            }
            String code = errorCode(reply);
            out.println(code);
            return "INTEGRITY_FAILED".equals(code) ? 0 : 1;
        } catch (IOException ex) {
            err.println(Logging.format("ERROR", "dev.dess.client", "connection closed"));
            return 1;
        } finally {
            session.close();
        }
    }

    private static Frame awaitReply(BrokerSession session) throws IOException {
        while (true) {
            Frame frame = session.readFrame();
            switch (frame.command()) {
                case ACK, ERROR -> {
                    return frame;
                }
                case PONG -> {
                }
                case HELLO, PUBLISH, SUBSCRIBE, UNSUBSCRIBE, DELIVER, PING -> {
                    return frame;
                }
                default -> {
                    Command unexpected = frame.command();
                    throw new IllegalStateException("Unhandled command " + unexpected);
                }
            }
        }
    }

    private static String errorCode(Frame frame) {
        try {
            return FrameCodec.errorCode(frame.header());
        } catch (FrameException ex) {
            return "ERROR";
        }
    }

    private static String unreachable(Request request, IOException ex) {
        String message = ex.getMessage();
        if (ClientTls.isHandshakeFailure(message)) {
            return message;
        }
        String target = request.host + ":" + request.port;
        if (message != null && message.contains(target)) {
            return message;
        }
        return "broker unreachable at " + target;
    }

    private static Request parse(String[] args) {
        String topic = null;
        String host = null;
        Integer port = null;
        Path identity = null;
        Path trustStore = null;
        Path passwordFile = null;
        Integer heartbeatMs = null;
        String logLevel = null;
        for (int index = 0; index < args.length; index++) {
            switch (args[index]) {
                case "--topic" -> {
                    if (topic != null) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    if (value == null) {
                        return null;
                    }
                    topic = value;
                    index++;
                }
                case "--host" -> {
                    if (host != null) {
                        return null;
                    }
                    String value = value(args, index + 1);
                    if (value == null) {
                        return null;
                    }
                    host = value;
                    index++;
                }
                case "--port" -> {
                    if (port != null) {
                        return null;
                    }
                    port = integer(value(args, index + 1), 1, 65535);
                    if (port == null) {
                        return null;
                    }
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
                case "--heartbeat-ms" -> {
                    if (heartbeatMs != null) {
                        return null;
                    }
                    heartbeatMs = integer(value(args, index + 1), 1, Integer.MAX_VALUE);
                    if (heartbeatMs == null) {
                        return null;
                    }
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
                default -> {
                    return null;
                }
            }
        }
        if (topic == null) {
            return null;
        }
        if (port != null && host == null) {
            return null;
        }
        return new Request(
                topic,
                host == null ? "127.0.0.1" : host,
                port == null ? 55123 : port,
                identity != null ? identity : Path.of("./config/pki/producer.p12"),
                trustStore != null ? trustStore : Path.of("./config/pki/trust.p12"),
                passwordFile != null ? passwordFile : Path.of("./config/pki/password"),
                heartbeatMs == null ? 10_000 : heartbeatMs,
                logLevel == null ? "INFO" : logLevel);
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
            int parsed = Integer.parseInt(raw);
            if (parsed < min || parsed > max) {
                return null;
            }
            return parsed;
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private record Request(
            String topic,
            String host,
            int port,
            Path identity,
            Path trustStore,
            Path passwordFile,
            int heartbeatMs,
            String logLevel) {
    }
}
