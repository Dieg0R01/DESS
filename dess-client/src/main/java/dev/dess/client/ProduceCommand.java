package dev.dess.client;

import dev.dess.common.logging.Logging;
import dev.dess.common.protocol.AckTimeout;
import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import dev.dess.common.protocol.FrameException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.net.ssl.SSLContext;

final class ProduceCommand {

    private ProduceCommand() {
    }

    static int execute(ClientRequest request, InputStream in, PrintStream err) {
        SSLContext tls;
        try {
            tls = ClientTls.context(request.identity(), request.trustStore(), request.passwordFile());
        } catch (IOException ex) {
            return fail(err, ex.getMessage());
        }
        Discovery.Endpoint endpoint;
        try {
            endpoint = Discovery.resolve(request);
        } catch (IOException ex) {
            return fail(err, "no broker found");
        }
        if (endpoint == null) {
            return fail(err, "no broker found");
        }
        BrokerSession session;
        try {
            session = BrokerSession.connect(endpoint.host(), endpoint.port(), request.heartbeatMs(), tls);
        } catch (IOException ex) {
            return fail(err, unreachable(endpoint, ex));
        }
        try {
            for (byte[] payload : payloads(request, in)) {
                session.publish(request.topics().get(0), payload, request.confirm());
                Frame reply = awaitReply(session);
                if (reply.command() == Command.ERROR) {
                    return fail(err, errorText(reply));
                }
                if (reply.command() != Command.ACK) {
                    return fail(err, "unexpected " + reply.command());
                }
                try {
                    FrameCodec.brokerAck(reply.header());
                } catch (FrameException ex) {
                    return fail(err, "malformed ack");
                }
            }
            return 0;
        } catch (IOException ex) {
            return fail(err, "connection closed");
        } finally {
            session.close();
        }
    }

    private static List<byte[]> payloads(ClientRequest request, InputStream in) throws IOException {
        if (request.message() != null) {
            return List.of(request.message().getBytes(StandardCharsets.UTF_8));
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        List<byte[]> payloads = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            payloads.add(line.getBytes(StandardCharsets.UTF_8));
        }
        return payloads;
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

    private static String errorText(Frame frame) {
        try {
            AckTimeout timeout = FrameCodec.ackTimeout(frame.header());
            return timeout.code() + " expected=" + timeout.expected() + " received=" + timeout.received();
        } catch (FrameException ex) {
            try {
                return FrameCodec.errorCode(frame.header());
            } catch (FrameException again) {
                return "ERROR";
            }
        }
    }

    private static String unreachable(Discovery.Endpoint endpoint, IOException ex) {
        String message = ex.getMessage();
        if (ClientTls.isHandshakeFailure(message)) {
            return message;
        }
        String target = endpoint.host() + ":" + endpoint.port();
        if (message != null && message.contains(target)) {
            return message;
        }
        return "broker unreachable at " + target;
    }

    private static int fail(PrintStream err, String message) {
        err.println(Logging.format("ERROR", "dev.dess.client", message));
        return 1;
    }
}
