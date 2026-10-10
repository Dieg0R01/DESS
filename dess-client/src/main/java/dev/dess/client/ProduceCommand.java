package dev.dess.client;

import dev.dess.common.logging.Logging;
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

final class ProduceCommand {

    private ProduceCommand() {
    }

    static int execute(ClientRequest request, InputStream in, PrintStream err) {
        BrokerSession session;
        try {
            session = BrokerSession.connect(request.host(), request.port(), request.heartbeatMs());
        } catch (IOException ex) {
            return fail(err, unreachable(request, ex));
        }
        try {
            for (byte[] payload : payloads(request, in)) {
                session.publish(request.topics().get(0), payload);
                Frame reply = awaitReply(session);
                if (reply.command() == Command.ERROR) {
                    return fail(err, errorCode(reply));
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

    private static String errorCode(Frame frame) {
        try {
            return FrameCodec.errorCode(frame.header());
        } catch (FrameException ex) {
            return "ERROR";
        }
    }

    private static String unreachable(ClientRequest request, IOException ex) {
        String endpoint = request.host() + ":" + request.port();
        if (ex.getMessage() != null && ex.getMessage().contains(endpoint)) {
            return ex.getMessage();
        }
        return "broker unreachable at " + endpoint;
    }

    private static int fail(PrintStream err, String message) {
        err.println(Logging.format("ERROR", "dev.dess.client", message));
        return 1;
    }
}
