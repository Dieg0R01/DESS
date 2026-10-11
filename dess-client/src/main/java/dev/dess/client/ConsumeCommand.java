package dev.dess.client;

import dev.dess.common.logging.Logging;
import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Delivery;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import dev.dess.common.protocol.FrameException;
import dev.dess.common.protocol.StartPosition;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

final class ConsumeCommand {

    private ConsumeCommand() {
    }

    static int execute(ClientRequest request, PrintStream out, PrintStream err) {
        BrokerSession session;
        try {
            session = BrokerSession.connect(request.host(), request.port(), request.heartbeatMs());
        } catch (IOException ex) {
            err.println(Logging.format("ERROR", "dev.dess.client", unreachable(request, ex)));
            return 1;
        }
        OffsetFile offsets = null;
        if (request.resume()) {
            try {
                offsets = OffsetFile.open(request.resumeFile());
            } catch (IOException ex) {
                session.close();
                err.println(Logging.format("ERROR", "dev.dess.client", ex.getMessage()));
                return 1;
            }
        }
        List<String> subscribed = new ArrayList<>();
        Thread hook = new Thread(() -> stop(session, subscribed), "dess-client-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);
        try {
            for (String topic : request.topics()) {
                session.subscribe(topic, position(request, offsets, topic));
                synchronized (subscribed) {
                    subscribed.add(topic);
                }
            }
            while (true) {
                Frame frame = session.readFrame();
                if (frame.command() == Command.ERROR) {
                    return fail(session, hook, err, errorCode(frame));
                }
                if (frame.command() == Command.DELIVER) {
                    Delivery delivery = FrameCodec.delivery(frame.header());
                    String text = new String(frame.payload(), StandardCharsets.UTF_8);
                    out.println(delivery.topic() + " " + text);
                    if (offsets != null) {
                        offsets.acknowledge(delivery.topic(), delivery.offset());
                    }
                    session.acknowledge(delivery.topic(), delivery.offset());
                }
            }
        } catch (IOException ex) {
            if (!removeHook(hook)) {
                return 0;
            }
            session.close();
            err.println(Logging.format("ERROR", "dev.dess.client", "connection closed"));
            return 1;
        }
    }

    private static StartPosition position(ClientRequest request, OffsetFile offsets, String topic) {
        if (offsets == null) {
            StartPosition chosen = request.from();
            return chosen == null ? StartPosition.latest() : chosen;
        }
        Long stored = offsets.stored(topic);
        if (stored == null) {
            return StartPosition.latest();
        }
        return StartPosition.at(stored + 1);
    }

    private static void stop(BrokerSession session, List<String> subscribed) {
        List<String> topics;
        synchronized (subscribed) {
            topics = List.copyOf(subscribed);
        }
        for (String topic : topics) {
            try {
                session.unsubscribe(topic);
            } catch (IOException ex) {
                break;
            }
        }
        session.close();
        Runtime.getRuntime().halt(0);
    }

    private static int fail(BrokerSession session, Thread hook, PrintStream err, String code) {
        if (!removeHook(hook)) {
            return 0;
        }
        session.close();
        err.println(Logging.format("ERROR", "dev.dess.client", code));
        return 1;
    }

    private static String errorCode(Frame frame) {
        try {
            return FrameCodec.errorCode(frame.header());
        } catch (FrameException ex) {
            return "ERROR";
        }
    }

    private static boolean removeHook(Thread hook) {
        try {
            return Runtime.getRuntime().removeShutdownHook(hook);
        } catch (IllegalStateException ex) {
            return false;
        }
    }

    private static String unreachable(ClientRequest request, IOException ex) {
        String endpoint = request.host() + ":" + request.port();
        if (ex.getMessage() != null && ex.getMessage().contains(endpoint)) {
            return ex.getMessage();
        }
        return "broker unreachable at " + endpoint;
    }
}
