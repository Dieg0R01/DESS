package dev.dess.broker;

import dev.dess.common.logging.Logging;
import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.ConfirmationMode;
import dev.dess.common.protocol.ErrorCode;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import dev.dess.common.protocol.FrameException;
import dev.dess.common.protocol.Publication;
import dev.dess.common.protocol.TopicName;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

public final class ClientConnection implements Runnable {

    private enum State {
        HANDSHAKING,
        ACTIVE,
        CLOSING
    }

    private final long id;
    private final Socket socket;
    private final InputStream input;
    private final OutputStream output;
    private final BrokerConfig config;
    private final TopicRegistry registry;
    private final Broker broker;
    private final Semaphore permits;
    private final String remoteAddress;
    private final Instant acceptedAt;
    private final Object outputLock = new Object();

    /** Guarded by itself. */
    private final Set<String> subscriptionNames = new HashSet<>();

    private static final Object SEND_BUFFER = new Object();

    private static int forcedSendBuffer;

    private volatile State state = State.HANDSHAKING;
    private volatile String closeReason = "CLOSED";
    private boolean cleaned;

    /** Limits the send buffer of connections accepted while set. Zero clears the limit. */
    static void forceSendBuffer(int bytes) {
        synchronized (SEND_BUFFER) {
            forcedSendBuffer = bytes;
        }
    }

    ClientConnection(long id, Socket socket, BrokerConfig config, TopicRegistry registry, Broker broker, Semaphore permits) throws IOException {
        this.id = id;
        this.socket = socket;
        this.input = socket.getInputStream();
        this.output = socket.getOutputStream();
        this.config = config;
        this.registry = registry;
        this.broker = broker;
        this.permits = permits;
        this.remoteAddress = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
        this.acceptedAt = Instant.now();
        socket.setTcpNoDelay(true);
        int sendBuffer;
        synchronized (SEND_BUFFER) {
            sendBuffer = forcedSendBuffer;
        }
        if (sendBuffer > 0) {
            socket.setSendBufferSize(sendBuffer);
        }
    }

    public long id() {
        return id;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("dess-client-" + id);
        try {
            socket.setSoTimeout(config.idleTimeoutMillis());
            while (state != State.CLOSING) {
                try {
                    handle(FrameCodec.read(input, config.maxPayloadBytes()));
                } catch (FrameException ex) {
                    if (!ex.fatal() && ex.code() == ErrorCode.PAYLOAD_TOO_LARGE) {
                        Map<String, String> properties = new LinkedHashMap<>();
                        String topic = ex.topic();
                        properties.put("topic", topic == null || topic.length() > 64 ? "-" : topic);
                        properties.put("errorCode", ex.code().name());
                        properties.put("clientId", Long.toString(id));
                        Logging.event("dev.dess.broker", "message.rejected", properties);
                    }
                    writeError(ex.code(), ex.getMessage());
                    if (ex.fatal()) {
                        closeReason = "ERROR";
                        state = State.CLOSING;
                    }
                }
            }
        } catch (SocketTimeoutException ex) {
            closeReason = "TIMEOUT";
        } catch (EOFException ex) {
            closeReason = broker.isShuttingDown() ? "SHUTDOWN" : "CLOSED";
        } catch (SocketException ex) {
            closeReason = broker.isShuttingDown() ? "SHUTDOWN" : "CLOSED";
        } catch (IOException ex) {
            closeReason = broker.isShuttingDown() ? "SHUTDOWN" : "ERROR";
        } finally {
            cleanup();
        }
    }

    void writeDeliver(String topic, long offset, byte[] payload) throws IOException {
        if (state == State.CLOSING) {
            throw new IOException("connection is closing");
        }
        writeFrame(new Frame(Command.DELIVER, FrameCodec.deliverHeader(topic, offset), payload));
    }

    void cleanup() {
        synchronized (this) {
            if (cleaned) {
                return;
            }
            cleaned = true;
            state = State.CLOSING;
            permits.release();
        }
        for (String name : names()) {
            registry.remove(this, name, "DISCONNECT");
        }
        closeSocket();
        broker.forget(this);
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("clientId", Long.toString(id));
        properties.put("reason", broker.isShuttingDown() ? "SHUTDOWN" : closeReason);
        properties.put("durationMs", Long.toString(Duration.between(acceptedAt, Instant.now()).toMillis()));
        Logging.event("dev.dess.broker", "client.disconnected", properties);
    }

    private void handle(Frame frame) throws IOException {
        if (state == State.HANDSHAKING && frame.command() != Command.HELLO) {
            fail(ErrorCode.HANDSHAKE_REQUIRED, "handshake required");
            return;
        }
        switch (frame.command()) {
            case HELLO -> hello(frame);
            case SUBSCRIBE -> subscribe(frame);
            case PUBLISH -> publish(frame);
            case UNSUBSCRIBE -> unsubscribe(frame);
            case PING -> writeFrame(new Frame(Command.PONG, new byte[0], new byte[0]));
            case ACK -> acknowledge(frame);
            case PONG, ERROR, DELIVER -> {
                // Server-originated frames from a client are ignored until confirmation modes exist.
            }
            default -> {
                Command unexpected = frame.command();
                throw new IllegalStateException("Unhandled command " + unexpected);
            }
        }
    }

    private void hello(Frame frame) throws IOException {
        if (state != State.HANDSHAKING) {
            fail(ErrorCode.MALFORMED_FRAME, "handshake already completed");
            return;
        }
        int version;
        try {
            version = FrameCodec.helloVersion(frame.header());
        } catch (FrameException ex) {
            fail(ErrorCode.MALFORMED_FRAME, "malformed hello");
            return;
        }
        if (version != FrameCodec.PROTOCOL_VERSION) {
            fail(ErrorCode.UNSUPPORTED_VERSION, "unsupported version");
            return;
        }
        writeFrame(new Frame(Command.HELLO, FrameCodec.helloHeader(FrameCodec.PROTOCOL_VERSION), new byte[0]));
        state = State.ACTIVE;
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("clientId", Long.toString(id));
        properties.put("remoteAddress", remoteAddress);
        properties.put("protocolVersion", Integer.toString(FrameCodec.PROTOCOL_VERSION));
        Logging.event("dev.dess.broker", "client.connected", properties);
    }

    private void subscribe(Frame frame) throws IOException {
        String topic = topic(frame);
        if (topic == null) {
            return;
        }
        if (!TopicName.isValid(topic)) {
            reject(topic, ErrorCode.INVALID_TOPIC_NAME, "invalid topic name");
            return;
        }
        registry.subscribe(this, topic);
        synchronized (subscriptionNames) {
            subscriptionNames.add(topic);
        }
    }

    private void unsubscribe(Frame frame) throws IOException {
        String topic = topic(frame);
        if (topic == null) {
            return;
        }
        if (!TopicName.isValid(topic)) {
            reject(topic, ErrorCode.INVALID_TOPIC_NAME, "invalid topic name");
            return;
        }
        registry.remove(this, topic, "UNSUBSCRIBE");
        synchronized (subscriptionNames) {
            subscriptionNames.remove(topic);
        }
    }

    private void publish(Frame frame) throws IOException {
        Publication publication;
        try {
            publication = FrameCodec.publication(frame.header());
        } catch (FrameException ex) {
            fail(ErrorCode.MALFORMED_FRAME, "malformed publish");
            return;
        }
        ConfirmationMode mode = publication.mode();
        switch (mode) {
            case BROKER, ALL -> {
            }
            default -> {
                ConfirmationMode unexpected = mode;
                throw new IllegalStateException("Unhandled mode " + unexpected);
            }
        }
        String topic = publication.topic();
        byte[] payload = frame.payload();
        if (!TopicName.isValid(topic)) {
            reject(topic, ErrorCode.INVALID_TOPIC_NAME, "invalid topic name");
            return;
        }
        if (payload.length > config.maxPayloadBytes()) {
            reject(topic, ErrorCode.PAYLOAD_TOO_LARGE, "payload is too large");
            return;
        }
        long offset;
        List<ClientConnection> failed = new ArrayList<>();
        while (true) {
            TopicRegistry.Topic channel = registry.open(topic);
            synchronized (channel) {
                if (!registry.isCurrent(channel)) {
                    continue;
                }
                offset = channel.takeOffset();
                for (ClientConnection subscriber : channel.snapshot()) {
                    try {
                        subscriber.writeDeliver(topic, offset, payload);
                    } catch (IOException ex) {
                        failed.add(subscriber);
                        registry.remove(subscriber, topic, "DISCONNECT");
                    }
                }
                break;
            }
        }
        for (ClientConnection subscriber : failed) {
            subscriber.cleanup();
        }
        writeFrame(new Frame(Command.ACK, FrameCodec.brokerAckHeader(topic, offset, ConfirmationMode.BROKER, 0), new byte[0]));
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("topic", topic);
        properties.put("offset", Long.toString(offset));
        properties.put("sizeBytes", Integer.toString(payload.length));
        properties.put("clientId", Long.toString(id));
        Logging.event("dev.dess.broker", "message.accepted", properties);
    }

    private void acknowledge(Frame frame) throws IOException {
        if (frame.payload().length != 0) {
            fail(ErrorCode.MALFORMED_FRAME, "malformed ack");
            return;
        }
        try {
            FrameCodec.clientAck(frame.header());
        } catch (FrameException ex) {
            fail(ErrorCode.MALFORMED_FRAME, "malformed ack");
        }
    }

    private String topic(Frame frame) throws IOException {
        try {
            return FrameCodec.topic(frame.header());
        } catch (FrameException ex) {
            fail(ErrorCode.MALFORMED_FRAME, "malformed topic");
            return null;
        }
    }

    private void reject(String topic, ErrorCode code, String reason) throws IOException {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("topic", topic.length() <= 64 ? topic : "-");
        properties.put("errorCode", code.name());
        properties.put("clientId", Long.toString(id));
        Logging.event("dev.dess.broker", "message.rejected", properties);
        writeError(code, reason);
    }

    private void fail(ErrorCode code, String reason) throws IOException {
        closeReason = "ERROR";
        writeError(code, reason);
        state = State.CLOSING;
    }

    private void writeError(ErrorCode code, String reason) throws IOException {
        writeFrame(new Frame(Command.ERROR, FrameCodec.errorHeader(code.name(), reason), new byte[0]));
    }

    private void writeFrame(Frame frame) throws IOException {
        synchronized (outputLock) {
            FrameCodec.write(output, frame);
        }
    }

    private void closeSocket() {
        if (socket.isClosed()) {
            return;
        }
        try {
            socket.shutdownOutput();
        } catch (IOException ex) {
            // The peer may already be gone.
        }
        try {
            socket.close();
        } catch (IOException ex) {
            // Closing a failed socket still completes cleanup.
        }
    }

    private List<String> names() {
        synchronized (subscriptionNames) {
            return List.copyOf(subscriptionNames);
        }
    }
}
