package dev.dess.broker;

import dev.dess.common.logging.Logging;
import dev.dess.common.protocol.ClientAck;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

    /** Concurrent map of topics this connection is on. */
    private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();

    private final LinkedBlockingQueue<Subscription> ready = new LinkedBlockingQueue<>();

    private final ConnectionWriter writer = new ConnectionWriter(this);

    private volatile State state = State.HANDSHAKING;

    private volatile boolean closing;

    private volatile boolean slow;

    /** CLOSED, TIMEOUT, ERROR, or SLOW_CONSUMER. Shutdown is logged as SHUTDOWN. */
    private volatile String closeReason = "CLOSED";

    /** Zero while idle. {@link System#nanoTime()} while a delivery write is in progress. */
    private volatile long writingSince;

    private final AtomicBoolean writeWatch = new AtomicBoolean();

    private boolean cleaned;

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
    }

    public long id() {
        return id;
    }

    boolean isClosing() {
        return closing;
    }

    Runnable writer() {
        return writer;
    }

    void remember(Subscription subscription) {
        subscriptions.put(subscription.topic().name(), subscription);
    }

    void forget(String topic) {
        subscriptions.remove(topic);
    }

    void enqueue(Subscription subscription) {
        ready.offer(subscription);
    }

    Subscription pollReady() throws InterruptedException {
        return ready.poll(200, TimeUnit.MILLISECONDS);
    }

    boolean isSlow() {
        return slow;
    }

    void markSlow() {
        slow = true;
        if (!"SLOW_CONSUMER".equals(closeReason)) {
            closeReason = "SLOW_CONSUMER";
        }
    }

    long writingSince() {
        return writingSince;
    }

    void clearWriteWatch() {
        writeWatch.set(false);
    }

    boolean armWriteWatch() {
        return writeWatch.compareAndSet(false, true);
    }

    boolean outboundFull() {
        for (Subscription subscription : subscriptions.values()) {
            if (subscription.topic().isFull(subscription)) {
                return true;
            }
        }
        return false;
    }

    void beginWriting() {
        writingSince = System.nanoTime();
        if (armWriteWatch()) {
            broker.watchWrite(this);
        }
    }

    void endWriting() {
        writingSince = 0L;
    }

    void failSlow() {
        markSlow();
        beginWriting();
        try {
            writeFrame(ConnectionWriter.slowError());
        } catch (IOException ex) {
            // The error frame could not be written; the socket is closed below.
        } finally {
            endWriting();
        }
        cleanup();
    }

    void forceCloseSlow() {
        markSlow();
        cleanup();
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
        if (closing || state == State.CLOSING) {
            throw new IOException("connection is closing");
        }
        writeFrame(new Frame(Command.DELIVER, FrameCodec.deliverHeader(topic, offset), payload));
    }

    void cleanup() {
        closing = true;
        List<Subscription> owned = List.copyOf(subscriptions.values());
        subscriptions.clear();
        boolean first;
        synchronized (this) {
            first = !cleaned;
            cleaned = true;
            if (first) {
                state = State.CLOSING;
                permits.release();
            }
        }
        for (Subscription subscription : owned) {
            registry.remove(this, subscription.topic().name(), "DISCONNECT");
        }
        if (first) {
            registry.dropExpecting(id);
        }
        if (!first) {
            return;
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
        broker.startWriter(this);
    }

    private void subscribe(Frame frame) throws IOException {
        String topic;
        try {
            topic = FrameCodec.subscription(frame.header()).topic();
        } catch (FrameException ex) {
            fail(ErrorCode.MALFORMED_FRAME, "malformed topic");
            return;
        }
        if (!TopicName.isValid(topic)) {
            reject(topic, ErrorCode.INVALID_TOPIC_NAME, "invalid topic name");
            return;
        }
        if (!registry.subscribe(this, topic)) {
            reject(topic, ErrorCode.UNKNOWN_TOPIC, "unknown topic");
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
        Topic channel = registry.open(topic);
        if (channel == null) {
            reject(topic, ErrorCode.UNKNOWN_TOPIC, "unknown topic");
            return;
        }
        FlowControl flow = broker.flow();
        long started = System.nanoTime();
        long deadline = started + TimeUnit.MILLISECONDS.toNanos(config.publishWaitMillis());
        boolean waited = false;
        boolean acquired = false;
        Topic.AppendResult accepted;
        try {
            if (!flow.tryAcquireNow()) {
                waited = true;
                if (!flow.acquireUntil(deadline)) {
                    flow.throttled(System.nanoTime() - started, false);
                    reject(topic, ErrorCode.BROKER_BUSY, "broker is busy");
                    return;
                }
            }
            acquired = true;
            accepted = channel.append(id, payload, deadline, mode, config.ackTimeoutMillis());
        } catch (InterruptedException ex) {
            if (acquired) {
                flow.release();
            }
            Thread.currentThread().interrupt();
            closing = true;
            state = State.CLOSING;
            return;
        }
        if (!accepted.accepted()) {
            flow.release();
            flow.throttled(System.nanoTime() - started, false);
            reject(topic, ErrorCode.BROKER_BUSY, "broker is busy");
            return;
        }
        if (waited || accepted.waited()) {
            flow.throttled(System.nanoTime() - started, true);
        }
        long offset = accepted.offset();
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("topic", topic);
        properties.put("offset", Long.toString(offset));
        properties.put("sizeBytes", Integer.toString(payload.length));
        properties.put("clientId", Long.toString(id));
        Logging.event("dev.dess.broker", "message.accepted", properties);
        switch (mode) {
            case BROKER -> acknowledgeBroker(topic, offset);
            case ALL -> confirmAll(topic, offset);
            default -> {
                ConfirmationMode unexpected = mode;
                throw new IllegalStateException("Unhandled mode " + unexpected);
            }
        }
    }

    private void acknowledgeBroker(String topic, long offset) throws IOException {
        writeFrame(new Frame(Command.ACK, FrameCodec.brokerAckHeader(topic, offset, ConfirmationMode.BROKER, 0), new byte[0]));
    }

    private void confirmAll(String topic, long offset) throws IOException {
        PendingConfirmation confirmation = registry.confirmation(topic, offset);
        if (confirmation == null) {
            writeFrame(new Frame(Command.ACK, FrameCodec.brokerAckHeader(topic, offset, ConfirmationMode.ALL, 0), new byte[0]));
            return;
        }
        PendingConfirmation.Outcome outcome;
        try {
            outcome = confirmation.await();
        } catch (InterruptedException ex) {
            registry.discard(topic, offset);
            Thread.currentThread().interrupt();
            closing = true;
            state = State.CLOSING;
            return;
        }
        registry.discard(topic, offset);
        if (outcome.complete()) {
            writeFrame(new Frame(Command.ACK, FrameCodec.brokerAckHeader(topic, offset, ConfirmationMode.ALL, outcome.received()), new byte[0]));
            return;
        }
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("topic", topic);
        properties.put("offset", Long.toString(offset));
        properties.put("expected", Integer.toString(outcome.expectedCount()));
        properties.put("received", Integer.toString(outcome.received()));
        properties.put("clientId", Long.toString(id));
        Logging.event("dev.dess.broker", "ack.timeout", properties);
        writeFrame(new Frame(Command.ERROR, FrameCodec.ackTimeoutHeader(
                ErrorCode.ACK_TIMEOUT.name(), "acknowledgement timed out", topic, offset, outcome.expectedCount(), outcome.received()), new byte[0]));
    }

    private void acknowledge(Frame frame) throws IOException {
        if (frame.payload().length != 0) {
            fail(ErrorCode.MALFORMED_FRAME, "malformed ack");
            return;
        }
        ClientAck ack;
        try {
            ack = FrameCodec.clientAck(frame.header());
        } catch (FrameException ex) {
            fail(ErrorCode.MALFORMED_FRAME, "malformed ack");
            return;
        }
        Subscription subscription = subscriptions.get(ack.topic());
        if (subscription == null || !subscription.topic().delivered(subscription, ack.offset())) {
            Logging.log("WARN", "dev.dess.broker", "clientId=" + id + " topic=" + ack.topic() + " offset=" + ack.offset());
            return;
        }
        PendingConfirmation confirmation = registry.confirmation(ack.topic(), ack.offset());
        if (confirmation != null) {
            confirmation.acknowledge(id);
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
        closing = true;
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

}
