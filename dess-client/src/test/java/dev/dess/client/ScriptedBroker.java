package dev.dess.client;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.ConfirmationMode;
import dev.dess.common.protocol.ErrorCode;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import dev.dess.common.protocol.StartPosition;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final class ScriptedBroker implements AutoCloseable {

    private final ServerSocket server;
    private final List<Frame> frames = new ArrayList<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition frameArrived = lock.newCondition();
    private final Object writeLock = new Object();
    private final Thread acceptor;
    private volatile Socket client;
    private volatile OutputStream output;
    private volatile String publishError;
    private volatile String subscribeError;
    private volatile int[] acknowledgementTimeout;
    private long nextOffset;

    private ScriptedBroker(ServerSocket server) {
        this.server = server;
        this.acceptor = new Thread(this::serve, "dess-scripted-broker");
        this.acceptor.start();
    }

    static ScriptedBroker start() throws IOException {
        ServerSocket server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        return new ScriptedBroker(server);
    }

    int port() {
        return server.getLocalPort();
    }

    void rejectPublishes(String code) {
        publishError = code;
    }

    void failSubscribes(String code) {
        subscribeError = code;
    }

    void timeOutAcknowledgements(int expected, int received) {
        acknowledgementTimeout = new int[] {expected, received};
    }

    void deliver(String topic, byte[] payload) throws IOException {
        deliver(topic, nextOffset, payload);
    }

    void deliver(String topic, long offset, byte[] payload) throws IOException {
        write(new Frame(Command.DELIVER, FrameCodec.deliverHeader(topic, offset), payload));
        nextOffset = Math.max(nextOffset, offset + 1);
    }

    StartPosition startPosition(String topic) {
        lock.lock();
        try {
            for (Frame frame : frames) {
                if (frame.command() == Command.SUBSCRIBE && topic.equals(FrameCodec.leadingTopic(frame.header()))) {
                    return FrameCodec.subscription(frame.header()).position();
                }
            }
        } finally {
            lock.unlock();
        }
        throw new AssertionError(topic);
    }

    List<Frame> frames() {
        lock.lock();
        try {
            return List.copyOf(frames);
        } finally {
            lock.unlock();
        }
    }

    boolean await(Command command, String topic, Duration timeout) throws InterruptedException {
        long nanos = timeout.toNanos();
        lock.lock();
        try {
            while (!seen(command, topic)) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = frameArrived.awaitNanos(nanos);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    List<String> topics(Command command) {
        lock.lock();
        try {
            List<String> names = new ArrayList<>();
            for (Frame frame : frames) {
                if (frame.command() == command) {
                    names.add(FrameCodec.leadingTopic(frame.header()));
                }
            }
            return names;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        server.close();
        Socket current = client;
        if (current != null) {
            current.close();
        }
        try {
            acceptor.join(1_000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private void record(Frame frame) {
        lock.lock();
        try {
            frames.add(frame);
            frameArrived.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private boolean seen(Command command, String topic) {
        for (Frame frame : frames) {
            if (frame.command() != command) {
                continue;
            }
            if (topic == null || topic.equals(FrameCodec.leadingTopic(frame.header()))) {
                return true;
            }
        }
        return false;
    }

    private void serve() {
        try {
            Socket socket = server.accept();
            socket.setTcpNoDelay(true);
            client = socket;
            InputStream input = socket.getInputStream();
            output = socket.getOutputStream();
            Frame hello = FrameCodec.read(input);
            record(hello);
            write(new Frame(Command.HELLO, FrameCodec.helloHeader(FrameCodec.PROTOCOL_VERSION), new byte[0]));
            while (true) {
                Frame frame = FrameCodec.read(input);
                record(frame);
                reply(frame);
            }
        } catch (IOException ex) {
            // The client closed the socket, or the test closed the broker.
        }
    }

    private void reply(Frame frame) throws IOException {
        switch (frame.command()) {
            case PUBLISH -> {
                String topic = FrameCodec.publication(frame.header()).topic();
                int[] timeout = acknowledgementTimeout;
                String code = publishError;
                if (timeout != null) {
                    write(new Frame(Command.ERROR, FrameCodec.ackTimeoutHeader(
                            ErrorCode.ACK_TIMEOUT.name(), "acknowledgement timed out", topic, 0, timeout[0], timeout[1]), new byte[0]));
                } else if (code == null) {
                    write(new Frame(Command.ACK, FrameCodec.brokerAckHeader(topic, 0, ConfirmationMode.BROKER, 0), new byte[0]));
                } else {
                    write(new Frame(Command.ERROR, FrameCodec.errorHeader(code, "rejected"), new byte[0]));
                }
            }
            case SUBSCRIBE -> {
                FrameCodec.subscription(frame.header());
                String code = subscribeError;
                if (code != null) {
                    write(new Frame(Command.ERROR, FrameCodec.errorHeader(code, "rejected"), new byte[0]));
                }
            }
            case PING -> write(new Frame(Command.PONG, new byte[0], new byte[0]));
            case HELLO, UNSUBSCRIBE, DELIVER, ACK, PONG, ERROR -> {
            }
            default -> {
                Command unexpected = frame.command();
                throw new IllegalStateException("Unhandled command " + unexpected);
            }
        }
    }

    private void write(Frame frame) throws IOException {
        synchronized (writeLock) {
            FrameCodec.write(output, frame);
        }
    }
}
