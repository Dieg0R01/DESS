package dev.dess.client;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

final class ScriptedBroker implements AutoCloseable {

    private final ServerSocket server;
    private final List<Frame> frames = new CopyOnWriteArrayList<>();
    private final Object writeLock = new Object();
    private final Thread acceptor;
    private volatile Socket client;
    private volatile OutputStream output;
    private volatile String publishError;

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

    void deliver(String topic, byte[] payload) throws IOException {
        write(new Frame(Command.DELIVER, FrameCodec.topicHeader(topic), payload));
    }

    List<Frame> frames() {
        return List.copyOf(frames);
    }

    boolean await(Command command, String topic, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (seen(command, topic)) {
                return true;
            }
        }
        return false;
    }

    List<String> topics(Command command) {
        List<String> names = new ArrayList<>();
        for (Frame frame : frames) {
            if (frame.command() == command) {
                names.add(FrameCodec.topic(frame.header()));
            }
        }
        return names;
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

    private boolean seen(Command command, String topic) {
        for (Frame frame : frames) {
            if (frame.command() != command) {
                continue;
            }
            if (topic == null || topic.equals(FrameCodec.topic(frame.header()))) {
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
            frames.add(hello);
            write(new Frame(Command.HELLO, FrameCodec.helloHeader(FrameCodec.PROTOCOL_VERSION), new byte[0]));
            while (true) {
                Frame frame = FrameCodec.read(input);
                frames.add(frame);
                reply(frame);
            }
        } catch (IOException ex) {
            // The client closed the socket, or the test closed the broker.
        }
    }

    private void reply(Frame frame) throws IOException {
        switch (frame.command()) {
            case PUBLISH -> {
                String code = publishError;
                if (code == null) {
                    write(new Frame(Command.ACK, frame.header(), new byte[0]));
                } else {
                    write(new Frame(Command.ERROR, FrameCodec.errorHeader(code, "rejected"), new byte[0]));
                }
            }
            case PING -> write(new Frame(Command.PONG, new byte[0], new byte[0]));
            case HELLO, SUBSCRIBE, UNSUBSCRIBE, DELIVER, ACK, PONG, ERROR -> {
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
