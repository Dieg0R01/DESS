package dev.dess.client;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.ConfirmationMode;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import dev.dess.common.protocol.StartPosition;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

public final class BrokerSession implements AutoCloseable {

    private final Socket socket;
    private final InputStream input;
    private final OutputStream output;
    private final Object writeLock = new Object();
    private boolean closed;

    private BrokerSession(Socket socket) throws IOException {
        this.socket = socket;
        this.input = socket.getInputStream();
        this.output = socket.getOutputStream();
    }

    public static BrokerSession connect(String host, int port, int heartbeatMs) throws IOException {
        Socket socket = new Socket();
        socket.setTcpNoDelay(true);
        try {
            socket.connect(new InetSocketAddress(host, port), 2_000);
        } catch (IOException ex) {
            socket.close();
            throw new IOException("broker unreachable at " + host + ":" + port, ex);
        }
        socket.setSoTimeout(Math.max(heartbeatMs, 2_000));
        BrokerSession session = new BrokerSession(socket);
        try {
            session.handshake();
            socket.setSoTimeout(heartbeatMs);
            return session;
        } catch (IOException ex) {
            session.close();
            throw ex;
        }
    }

    public void publish(String topic, byte[] payload) throws IOException {
        publish(topic, payload, ConfirmationMode.BROKER);
    }

    public void publish(String topic, byte[] payload, ConfirmationMode mode) throws IOException {
        write(new Frame(Command.PUBLISH, FrameCodec.publishHeader(topic, mode), payload));
    }

    public void acknowledge(String topic, long offset) throws IOException {
        write(new Frame(Command.ACK, FrameCodec.clientAckHeader(topic, offset), new byte[0]));
    }

    public void subscribe(String topic) throws IOException {
        subscribe(topic, StartPosition.latest());
    }

    public void subscribe(String topic, StartPosition start) throws IOException {
        write(new Frame(Command.SUBSCRIBE, FrameCodec.subscribeHeader(topic, start), new byte[0]));
    }

    public void unsubscribe(String topic) throws IOException {
        write(new Frame(Command.UNSUBSCRIBE, FrameCodec.topicHeader(topic), new byte[0]));
    }

    public Frame readFrame() throws IOException {
        while (true) {
            try {
                return FrameCodec.read(input);
            } catch (SocketTimeoutException ex) {
                write(new Frame(Command.PING, new byte[0], new byte[0]));
            }
        }
    }

    @Override
    public void close() {
        synchronized (writeLock) {
            if (closed) {
                return;
            }
            closed = true;
            try {
                socket.shutdownOutput();
            } catch (IOException ex) {
                // The peer is already gone.
            }
            try {
                socket.close();
            } catch (IOException ex) {
                // The socket is already closed.
            }
        }
    }

    private void handshake() throws IOException {
        write(new Frame(Command.HELLO, FrameCodec.helloHeader(FrameCodec.PROTOCOL_VERSION), new byte[0]));
        Frame reply = FrameCodec.read(input);
        if (reply.command() != Command.HELLO || FrameCodec.helloVersion(reply.header()) != FrameCodec.PROTOCOL_VERSION) {
            throw new IOException("handshake failed");
        }
    }

    private void write(Frame frame) throws IOException {
        synchronized (writeLock) {
            FrameCodec.write(output, frame);
        }
    }
}
