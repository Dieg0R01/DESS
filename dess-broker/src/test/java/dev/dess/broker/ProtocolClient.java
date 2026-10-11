package dev.dess.broker;

import dev.dess.common.protocol.BrokerAck;
import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.ConfirmationMode;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import dev.dess.common.protocol.StartPosition;
import dev.dess.common.security.Digests;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import javax.net.ssl.SSLSocket;

public final class ProtocolClient implements AutoCloseable {

    private final Socket socket;
    private final InputStream input;
    private final OutputStream output;

    private ProtocolClient(Socket socket) throws IOException {
        this.socket = socket;
        this.input = socket.getInputStream();
        this.output = socket.getOutputStream();
    }

    public static ProtocolClient connect(int port) throws IOException {
        SSLSocket socket = TestPki.clientSocket(port);
        socket.startHandshake();
        return new ProtocolClient(socket);
    }

    public void shrinkReceiveBuffer(int bytes) throws IOException {
        socket.setReceiveBufferSize(bytes);
    }

    public void abort() throws IOException {
        socket.setSoLinger(true, 0);
        socket.close();
    }

    public void hello() throws IOException {
        FrameCodec.write(output, new Frame(Command.HELLO, FrameCodec.helloHeader(FrameCodec.PROTOCOL_VERSION), new byte[0]));
        Frame reply = readFrame();
        if (reply.command() != Command.HELLO || FrameCodec.helloVersion(reply.header()) != FrameCodec.PROTOCOL_VERSION) {
            throw new IOException("broker hello was not version 4");
        }
    }

    public void publish(String topic, byte[] payload) throws IOException {
        publish(topic, payload, ConfirmationMode.BROKER);
    }

    public void publish(String topic, byte[] payload, ConfirmationMode mode) throws IOException {
        write(Command.PUBLISH, FrameCodec.publishHeader(topic, mode, Digests.of(topic, payload == null ? new byte[0] : payload)), payload);
    }

    public void acknowledge(String topic, long offset) throws IOException {
        write(Command.ACK, FrameCodec.clientAckHeader(topic, offset), new byte[0]);
    }

    public long deliverOffset(Frame frame) {
        return FrameCodec.delivery(frame.header()).offset();
    }

    public BrokerAck brokerAck(Frame frame) {
        return FrameCodec.brokerAck(frame.header());
    }

    public void subscribe(String topic) throws IOException {
        subscribe(topic, StartPosition.latest());
    }

    public void subscribe(String topic, StartPosition start) throws IOException {
        write(Command.SUBSCRIBE, FrameCodec.subscribeHeader(topic, start), new byte[0]);
    }

    public void unsubscribe(String topic) throws IOException {
        write(Command.UNSUBSCRIBE, FrameCodec.topicHeader(topic), new byte[0]);
    }

    public void ping() throws IOException {
        write(Command.PING, new byte[0], new byte[0]);
    }

    public void setReadTimeoutMillis(int millis) throws IOException {
        socket.setSoTimeout(millis);
    }

    public String endpoint() {
        return socket.getLocalAddress().getHostAddress() + ":" + socket.getLocalPort();
    }

    public Frame readFrame() throws IOException {
        return FrameCodec.read(input);
    }

    public void writeEncoded(byte[] encoded) throws IOException {
        output.write(encoded);
        output.flush();
    }

    @Override
    public void close() throws IOException {
        try {
            socket.setSoTimeout(1);
        } catch (IOException ex) {
            // Closing still proceeds.
        }
        socket.close();
    }

    private void write(Command command, byte[] header, byte[] payload) throws IOException {
        FrameCodec.write(output, new Frame(command, header, payload == null ? new byte[0] : payload));
    }
}
