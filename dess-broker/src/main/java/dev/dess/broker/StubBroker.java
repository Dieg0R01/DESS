package dev.dess.broker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;

public final class StubBroker implements AutoCloseable {

    private enum State {
        RUNNING,
        STOPPED
    }

    private final ServerSocket serverSocket;
    private final Thread acceptThread;
    private State state;

    public StubBroker() throws IOException {
        this.serverSocket = new ServerSocket(0);
        this.acceptThread = new Thread(this::acceptLoop, "dess-accept-" + serverSocket.getLocalPort());
        this.acceptThread.setDaemon(false);
        this.state = State.RUNNING;
        this.acceptThread.start();
    }

    public int port() {
        ensureRunning();
        return serverSocket.getLocalPort();
    }

    @Override
    public synchronized void close() {
        if (state == State.STOPPED) {
            return;
        }
        state = State.STOPPED;
        try {
            serverSocket.close();
        } catch (IOException ex) {
            throw new IllegalStateException("Could not close the broker socket", ex);
        }
        try {
            acceptThread.join(Duration.ofSeconds(2).toMillis());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping the accept thread", ex);
        }
        if (acceptThread.isAlive()) {
            throw new IllegalStateException("Accept thread " + acceptThread.getName() + " did not stop");
        }
    }

    private void acceptLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Socket accepted = serverSocket.accept();
                accepted.close();
            } catch (IOException ex) {
                return;
            }
        }
    }

    private synchronized void ensureRunning() {
        if (state != State.RUNNING) {
            throw new IllegalStateException("Broker is not running");
        }
    }
}
