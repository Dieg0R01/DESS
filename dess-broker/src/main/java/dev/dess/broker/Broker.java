package dev.dess.broker;

import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.ErrorCode;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class Broker implements AutoCloseable {

    private final BrokerConfig config;
    private final ServerSocket serverSocket;
    private final Thread acceptThread;
    private final ExecutorService executor;
    private final TopicRegistry registry = new TopicRegistry();
    private final ConcurrentHashMap<Long, ClientConnection> connections = new ConcurrentHashMap<>();
    private final Semaphore permits;
    private final AtomicLong ids = new AtomicLong();
    private volatile boolean running = true;
    private volatile boolean shuttingDown;

    private Broker(BrokerConfig config) throws IOException {
        this.config = config;
        this.permits = new Semaphore(config.maxConnections());
        this.serverSocket = new ServerSocket(config.port());
        this.executor = Executors.newFixedThreadPool(config.maxConnections(), runnable -> {
            Thread thread = new Thread(runnable);
            thread.setDaemon(false);
            return thread;
        });
        this.acceptThread = new Thread(this::acceptLoop, "dess-accept-" + serverSocket.getLocalPort());
        this.acceptThread.setDaemon(false);
        this.acceptThread.start();
    }

    public static Broker start(BrokerConfig config) throws IOException {
        return new Broker(config);
    }

    public int port() {
        if (!running) {
            throw new IllegalStateException("Broker is not running");
        }
        return serverSocket.getLocalPort();
    }

    public boolean isShuttingDown() {
        return shuttingDown;
    }

    void forget(ClientConnection connection) {
        connections.remove(connection.id());
    }

    @Override
    public void close() {
        synchronized (this) {
            if (!running) {
                return;
            }
            running = false;
            shuttingDown = true;
        }
        try {
            serverSocket.close();
        } catch (IOException ex) {
            throw new IllegalStateException("Could not close the broker socket", ex);
        }
        try {
            acceptThread.join(2_000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping the accept thread", ex);
        }
        if (acceptThread.isAlive()) {
            throw new IllegalStateException("Accept thread " + acceptThread.getName() + " did not stop");
        }
        for (ClientConnection connection : List.copyOf(connections.values())) {
            connection.cleanup();
        }
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Connection threads did not stop");
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping the broker", ex);
        }
    }

    private void acceptLoop() {
        while (running) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException ex) {
                return;
            }
            try {
                socket.setTcpNoDelay(true);
                if (!permits.tryAcquire()) {
                    refuse(socket);
                    continue;
                }
                ClientConnection connection = null;
                try {
                    long id = ids.incrementAndGet();
                    connection = new ClientConnection(id, socket, config, registry, this, permits);
                    connections.put(id, connection);
                    executor.execute(connection);
                } catch (IOException | RejectedExecutionException ex) {
                    if (connection != null) {
                        connection.cleanup();
                    } else {
                        permits.release();
                        socket.close();
                    }
                }
            } catch (IOException ex) {
                try {
                    socket.close();
                } catch (IOException closeEx) {
                    // This socket cannot be served.
                }
            }
        }
    }

    private static void refuse(Socket socket) {
        try {
            FrameCodec.write(socket.getOutputStream(), new Frame(
                    Command.ERROR,
                    FrameCodec.errorHeader(ErrorCode.BROKER_BUSY.name(), "broker is busy"),
                    new byte[0]));
            socket.shutdownOutput();
        } catch (IOException ex) {
            // The client may already have gone.
        } finally {
            try {
                socket.close();
            } catch (IOException ex) {
                // The refused socket is closed either way.
            }
        }
    }
}
