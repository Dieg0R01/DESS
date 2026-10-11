package dev.dess.broker;

import dev.dess.common.logging.Logging;
import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.ErrorCode;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class Broker implements AutoCloseable {

    private final BrokerConfig config;
    private final ServerSocket serverSocket;
    private final Thread acceptThread;
    private final ExecutorService readers;
    private final ExecutorService writers;
    private final ScheduledExecutorService flowTimer;
    private final FlowControl flow;
    private final TopicRegistry registry;
    private final ConcurrentHashMap<Long, ClientConnection> connections = new ConcurrentHashMap<>();
    private final Semaphore permits;
    private final long startedAt;
    private final AtomicLong ids = new AtomicLong();
    private volatile boolean running = true;
    private volatile boolean shuttingDown;

    private Broker(BrokerConfig config) throws IOException {
        this.config = config;
        this.flowTimer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "dess-flow-timer");
            thread.setDaemon(false);
            return thread;
        });
        this.flow = new FlowControl(config.maxInFlight());
        this.registry = new TopicRegistry(config.topicQueueCapacity(), config.outboundCapacity(), flow, config.autoCreateTopics(), this::scheduleLagCheck);
        for (String name : config.topics()) {
            registry.declare(name);
        }
        this.permits = new Semaphore(config.maxConnections());
        this.serverSocket = bind(config.port());
        this.startedAt = System.nanoTime();
        this.readers = newPool(config.maxConnections(), config.threadKeepAliveMillis());
        this.writers = newPool(config.maxConnections(), config.threadKeepAliveMillis());
        this.acceptThread = new Thread(this::acceptLoop, "dess-accept-" + serverSocket.getLocalPort());
        this.acceptThread.setDaemon(false);
        Map<String, String> started = new LinkedHashMap<>();
        started.put("version", Integer.toString(FrameCodec.PROTOCOL_VERSION));
        started.put("tcpPort", Integer.toString(serverSocket.getLocalPort()));
        Logging.event("dev.dess.broker", "broker.started", started);
        this.acceptThread.start();
    }

    private static ServerSocket bind(int port) throws IOException {
        try {
            return new ServerSocket(port);
        } catch (IOException ex) {
            throw new IOException("Could not bind port " + port, ex);
        }
    }

    public static Broker start(BrokerConfig config) throws IOException {
        return new Broker(config);
    }

    public int inFlightPeak() {
        return flow.inFlightPeak();
    }

    public int inFlight() {
        return flow.inFlight();
    }

    FlowControl flow() {
        return flow;
    }

    boolean hasTopic(String name) {
        return registry.hasTopic(name);
    }

    Set<Long> subscribers(String name) {
        return registry.subscribers(name);
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

    void scheduleLagCheck(Subscription subscription) {
        schedule(config.slowConsumerGraceMillis(), () -> checkLag(subscription));
    }

    void watchWrite(ClientConnection connection) {
        schedule(config.slowConsumerGraceMillis(), () -> checkWrite(connection));
    }

    private void checkLag(Subscription subscription) {
        if (shuttingDown) {
            return;
        }
        long graceNanos = TimeUnit.MILLISECONDS.toNanos(config.slowConsumerGraceMillis());
        if (subscription.topic().graceElapsed(subscription, graceNanos)) {
            subscription.connection().markSlow();
        }
    }

    private void checkWrite(ClientConnection connection) {
        if (shuttingDown || connection.isClosing()) {
            connection.clearWriteWatch();
            return;
        }
        long since = connection.writingSince();
        if (since == 0L) {
            connection.clearWriteWatch();
            if (connection.writingSince() != 0L && connection.armWriteWatch()) {
                watchWrite(connection);
            }
            return;
        }
        long graceMillis = config.slowConsumerGraceMillis();
        long ageMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - since);
        boolean full = connection.outboundFull();
        if (ageMillis >= graceMillis && !full) {
            connection.clearWriteWatch();
            connection.forceCloseSlow();
            return;
        }
        if (ageMillis >= graceMillis) {
            connection.markSlow();
        }
        if (ageMillis >= graceMillis * 2L) {
            connection.clearWriteWatch();
            connection.forceCloseSlow();
            return;
        }
        long remaining = Math.max(1L, (full ? graceMillis * 2L : graceMillis) - ageMillis);
        schedule(remaining, () -> checkWrite(connection));
    }

    private void schedule(long delayMillis, Runnable task) {
        try {
            flowTimer.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ex) {
            // The timer is already shut down.
        }
    }

    void startWriter(ClientConnection connection) {
        try {
            writers.execute(connection.writer());
        } catch (RejectedExecutionException ex) {
            connection.cleanup();
        }
    }

    @Override
    public void close() {
        close("CLOSE");
    }

    public void close(String reason) {
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
        int inFlightLost = flow.inFlight();
        for (ClientConnection connection : List.copyOf(connections.values())) {
            connection.cleanup();
        }
        readers.shutdownNow();
        writers.shutdownNow();
        flowTimer.shutdownNow();
        try {
            long shutdownMillis = config.shutdownTimeoutMillis();
            if (!readers.awaitTermination(shutdownMillis, TimeUnit.MILLISECONDS)
                    || !writers.awaitTermination(shutdownMillis, TimeUnit.MILLISECONDS)
                    || !flowTimer.awaitTermination(shutdownMillis, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Connection threads did not stop");
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping the broker", ex);
        }
        long uptimeSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startedAt);
        Map<String, String> stopped = new LinkedHashMap<>();
        stopped.put("uptimeSeconds", Long.toString(uptimeSeconds));
        stopped.put("reason", reason);
        stopped.put("inFlightLost", Integer.toString(inFlightLost));
        Logging.event("dev.dess.broker", "broker.stopped", stopped);
        if ("SIGNAL".equals(reason)) {
            // Signal shutdown closes the log handlers, so this line is written directly.
            System.out.println(Logging.format("INFO", "dev.dess.broker", "event=broker.stopped uptimeSeconds=" + uptimeSeconds + " reason=" + reason + " inFlightLost=" + inFlightLost));
            System.out.flush();
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
                    readers.execute(connection);
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

    private static ExecutorService newPool(int threads, int keepAliveMillis) {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                threads,
                threads,
                keepAliveMillis,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setDaemon(false);
                    return thread;
                });
        pool.allowCoreThreadTimeOut(true);
        return pool;
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
