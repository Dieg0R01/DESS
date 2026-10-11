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
    private final TlsMaterial tls;
    private final ServerSocket serverSocket;
    private final DiscoveryResponder discovery;
    private final Thread acceptThread;
    private final ExecutorService readers;
    private final ExecutorService writers;
    private final ExecutorService refuse;
    private final ScheduledExecutorService flowTimer;
    private final FlowControl flow;
    private final TopicRegistry registry;
    private final LogDirectory directory;
    private final TopicLogs logs;
    private final Object syncMonitor = new Object();
    private volatile boolean syncStop;
    private Thread logSync;
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
        try {
            TlsMaterial.Result loaded = TlsMaterial.load(config);
            if (!loaded.problems().isEmpty()) {
                throw new TlsLoadException(loaded.problems());
            }
            this.tls = loaded.material();
            this.directory = LogDirectory.acquire(config.logDirectory());
            try {
                TopicLogs.deleteTemporary(config.logDirectory());
                this.logs = TopicLogs.open(config.logDirectory(), config.durability());
            } catch (IOException ex) {
                closeDirectory(directory, ex);
                throw ex;
            }
            ConfigWriter writer = new ConfigWriter(config);
            this.registry = new TopicRegistry(config.topicQueueCapacity(), config.outboundCapacity(), flow, config.autoCreateTopics(), this::scheduleLagCheck, logs, writer);
            try {
                for (String name : List.copyOf(config.topics())) {
                    registry.declare(name);
                }
                writer.includeRecovered(registry.names());
                this.permits = new Semaphore(config.maxConnections());
                ServerSocket bound = bind(config.port());
                DiscoveryResponder responder;
                try {
                    responder = openDiscovery(bound.getLocalPort());
                } catch (IOException ex) {
                    try {
                        bound.close();
                    } catch (IOException suppressed) {
                        ex.addSuppressed(suppressed);
                    }
                    throw ex;
                }
                this.serverSocket = bound;
                this.discovery = responder;
            } catch (IOException ex) {
                closeLogs(logs, ex);
                closeDirectory(directory, ex);
                throw ex;
            }
        } catch (IOException ex) {
            flowTimer.shutdownNow();
            throw ex;
        }
        this.startedAt = System.nanoTime();
        this.readers = newPool(config.maxConnections(), config.threadKeepAliveMillis());
        this.writers = newPool(config.maxConnections(), config.threadKeepAliveMillis());
        this.refuse = refusePool(config.maxConnections(), config.threadKeepAliveMillis());
        this.acceptThread = new Thread(this::acceptLoop, "dess-accept-" + serverSocket.getLocalPort());
        this.acceptThread.setDaemon(false);
        switch (config.durability()) {
            case INTERVAL -> {
                this.logSync = new Thread(this::syncLogs, "dess-log-sync");
                this.logSync.setDaemon(false);
                this.logSync.start();
            }
            case ALWAYS -> {
            }
            default -> {
                Durability unexpected = config.durability();
                throw new IllegalStateException("Unhandled durability " + unexpected);
            }
        }
        Map<String, String> recovered = new LinkedHashMap<>();
        recovered.put("files", Integer.toString(logs.files()));
        recovered.put("records", Long.toString(logs.records()));
        recovered.put("truncatedRecords", Integer.toString(logs.truncatedRecords()));
        recovered.put("durationMs", Long.toString(logs.durationMs()));
        Logging.event("dev.dess.broker", "log.recovered", recovered);
        logEffective();
        Map<String, String> started = new LinkedHashMap<>();
        started.put("version", Integer.toString(FrameCodec.PROTOCOL_VERSION));
        started.put("tls", "true");
        started.put("tcpPort", Integer.toString(serverSocket.getLocalPort()));
        if (config.configPath() != null) {
            started.put("configPath", config.configPath().toAbsolutePath().toString());
        }
        Logging.event("dev.dess.broker", "broker.started", started);
        this.acceptThread.start();
    }

    private static void closeLogs(TopicLogs logs, IOException failure) {
        try {
            logs.close();
        } catch (IOException ex) {
            failure.addSuppressed(ex);
        }
    }

    private static void closeDirectory(LogDirectory directory, IOException failure) {
        try {
            directory.close();
        } catch (IOException ex) {
            failure.addSuppressed(ex);
        }
    }

    private void logEffective() {
        Map<String, String> effective = new LinkedHashMap<>();
        effective.put("port", Integer.toString(config.port()));
        effective.put("maxConnections", Integer.toString(config.maxConnections()));
        effective.put("maxPayloadBytes", Integer.toString(config.maxPayloadBytes()));
        effective.put("idleTimeoutMillis", Integer.toString(config.idleTimeoutMillis()));
        effective.put("maxInFlight", Integer.toString(config.maxInFlight()));
        effective.put("topicQueueCapacity", Integer.toString(config.topicQueueCapacity()));
        effective.put("outboundCapacity", Integer.toString(config.outboundCapacity()));
        effective.put("slowConsumerGraceMillis", Integer.toString(config.slowConsumerGraceMillis()));
        effective.put("publishWaitMillis", Integer.toString(config.publishWaitMillis()));
        effective.put("ackTimeoutMillis", Integer.toString(config.ackTimeoutMillis()));
        effective.put("threadKeepAliveMillis", Integer.toString(config.threadKeepAliveMillis()));
        effective.put("shutdownTimeoutMillis", Integer.toString(config.shutdownTimeoutMillis()));
        effective.put("autoCreateTopics", Boolean.toString(config.autoCreateTopics()));
        effective.put("logDirectory", config.logDirectory().toAbsolutePath().toString());
        effective.put("durability", config.durability().name());
        effective.put("syncIntervalMillis", Integer.toString(config.syncIntervalMillis()));
        effective.put("logLevel", config.logLevel());
        effective.put("keyStore", config.keyStore().toAbsolutePath().toString());
        effective.put("trustStore", config.trustStore().toAbsolutePath().toString());
        effective.put("passwordFile", config.passwordFile().toAbsolutePath().toString());
        effective.put("discoveryEnabled", Boolean.toString(config.discoveryEnabled()));
        effective.put("discoveryPort", Integer.toString(config.discoveryPort()));
        effective.put("presenceEnabled", Boolean.toString(config.presenceEnabled()));
        effective.put("topics", String.join(",", config.topics()));
        Logging.event("dev.dess.broker", "config.effective", effective);
    }

    private DiscoveryResponder openDiscovery(int tcpPort) throws IOException {
        if (!config.discoveryEnabled() && !config.presenceEnabled()) {
            return null;
        }
        return DiscoveryResponder.open(
                config.discoveryPort(), tcpPort, config.discoveryEnabled(), config.idleTimeoutMillis());
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

    public int discoveryPort() {
        if (!running || discovery == null) {
            throw new IllegalStateException("Discovery is not running");
        }
        return discovery.port();
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
            acceptThread.join(config.shutdownTimeoutMillis());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping the accept thread", ex);
        }
        if (acceptThread.isAlive()) {
            throw new IllegalStateException("Accept thread " + acceptThread.getName() + " did not stop");
        }
        if (discovery != null) {
            discovery.close(config.shutdownTimeoutMillis());
        }
        int inFlightLost = flow.inFlight();
        stopSync();
        IOException storageFailure = null;
        try {
            logs.forceAll();
            logs.close();
        } catch (IOException ex) {
            storageFailure = ex;
        }
        try {
            directory.close();
        } catch (IOException ex) {
            if (storageFailure == null) {
                storageFailure = ex;
            } else {
                storageFailure.addSuppressed(ex);
            }
        }
        if (storageFailure != null) {
            throw new IllegalStateException("Could not force the topic logs", storageFailure);
        }
        for (ClientConnection connection : List.copyOf(connections.values())) {
            connection.cleanup();
        }
        readers.shutdownNow();
        writers.shutdownNow();
        refuse.shutdownNow();
        flowTimer.shutdownNow();
        try {
            long shutdownMillis = config.shutdownTimeoutMillis();
            if (!readers.awaitTermination(shutdownMillis, TimeUnit.MILLISECONDS)
                    || !writers.awaitTermination(shutdownMillis, TimeUnit.MILLISECONDS)
                    || !refuse.awaitTermination(shutdownMillis, TimeUnit.MILLISECONDS)
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
                    connection = new ClientConnection(id, socket, config, registry, this, permits, tls);
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

    private static ExecutorService refusePool(int queue, int keepAliveMillis) {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                1,
                1,
                keepAliveMillis,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(queue),
                runnable -> {
                    Thread thread = new Thread(runnable, "dess-refuse");
                    thread.setDaemon(false);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy());
        pool.allowCoreThreadTimeOut(true);
        return pool;
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

    private void syncLogs() {
        while (!syncStop) {
            synchronized (syncMonitor) {
                if (syncStop) {
                    return;
                }
                try {
                    syncMonitor.wait(config.syncIntervalMillis());
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (syncStop) {
                    return;
                }
            }
            try {
                long started = System.nanoTime();
                int files = logs.forceAll();
                long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
                Logging.log("DEBUG", "dev.dess.broker", "event=log.synced files=" + files + " durationMs=" + durationMs);
            } catch (IOException ex) {
                Logging.log("ERROR", "dev.dess.broker", "log sync failed: " + ex.getMessage());
            }
        }
    }

    private void stopSync() {
        Thread sync = logSync;
        if (sync == null) {
            return;
        }
        synchronized (syncMonitor) {
            syncStop = true;
            syncMonitor.notifyAll();
        }
        try {
            sync.join(config.shutdownTimeoutMillis());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping the log sync", ex);
        }
        if (sync.isAlive()) {
            throw new IllegalStateException("Log sync thread did not stop");
        }
    }

    private void refuse(Socket socket) {
        try {
            refuse.execute(() -> refuseOne(socket));
        } catch (RejectedExecutionException ex) {
            closeQuietly(socket);
        }
    }

    private void refuseOne(Socket socket) {
        TlsHandshake.Outcome outcome = TlsHandshake.open(socket, tls, config.idleTimeoutMillis());
        if (outcome.failed()) {
            logHandshakeFailed(remoteAddress(socket), outcome.reason(), outcome.detail());
            closeQuietly(socket);
            closeQuietly(outcome.socket());
            return;
        }
        try {
            FrameCodec.write(outcome.socket().getOutputStream(), new Frame(
                    Command.ERROR,
                    FrameCodec.errorHeader(ErrorCode.BROKER_BUSY.name(), "broker is busy"),
                    new byte[0]));
            outcome.socket().shutdownOutput();
        } catch (IOException ex) {
            // The client may already have gone.
        } finally {
            closeQuietly(socket);
            closeQuietly(outcome.socket());
        }
    }

    static void logHandshakeFailed(String remoteAddress, HandshakeReason reason, String detail) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("remoteAddress", remoteAddress);
        properties.put("reason", reason.name());
        if (reason == HandshakeReason.FAILED && detail != null) {
            properties.put("detail", detail);
        }
        Logging.event("dev.dess.broker", "tls.handshake.failed", properties);
    }

    static String remoteAddress(Socket socket) {
        return socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
    }

    static void closeQuietly(Socket socket) {
        if (socket == null) {
            return;
        }
        try {
            socket.setSoTimeout(1);
        } catch (IOException ex) {
            // Closing still proceeds.
        }
        try {
            socket.close();
        } catch (IOException ex) {
            // The refused socket is closed either way.
        }
    }
}
