package dev.dess.broker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.dess.common.json.Json;
import dev.dess.common.logging.Logging;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

final class AdminServer implements AutoCloseable {

    private static final int CORE_THREADS = 2;

    private static final int MAX_THREADS = 8;

    private static final int QUEUE_CAPACITY = 16;

    private final HttpServer server;

    private final ThreadPoolExecutor executor;

    private final Supplier<BrokerHealth> health;

    private AdminServer(HttpServer server, ThreadPoolExecutor executor, Supplier<BrokerHealth> health) {
        this.server = server;
        this.executor = executor;
        this.health = health;
    }

    static AdminServer open(String bindAddress, int port, Supplier<BrokerHealth> health) throws IOException {
        InetAddress address;
        try {
            address = InetAddress.getByName(bindAddress);
        } catch (IOException ex) {
            throw new IOException("Could not bind admin port " + port, ex);
        }
        HttpServer server;
        try {
            server = HttpServer.create(new InetSocketAddress(address, port), 0);
        } catch (IOException ex) {
            throw new IOException("Could not bind admin port " + port, ex);
        }
        AtomicInteger ids = new AtomicInteger();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                CORE_THREADS,
                MAX_THREADS,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(QUEUE_CAPACITY),
                runnable -> {
                    Thread thread = new Thread(runnable, "dess-admin-" + ids.incrementAndGet());
                    thread.setDaemon(false);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
        AdminServer admin = new AdminServer(server, executor, health);
        server.createContext("/", admin::handle);
        server.setExecutor(executor);
        server.start();
        return admin;
    }

    int port() {
        return server.getAddress().getPort();
    }

    InetSocketAddress address() {
        return server.getAddress();
    }

    private void handle(HttpExchange exchange) throws IOException {
        long started = System.nanoTime();
        int status = 500;
        try {
            status = route(exchange);
        } finally {
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put("method", exchange.getRequestMethod());
            properties.put("path", path(exchange));
            properties.put("status", Integer.toString(status));
            properties.put("durationMs", Long.toString(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)));
            Logging.event("dev.dess.broker", "admin.request", properties);
            exchange.close();
        }
    }

    private int route(HttpExchange exchange) throws IOException {
        BrokerHealth current = health.get();
        switch (current) {
            case UP -> {
            }
            case STARTING, STOPPING -> {
                return send(exchange, 503, statusBody("UNAVAILABLE"), null);
            }
            default -> {
                BrokerHealth unexpected = current;
                throw new IllegalStateException("Unhandled health " + unexpected);
            }
        }
        String method = exchange.getRequestMethod();
        String path = path(exchange);
        if ("/topics".equals(path)) {
            if ("GET".equals(method) || "POST".equals(method)) {
                return send(exchange, 404, error("NOT_FOUND", "Not found"), null);
            }
            return send(exchange, 405, error("METHOD_NOT_ALLOWED", "Method not allowed"), "GET, POST");
        }
        if (topicPath(path)) {
            if ("DELETE".equals(method)) {
                return send(exchange, 404, error("NOT_FOUND", "Not found"), null);
            }
            return send(exchange, 405, error("METHOD_NOT_ALLOWED", "Method not allowed"), "DELETE");
        }
        if ("/metrics".equals(path) || "/health".equals(path)) {
            if ("GET".equals(method)) {
                return send(exchange, 404, error("NOT_FOUND", "Not found"), null);
            }
            return send(exchange, 405, error("METHOD_NOT_ALLOWED", "Method not allowed"), "GET");
        }
        return send(exchange, 404, error("NOT_FOUND", "Not found"), null);
    }

    private static boolean topicPath(String path) {
        if (!path.startsWith("/topics/") || path.length() == "/topics/".length()) {
            return false;
        }
        String name = path.substring("/topics/".length());
        return !name.isEmpty() && name.indexOf('/') < 0;
    }

    private static String path(HttpExchange exchange) {
        String path = exchange.getRequestURI().getPath();
        if (path == null || path.isEmpty()) {
            return "/";
        }
        return path;
    }

    private static Map<String, Object> statusBody(String status) {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("status", status);
        return body;
    }

    private static Map<String, Object> error(String token, String message) {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("error", token);
        body.put("message", message);
        return body;
    }

    private static int send(HttpExchange exchange, int status, Map<String, Object> body, String allow) throws IOException {
        byte[] bytes = Json.encode(body).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        if (allow != null) {
            exchange.getResponseHeaders().set("Allow", allow);
        }
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
        return status;
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
