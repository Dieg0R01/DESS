package dev.dess.broker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.dess.common.json.Json;
import dev.dess.common.logging.Logging;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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

    private static final int POST_MAX_BYTES = 4096;

    private static final String TOPIC_NAME_RULE = "[a-z0-9][a-z0-9._-]{0,63}";

    private final HttpServer server;

    private final ThreadPoolExecutor executor;

    private final Supplier<BrokerHealth> health;

    private final Supplier<List<TopicSummary>> topics;

    private final Supplier<Map<String, Object>> metrics;

    private final TopicRegistry registry;

    private AdminServer(HttpServer server, ThreadPoolExecutor executor, Supplier<BrokerHealth> health, Supplier<List<TopicSummary>> topics, Supplier<Map<String, Object>> metrics, TopicRegistry registry) {
        this.server = server;
        this.executor = executor;
        this.health = health;
        this.topics = topics;
        this.metrics = metrics;
        this.registry = registry;
    }

    static AdminServer open(String bindAddress, int port, Supplier<BrokerHealth> health) throws IOException {
        return open(bindAddress, port, health, List::of);
    }

    static AdminServer open(String bindAddress, int port, Supplier<BrokerHealth> health, Supplier<List<TopicSummary>> topics) throws IOException {
        return open(bindAddress, port, health, topics, Map::of);
    }

    static AdminServer open(String bindAddress, int port, Supplier<BrokerHealth> health, Supplier<List<TopicSummary>> topics, Supplier<Map<String, Object>> metrics) throws IOException {
        return open(bindAddress, port, health, topics, metrics, null);
    }

    static AdminServer open(String bindAddress, int port, Supplier<BrokerHealth> health, Supplier<List<TopicSummary>> topics, Supplier<Map<String, Object>> metrics, TopicRegistry registry) throws IOException {
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
        AdminServer admin = new AdminServer(server, executor, health, topics, metrics, registry);
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
            if ("GET".equals(method)) {
                return send(exchange, 200, topicsBody(), null);
            }
            if ("POST".equals(method)) {
                return createTopic(exchange);
            }
            return send(exchange, 405, error("METHOD_NOT_ALLOWED", "Method not allowed"), "GET, POST");
        }
        if (topicPath(path)) {
            if ("DELETE".equals(method)) {
                return deleteTopic(exchange, path.substring("/topics/".length()));
            }
            return send(exchange, 405, error("METHOD_NOT_ALLOWED", "Method not allowed"), "DELETE");
        }
        if ("/health".equals(path)) {
            if ("GET".equals(method)) {
                return send(exchange, 200, statusBody("UP"), null);
            }
            return send(exchange, 405, error("METHOD_NOT_ALLOWED", "Method not allowed"), "GET");
        }
        if ("/metrics".equals(path)) {
            if ("GET".equals(method)) {
                return send(exchange, 200, metrics.get(), null);
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

    private int createTopic(HttpExchange exchange) throws IOException {
        if (registry == null) {
            return send(exchange, 404, error("NOT_FOUND", "Not found"), null);
        }
        byte[] raw = readBody(exchange, POST_MAX_BYTES);
        if (raw == null) {
            return send(exchange, 400, error("MALFORMED_BODY", "Request body must be at most " + POST_MAX_BYTES + " bytes"), null);
        }
        String text = new String(raw, StandardCharsets.UTF_8);
        Map<String, Object> body;
        try {
            body = Json.object(text);
        } catch (IllegalArgumentException ex) {
            return send(exchange, 400, error("MALFORMED_BODY", ex.getMessage()), null);
        }
        Object value = body.get("name");
        if (!(value instanceof String name) || name.isEmpty()) {
            return send(exchange, 400, error("MALFORMED_BODY", "name is required"), null);
        }
        TopicRegistry.Created created = registry.create(name);
        switch (created.status()) {
            case CREATED -> {
                exchange.getResponseHeaders().set("Location", "/topics/" + name);
                return send(exchange, 201, created.summary().toJson(), null);
            }
            case EXISTS -> {
                return send(exchange, 409, error("TOPIC_EXISTS", "Topic already exists"), null);
            }
            case INVALID -> {
                return send(exchange, 400, error("INVALID_TOPIC_NAME", "Topic names match " + TOPIC_NAME_RULE), null);
            }
            default -> {
                TopicRegistry.CreateStatus unexpected = created.status();
                throw new IllegalStateException("Unhandled create status " + unexpected);
            }
        }
    }

    private int deleteTopic(HttpExchange exchange, String name) throws IOException {
        if (registry == null) {
            return send(exchange, 404, error("NOT_FOUND", "Not found"), null);
        }
        TopicRegistry.DeleteStatus status = registry.delete(name);
        switch (status) {
            case DELETED -> {
                exchange.sendResponseHeaders(204, -1);
                return 204;
            }
            case NOT_FOUND -> {
                return send(exchange, 404, error("UNKNOWN_TOPIC", "Unknown topic"), null);
            }
            case INVALID -> {
                return send(exchange, 400, error("INVALID_TOPIC_NAME", "Topic names match " + TOPIC_NAME_RULE), null);
            }
            default -> {
                TopicRegistry.DeleteStatus unexpected = status;
                throw new IllegalStateException("Unhandled delete status " + unexpected);
            }
        }
    }

    private static byte[] readBody(HttpExchange exchange, int maxBytes) throws IOException {
        try (InputStream input = exchange.getRequestBody()) {
            byte[] bytes = input.readNBytes(maxBytes + 1);
            if (bytes.length > maxBytes) {
                input.transferTo(OutputStream.nullOutputStream());
                return null;
            }
            return bytes;
        }
    }

    private Map<String, Object> topicsBody() {
        ArrayList<Map<String, Object>> items = new ArrayList<>();
        for (TopicSummary summary : topics.get()) {
            items.add(summary.toJson());
        }
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("topics", items);
        return body;
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
