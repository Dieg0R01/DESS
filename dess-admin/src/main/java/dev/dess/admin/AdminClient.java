package dev.dess.admin;

import dev.dess.common.json.Json;
import dev.dess.common.logging.Logging;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

final class AdminClient {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);

    private AdminClient() {
    }

    static int execute(AdminRequest request, PrintStream out, PrintStream err) {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        HttpRequest http;
        try {
            http = httpRequest(request);
        } catch (IllegalArgumentException ex) {
            return fail(err, "UNREACHABLE");
        }
        HttpResponse<String> response;
        try {
            response = client.send(http, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException ex) {
            return fail(err, "UNREACHABLE");
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return fail(err, "UNREACHABLE");
        }
        int status = response.statusCode();
        String body = response.body() == null ? "" : response.body();
        if (status < 200 || status >= 300) {
            return httpError(err, status, body);
        }
        String table;
        try {
            table = AdminFormat.render(request.verb(), body);
        } catch (RuntimeException ex) {
            return fail(err, Integer.toString(status));
        }
        if (request.json()) {
            out.print(body);
        } else {
            out.print(table);
        }
        return 0;
    }

    private static HttpRequest httpRequest(AdminRequest request) {
        return switch (request.verb()) {
            case LIST -> get(request, "/topics");
            case CREATE -> post(request, "/topics", createBody(request.name()));
            case DELETE -> delete(request, "/topics/" + request.name());
            case METRICS -> get(request, "/metrics");
            case HEALTH -> get(request, "/health");
            default -> {
                AdminRequest.Verb unexpected = request.verb();
                throw new IllegalStateException("Unhandled verb " + unexpected);
            }
        };
    }

    private static HttpRequest get(AdminRequest request, String path) {
        return HttpRequest.newBuilder(uri(request, path)).GET().build();
    }

    private static HttpRequest post(AdminRequest request, String path, String body) {
        return HttpRequest.newBuilder(uri(request, path))
                .header("Content-Type", "application/json; charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
    }

    private static HttpRequest delete(AdminRequest request, String path) {
        return HttpRequest.newBuilder(uri(request, path)).DELETE().build();
    }

    private static URI uri(AdminRequest request, String path) {
        String origin = request.url();
        while (origin.endsWith("/")) {
            origin = origin.substring(0, origin.length() - 1);
        }
        return URI.create(origin + path);
    }

    private static String createBody(String name) {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        return Json.encode(body);
    }

    private static int httpError(PrintStream err, int status, String body) {
        String message = errorMessage(body);
        if (message == null) {
            return fail(err, Integer.toString(status));
        }
        return fail(err, status + " " + message);
    }

    private static String errorMessage(String body) {
        if (body == null || body.isBlank()) {
            return null;
        }
        try {
            Map<String, Object> object = Json.object(body);
            Object message = object.get("message");
            if (message instanceof String text && !text.isBlank()) {
                return text;
            }
            return null;
        } catch (RuntimeException ex) {
            return null;
        }
    }

    private static int fail(PrintStream err, String message) {
        err.println(Logging.format("ERROR", "dev.dess.admin", message));
        return 1;
    }
}
