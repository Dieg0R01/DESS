package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.json.Json;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class AdminServerTest {

    @Test
    void bindsLoopbackAndIsReachable() throws Exception {
        try (Broker broker = Broker.start(TestConfigs.of(0, 8, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 30_000))) {
            assertTrue(broker.adminAddress().getAddress().isLoopbackAddress());
            HttpResponse<String> response = send(broker.adminPort(), "GET", "/nope");
            assertEquals(404, response.statusCode());
        }
    }

    @Test
    void secondBrokerOnTheSameAdminPortFailsNamingThePort() throws Exception {
        try (Broker first = Broker.start(TestConfigs.of(0, 8, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 30_000))) {
            int port = first.adminPort();
            IOException ex = assertThrows(IOException.class, () -> Broker.start(TestConfigs.withAdminPort(port)));
            assertTrue(ex.getMessage().contains("Could not bind admin port"), ex.getMessage());
            assertTrue(ex.getMessage().contains(Integer.toString(port)), ex.getMessage());
        }
    }

    @Test
    void startingAndStoppingReturnUnavailable() throws Exception {
        AtomicReference<BrokerHealth> health = new AtomicReference<>(BrokerHealth.STARTING);
        try (AdminServer server = AdminServer.open("127.0.0.1", 0, health::get);
                LogCapture logs = LogCapture.open()) {
            for (BrokerHealth state : new BrokerHealth[] {BrokerHealth.STARTING, BrokerHealth.STOPPING}) {
                health.set(state);
                for (String path : new String[] {"/health", "/topics", "/metrics"}) {
                    HttpResponse<String> response = send(server.port(), "GET", path);
                    assertEquals(503, response.statusCode(), path);
                    Map<String, Object> body = Json.object(response.body());
                    assertEquals("UNAVAILABLE", body.get("status"));
                    assertTrue(logs.await(Duration.ofSeconds(2), "event=admin.request", "method=GET",
                            "path=" + path, "status=503", "durationMs="), logs.lines()::toString);
                }
            }
        }
        try (Broker broker = Broker.start(TestConfigs.of(0, 8, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 30_000))) {
            broker.health(BrokerHealth.STOPPING);
            HttpResponse<String> response = send(broker.adminPort(), "GET", "/topics");
            assertEquals(503, response.statusCode());
            assertEquals("UNAVAILABLE", Json.object(response.body()).get("status"));
        }
    }

    @Test
    void wrongMethodIs405AndUnknownPathIs404() throws Exception {
        try (LogCapture logs = LogCapture.open();
                Broker broker = Broker.start(TestConfigs.of(0, 8, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 30_000))) {
            HttpResponse<String> put = send(broker.adminPort(), "PUT", "/topics");
            assertEquals(405, put.statusCode());
            assertTrue(put.headers().firstValue("Allow").orElse("").contains("GET"), put.headers().toString());
            assertTrue(put.headers().firstValue("Allow").orElse("").contains("POST"), put.headers().toString());
            assertEquals("METHOD_NOT_ALLOWED", Json.object(put.body()).get("error"));

            HttpResponse<String> missing = send(broker.adminPort(), "GET", "/nope");
            assertEquals(404, missing.statusCode());
            assertEquals("NOT_FOUND", Json.object(missing.body()).get("error"));

            assertTrue(logs.await(Duration.ofSeconds(2), "event=admin.request", "method=PUT",
                    "path=/topics", "status=405", "durationMs="), logs.lines()::toString);
            assertTrue(logs.await(Duration.ofSeconds(2), "event=admin.request", "method=GET",
                    "path=/nope", "status=404", "durationMs="), logs.lines()::toString);
        }
    }

    private static HttpResponse<String> send(int port, String method, String path) throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2))
                .build();
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(2));
        HttpRequest request = switch (method) {
            case "GET" -> builder.GET().build();
            case "PUT" -> builder.PUT(HttpRequest.BodyPublishers.noBody()).build();
            case "POST" -> builder.POST(HttpRequest.BodyPublishers.noBody()).build();
            case "DELETE" -> builder.DELETE().build();
            default -> throw new IllegalStateException("Unhandled method " + method);
        };
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
