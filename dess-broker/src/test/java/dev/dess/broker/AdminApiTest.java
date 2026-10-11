package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.protocol.Command;
import dev.dess.common.json.Json;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class AdminApiTest {

    @Test
    void listsTopicsCreatedByClientTraffic() throws Exception {
        try (LogCapture logs = LogCapture.open();
                Broker broker = Broker.start(TestConfigs.of(0, 8, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 30_000));
                ProtocolClient producer = ProtocolClient.connect(broker.port());
                ProtocolClient consumer = ProtocolClient.connect(broker.port())) {
            producer.setReadTimeoutMillis(5_000);
            consumer.setReadTimeoutMillis(5_000);
            producer.hello();
            consumer.hello();
            consumer.subscribe("sales");
            logs.awaitAdded(consumer, "sales");
            producer.publish("sales", "hello".getBytes(StandardCharsets.UTF_8));
            assertEquals(Command.ACK, producer.readFrame().command());

            HttpResponse<String> response = send(broker.adminPort(), "GET", "/topics");
            assertEquals(200, response.statusCode());
            Map<String, Object> body = Json.object(response.body());
            @SuppressWarnings("unchecked")
            List<Object> topics = (List<Object>) body.get("topics");
            assertEquals(1, topics.size());
            @SuppressWarnings("unchecked")
            Map<String, Object> sales = (Map<String, Object>) topics.get(0);
            TopicSummary expected = broker.summaries().get(0);
            assertEquals("sales", sales.get("name"));
            assertEquals(expected.retainedCount(), ((Number) sales.get("retainedCount")).longValue());
            assertEquals(expected.subscriberCount(), ((Number) sales.get("subscriberCount")).intValue());
            assertEquals(expected.queueDepth(), ((Number) sales.get("queueDepth")).intValue());
            assertTrue(sales.containsKey("retainedCount"));
            assertTrue(sales.containsKey("subscriberCount"));
            assertTrue(sales.containsKey("queueDepth"));
        }
    }

    @Test
    void listsOneThousandTopicsInUnder500Ms() throws Exception {
        ArrayList<String> names = new ArrayList<>();
        for (int index = 0; index < 1_000; index++) {
            names.add("t" + String.format("%04d", index));
        }
        BrokerConfig config = TestConfigs.full(
                0,
                32,
                TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES,
                30_000,
                TestConfigs.DEFAULT_MAX_IN_FLIGHT,
                TestConfigs.DEFAULT_TOPIC_QUEUE_CAPACITY,
                TestConfigs.DEFAULT_OUTBOUND_CAPACITY,
                TestConfigs.DEFAULT_SLOW_CONSUMER_GRACE_MILLIS,
                TestConfigs.DEFAULT_PUBLISH_WAIT_MILLIS,
                TestConfigs.DEFAULT_ACK_TIMEOUT_MILLIS,
                true,
                names);
        try (LogCapture logs = LogCapture.open()) {
            logs.quiet();
            try (Broker broker = Broker.start(config)) {
                long started = System.nanoTime();
                HttpResponse<String> response = send(broker.adminPort(), "GET", "/topics");
                long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
                assertEquals(200, response.statusCode());
                assertTrue(elapsed < 500, "GET /topics took " + elapsed + " ms");
                @SuppressWarnings("unchecked")
                List<Object> topics = (List<Object>) Json.object(response.body()).get("topics");
                assertEquals(1_000, topics.size());
            }
        }
    }

    @Test
    void healthIsUpWhileAcceptingAndUnavailableOtherwise() throws Exception {
        try (Broker broker = Broker.start(TestConfigs.of(0, 8, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 30_000))) {
            HttpResponse<String> up = send(broker.adminPort(), "GET", "/health");
            assertEquals(200, up.statusCode());
            assertEquals("UP", Json.object(up.body()).get("status"));
            assertTrue(broker.adminAddress().getAddress().isLoopbackAddress());

            broker.health(BrokerHealth.STOPPING);
            for (String path : new String[] {"/health", "/topics"}) {
                HttpResponse<String> response = send(broker.adminPort(), "GET", path);
                assertEquals(503, response.statusCode(), path);
                assertEquals("UNAVAILABLE", Json.object(response.body()).get("status"));
            }
        }
        AtomicReference<BrokerHealth> health = new AtomicReference<>(BrokerHealth.STARTING);
        try (AdminServer server = AdminServer.open("127.0.0.1", 0, health::get)) {
            for (String path : new String[] {"/health", "/topics"}) {
                HttpResponse<String> response = send(server.port(), "GET", path);
                assertEquals(503, response.statusCode(), path);
                assertEquals("UNAVAILABLE", Json.object(response.body()).get("status"));
            }
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
            default -> throw new IllegalStateException("Unhandled method " + method);
        };
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
