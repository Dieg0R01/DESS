package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.json.Json;
import dev.dess.common.protocol.Command;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class MetricsTest {

    private static final byte[] PAYLOAD = "x".getBytes(StandardCharsets.UTF_8);

    @Test
    void metricsSnapshotHasContractFields() throws Exception {
        try (LogCapture logs = LogCapture.open();
                Broker broker = Broker.start(TestConfigs.of(0, 8, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 30_000));
                ProtocolClient producer = ProtocolClient.connect(broker.port())) {
            producer.setReadTimeoutMillis(5_000);
            producer.hello();
            producer.publish("sales", PAYLOAD);
            assertEquals(Command.ACK, producer.readFrame().command());

            HttpResponse<String> response = send(broker.adminPort(), "GET", "/metrics");
            assertEquals(200, response.statusCode());
            Map<String, Object> body = Json.object(response.body());
            Instant.parse((String) body.get("takenAt"));
            assertInstanceOf(Number.class, body.get("messagesInPerSecond"));
            assertInstanceOf(Number.class, body.get("messagesOutPerSecond"));
            assertInstanceOf(Number.class, body.get("activeConnections"));
            assertInstanceOf(Number.class, body.get("activeThreads"));
            assertInstanceOf(Number.class, body.get("heapUsedBytes"));
            Object heapMax = body.get("heapMaxBytes");
            if (heapMax != null) {
                assertInstanceOf(Number.class, heapMax);
            }
            assertInstanceOf(Number.class, body.get("inFlight"));
            assertInstanceOf(Number.class, body.get("uptimeSeconds"));
            assertTrue(((Number) body.get("uptimeSeconds")).longValue() >= 0);
            assertTrue(((Number) body.get("activeThreads")).intValue() > 0);
            @SuppressWarnings("unchecked")
            List<Object> topics = (List<Object>) body.get("topics");
            assertNotNull(topics);
            assertEquals(1, topics.size());
            @SuppressWarnings("unchecked")
            Map<String, Object> sales = (Map<String, Object>) topics.get(0);
            TopicSummary expected = broker.summaries().get(0);
            assertEquals("sales", sales.get("name"));
            assertEquals(expected.queueDepth(), ((Number) sales.get("queueDepth")).intValue());
        }
    }

    @Test
    void steadyThousandPublishesPerSecondAndMetricsReadDoesNotStall() throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ScheduledExecutorService pace = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "metrics-pace");
            thread.setDaemon(true);
            return thread;
        });
        Semaphore permits = new Semaphore(0);
        try (LogCapture logs = LogCapture.open();
                Broker broker = Broker.start(TestConfigs.of(0, 8, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 30_000));
                ProtocolClient load = ProtocolClient.connect(broker.port());
                ProtocolClient probe = ProtocolClient.connect(broker.port())) {
            logs.quiet();
            load.setReadTimeoutMillis(5_000);
            probe.setReadTimeoutMillis(TestConfigs.DEFAULT_PUBLISH_WAIT_MILLIS);
            load.hello();
            probe.hello();

            Thread producer = new Thread(() -> publishAtRate(load, permits, running, failure), "metrics-load");
            producer.start();
            try {
                int mark = broker.metrics().ticks();
                pace.scheduleAtFixedRate(() -> permits.release(1_000), 0, 1, TimeUnit.SECONDS);
                assertTrue(broker.metrics().awaitTicks(mark + 11, Duration.ofSeconds(20)), "metrics window did not fill");
                assertNull(failure.get(), () -> String.valueOf(failure.get()));

                HttpResponse<String> during = send(broker.adminPort(), "GET", "/metrics");
                assertEquals(200, during.statusCode());
                probe.publish("sales", PAYLOAD);
                assertEquals(Command.ACK, probe.readFrame().command());

                Map<String, Object> body = Json.object(during.body());
                double inbound = ((Number) body.get("messagesInPerSecond")).doubleValue();
                assertTrue(inbound >= 900.0 && inbound <= 1_100.0, "messagesInPerSecond=" + inbound);
            } finally {
                running.set(false);
                permits.release(1_000);
                producer.join(5_000);
                pace.shutdownNow();
            }
            assertNull(failure.get(), () -> String.valueOf(failure.get()));
        } finally {
            running.set(false);
            pace.shutdownNow();
        }
    }

    private static void publishAtRate(ProtocolClient load, Semaphore permits, AtomicBoolean running, AtomicReference<Throwable> failure) {
        try {
            while (running.get()) {
                permits.acquire();
                if (!running.get()) {
                    return;
                }
                load.publish("sales", PAYLOAD);
                assertEquals(Command.ACK, load.readFrame().command());
            }
        } catch (Throwable ex) {
            failure.compareAndSet(null, ex);
            running.set(false);
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
