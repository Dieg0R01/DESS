package dev.dess.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dess.common.json.Json;
import dev.dess.common.protocol.Command;
import dev.dess.common.protocol.Frame;
import dev.dess.common.protocol.FrameCodec;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("dess-accept-threads")
class TopicAdminTest {

    private static final byte[] PAYLOAD = "hello".getBytes(StandardCharsets.UTF_8);

    @Test
    void postCreatesTopicFromAdmin() throws Exception {
        try (LogCapture logs = LogCapture.open();
                Broker broker = Broker.start(TestConfigs.of(0, 8, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 30_000))) {
            HttpResponse<String> created = send(broker.adminPort(), "POST", "/topics", "{\"name\":\"ops\"}");
            assertEquals(201, created.statusCode());
            assertEquals("/topics/ops", created.headers().firstValue("Location").orElse(""));
            Map<String, Object> body = Json.object(created.body());
            assertEquals("ops", body.get("name"));
            assertEquals(0L, ((Number) body.get("retainedCount")).longValue());
            assertEquals(0, ((Number) body.get("subscriberCount")).intValue());
            assertEquals(0, ((Number) body.get("queueDepth")).intValue());
            assertTrue(logs.await(Duration.ofSeconds(2), "event=topic.created", "topic=ops", "source=ADMIN"),
                    logs.lines()::toString);

            HttpResponse<String> duplicate = send(broker.adminPort(), "POST", "/topics", "{\"name\":\"ops\"}");
            assertEquals(409, duplicate.statusCode());
            assertEquals("TOPIC_EXISTS", Json.object(duplicate.body()).get("error"));
        }
    }

    @Test
    void postRejectsInvalidNameAndMalformedBody() throws Exception {
        try (Broker broker = Broker.start(TestConfigs.of(0, 8, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 30_000))) {
            HttpResponse<String> invalid = send(broker.adminPort(), "POST", "/topics", "{\"name\":\"NOPE\"}");
            assertEquals(400, invalid.statusCode());
            Map<String, Object> invalidBody = Json.object(invalid.body());
            assertEquals("INVALID_TOPIC_NAME", invalidBody.get("error"));
            assertTrue(invalidBody.get("message").toString().contains("[a-z0-9][a-z0-9._-]{0,63}"),
                    String.valueOf(invalidBody.get("message")));

            HttpResponse<String> malformed = send(broker.adminPort(), "POST", "/topics", "{");
            assertEquals(400, malformed.statusCode());
            assertEquals("MALFORMED_BODY", Json.object(malformed.body()).get("error"));

            HttpResponse<String> missing = send(broker.adminPort(), "POST", "/topics", "{}");
            assertEquals(400, missing.statusCode());
            assertEquals("MALFORMED_BODY", Json.object(missing.body()).get("error"));
        }
    }

    @Test
    void deleteNotifiesSubscriberAndLeavesOtherTopic() throws Exception {
        try (LogCapture logs = LogCapture.open();
                Broker broker = Broker.start(TestConfigs.of(0, 8, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 30_000));
                ProtocolClient producer = ProtocolClient.connect(broker.port());
                ProtocolClient consumer = ProtocolClient.connect(broker.port())) {
            producer.setReadTimeoutMillis(5_000);
            consumer.setReadTimeoutMillis(5_000);
            producer.hello();
            consumer.hello();
            send(broker.adminPort(), "POST", "/topics", "{\"name\":\"ops\"}");
            send(broker.adminPort(), "POST", "/topics", "{\"name\":\"sales\"}");
            consumer.subscribe("ops");
            consumer.subscribe("sales");
            logs.awaitAdded(consumer, "ops");
            logs.awaitAdded(consumer, "sales");

            HttpResponse<String> deleted = send(broker.adminPort(), "DELETE", "/topics/ops", null);
            assertEquals(204, deleted.statusCode());
            Frame error = readUntil(consumer, Command.ERROR);
            assertEquals("TOPIC_DELETED", FrameCodec.errorCode(error.header()));
            assertEquals("ops", FrameCodec.errorReason(error.header()));

            producer.publish("sales", PAYLOAD);
            assertEquals(Command.ACK, producer.readFrame().command());
            Frame delivered = readUntil(consumer, Command.DELIVER);
            assertEquals("sales", FrameCodec.delivery(delivered.header()).topic());
            assertEquals("hello", new String(delivered.payload(), StandardCharsets.UTF_8));

            HttpResponse<String> missing = send(broker.adminPort(), "DELETE", "/topics/ops", null);
            assertEquals(404, missing.statusCode());
            assertEquals("UNKNOWN_TOPIC", Json.object(missing.body()).get("error"));
        }
    }

    @Test
    void deleteRemovesLogAndDoesNotReturnAfterRestart() throws Exception {
        Path directory = Files.createTempDirectory("dess-admin-topics");
        Path logs = directory.resolve("logs");
        Path config = directory.resolve("config.xml");
        writeConfig(config, logs);
        try {
            ConfigLoader.Result loaded = ConfigLoader.load(config);
            assertTrue(loaded.problems().isEmpty(), loaded.problems()::toString);
            try (Broker broker = Broker.start(loaded.config())) {
                assertEquals(201, send(broker.adminPort(), "POST", "/topics", "{\"name\":\"ops\"}").statusCode());
                assertTrue(Files.isRegularFile(logs.resolve("ops.log")));
                assertEquals(204, send(broker.adminPort(), "DELETE", "/topics/ops", null).statusCode());
                assertFalse(Files.exists(logs.resolve("ops.log")));
            }
            ConfigLoader.Result again = ConfigLoader.load(config);
            assertTrue(again.problems().isEmpty(), again.problems()::toString);
            assertFalse(again.config().topics().contains("ops"), again.config().topics()::toString);
            try (Broker broker = Broker.start(again.config())) {
                HttpResponse<String> listed = send(broker.adminPort(), "GET", "/topics");
                assertEquals(200, listed.statusCode());
                @SuppressWarnings("unchecked")
                List<Object> topics = (List<Object>) Json.object(listed.body()).get("topics");
                assertTrue(topics.isEmpty(), listed.body());
                assertFalse(Files.exists(logs.resolve("ops.log")));
            }
        } finally {
            delete(directory);
        }
    }

    @Test
    void publishAfterDeleteFollowsAutoCreate() throws Exception {
        try (Broker broker = Broker.start(TestConfigs.of(0, 8, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 30_000));
                ProtocolClient producer = ProtocolClient.connect(broker.port())) {
            producer.setReadTimeoutMillis(5_000);
            producer.hello();
            send(broker.adminPort(), "POST", "/topics", "{\"name\":\"ops\"}");
            assertEquals(204, send(broker.adminPort(), "DELETE", "/topics/ops", null).statusCode());
            producer.publish("ops", PAYLOAD);
            assertEquals(Command.ACK, producer.readFrame().command());
        }

        BrokerConfig config = TestConfigs.full(
                0,
                8,
                TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES,
                30_000,
                TestConfigs.DEFAULT_MAX_IN_FLIGHT,
                TestConfigs.DEFAULT_TOPIC_QUEUE_CAPACITY,
                TestConfigs.DEFAULT_OUTBOUND_CAPACITY,
                TestConfigs.DEFAULT_SLOW_CONSUMER_GRACE_MILLIS,
                TestConfigs.DEFAULT_PUBLISH_WAIT_MILLIS,
                TestConfigs.DEFAULT_ACK_TIMEOUT_MILLIS,
                false,
                List.of());
        try (Broker broker = Broker.start(config);
                ProtocolClient producer = ProtocolClient.connect(broker.port())) {
            producer.setReadTimeoutMillis(5_000);
            producer.hello();
            send(broker.adminPort(), "POST", "/topics", "{\"name\":\"ops\"}");
            assertEquals(204, send(broker.adminPort(), "DELETE", "/topics/ops", null).statusCode());
            producer.publish("ops", PAYLOAD);
            Frame reply = producer.readFrame();
            assertEquals(Command.ERROR, reply.command());
            assertEquals("UNKNOWN_TOPIC", FrameCodec.errorCode(reply.header()));
        }
    }

    @Test
    void deleteRejectsInvalidName() throws Exception {
        try (Broker broker = Broker.start(TestConfigs.of(0, 8, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 30_000))) {
            HttpResponse<String> response = send(broker.adminPort(), "DELETE", "/topics/NOPE", null);
            assertEquals(400, response.statusCode());
            Map<String, Object> body = Json.object(response.body());
            assertEquals("INVALID_TOPIC_NAME", body.get("error"));
            assertTrue(body.get("message").toString().contains("[a-z0-9][a-z0-9._-]{0,63}"),
                    String.valueOf(body.get("message")));
        }
    }

    private static Frame readUntil(ProtocolClient client, Command command) throws Exception {
        while (true) {
            Frame frame = client.readFrame();
            if (frame.command() == command) {
                return frame;
            }
        }
    }

    private static HttpResponse<String> send(int port, String method, String path) throws Exception {
        return send(port, method, path, null);
    }

    private static HttpResponse<String> send(int port, String method, String path, String body) throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2))
                .build();
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(5));
        HttpRequest request = switch (method) {
            case "GET" -> builder.GET().build();
            case "POST" -> builder.header("Content-Type", "application/json; charset=UTF-8")
                    .POST(HttpRequest.BodyPublishers.ofString(body == null ? "" : body, StandardCharsets.UTF_8))
                    .build();
            case "DELETE" -> builder.DELETE().build();
            default -> throw new IllegalStateException("Unhandled method " + method);
        };
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static void writeConfig(Path file, Path logs) throws Exception {
        Files.createDirectories(logs);
        String xml = """
                <broker>
                    <network><port>0</port></network>
                    <storage>
                        <logDirectory>%s</logDirectory>
                        <durability>INTERVAL</durability>
                        <syncIntervalMs>60000</syncIntervalMs>
                    </storage>
                    <topics autoCreate="true"></topics>
                    %s
                    <admin>
                        <bindAddress>127.0.0.1</bindAddress>
                        <port>0</port>
                    </admin>
                </broker>
                """.formatted(logs.toAbsolutePath(), TestConfigs.securityXml());
        Files.writeString(file, xml);
    }

    private static void delete(Path root) throws Exception {
        if (Files.notExists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            List<Path> paths = walk.sorted(Comparator.reverseOrder()).toList();
            for (Path path : paths) {
                Files.deleteIfExists(path);
            }
        }
    }
}
