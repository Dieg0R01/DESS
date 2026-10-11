package dev.dess.common.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonTest {

    @Test
    void encodesAndParsesAnObject() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", "sensor-temperature");
        String encoded = Json.encode(body);
        assertEquals("{\"name\":\"sensor-temperature\"}", encoded);
        assertEquals("sensor-temperature", Json.object(encoded).get("name"));
    }

    @Test
    void encodesAndParsesAnArrayOfObjects() {
        Map<String, Object> topic = new LinkedHashMap<>();
        topic.put("name", "sales");
        topic.put("retainedCount", 12L);
        topic.put("subscriberCount", 2L);
        topic.put("queueDepth", 3L);
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("topics", List.of(topic));
        Object parsed = Json.parse(Json.encode(root));
        assertTrue(parsed instanceof Map<?, ?>);
        @SuppressWarnings("unchecked")
        Map<String, Object> object = (Map<String, Object>) parsed;
        @SuppressWarnings("unchecked")
        List<Object> topics = (List<Object>) object.get("topics");
        @SuppressWarnings("unchecked")
        Map<String, Object> first = (Map<String, Object>) topics.get(0);
        assertEquals("sales", first.get("name"));
        assertEquals(12L, first.get("retainedCount"));
        assertEquals(2L, first.get("subscriberCount"));
        assertEquals(3L, first.get("queueDepth"));
    }

    @Test
    void encodesAndParsesEscapedStrings() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", "say \"hi\"\\path\n");
        String encoded = Json.encode(body);
        assertEquals("{\"name\":\"say \\\"hi\\\"\\\\path\\n\"}", encoded);
        assertEquals("say \"hi\"\\path\n", Json.object(encoded).get("name"));
        assertEquals("line\t1", Json.parse("\"line\\t1\""));
    }

    @Test
    void encodesAndParsesNumbersBooleansAndNull() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("count", 12L);
        body.put("rate", 1000.0);
        body.put("ok", true);
        body.put("off", false);
        body.put("heapMaxBytes", null);
        String encoded = Json.encode(body);
        assertEquals("{\"count\":12,\"rate\":1000.0,\"ok\":true,\"off\":false,\"heapMaxBytes\":null}", encoded);
        Map<String, Object> parsed = Json.object(encoded);
        assertEquals(12L, parsed.get("count"));
        assertEquals(1000.0, parsed.get("rate"));
        assertEquals(true, parsed.get("ok"));
        assertEquals(false, parsed.get("off"));
        assertNull(parsed.get("heapMaxBytes"));
        assertEquals(12L, Json.parse("12"));
        assertEquals(-4.5, Json.parse("-4.5"));
    }

    @Test
    void rejectsTruncatedInputTrailingCommaAndNonObject() {
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"name\":\""));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("[1,"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"name\":\"ops\",}"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("[1,]"));
        assertThrows(IllegalArgumentException.class, () -> Json.object("[\"name\"]"));
        assertThrows(IllegalArgumentException.class, () -> Json.object("\"just a string\""));
        assertThrows(IllegalArgumentException.class, () -> Json.parse(""));
    }
}
