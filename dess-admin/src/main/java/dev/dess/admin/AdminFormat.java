package dev.dess.admin;

import dev.dess.common.json.Json;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class AdminFormat {

    static final String TOPICS_HEADER = "NAME                 RETAINED  SUBSCRIBERS  QUEUE";

    private static final int NAME_WIDTH = 21;

    private static final int RETAINED_WIDTH = 8;

    private static final int SUBSCRIBERS_WIDTH = 11;

    private AdminFormat() {
    }

    static String render(AdminRequest.Verb verb, String body) {
        return switch (verb) {
            case LIST -> topicsTable(topics(Json.object(body)));
            case CREATE -> topicsTable(List.of(Json.object(body)));
            case DELETE -> {
                if (!body.isBlank()) {
                    throw new IllegalArgumentException("empty body required");
                }
                yield "";
            }
            case METRICS -> metricsTable(Json.object(body));
            case HEALTH -> healthTable(Json.object(body));
            default -> {
                AdminRequest.Verb unexpected = verb;
                throw new IllegalStateException("Unhandled verb " + unexpected);
            }
        };
    }

    private static String topicsTable(List<Map<String, Object>> rows) {
        StringBuilder builder = new StringBuilder();
        builder.append(TOPICS_HEADER).append(System.lineSeparator());
        for (Map<String, Object> row : rows) {
            builder.append(pad(text(row.get("name")), NAME_WIDTH));
            builder.append(pad(number(row.get("retainedCount")), RETAINED_WIDTH));
            builder.append("  ");
            builder.append(pad(number(row.get("subscriberCount")), SUBSCRIBERS_WIDTH));
            builder.append("  ");
            builder.append(number(row.get("queueDepth")));
            builder.append(System.lineSeparator());
        }
        return builder.toString();
    }

    private static String metricsTable(Map<String, Object> snapshot) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Object> entry : snapshot.entrySet()) {
            if ("topics".equals(entry.getKey())) {
                if (!(entry.getValue() instanceof List<?> topics)) {
                    throw new IllegalArgumentException("topics array required");
                }
                for (Object item : topics) {
                    Map<String, Object> topic = object(item);
                    builder.append("QUEUE ")
                            .append(text(topic.get("name")))
                            .append(' ')
                            .append(number(topic.get("queueDepth")))
                            .append(System.lineSeparator());
                }
            } else {
                builder.append(entry.getKey())
                        .append(' ')
                        .append(scalar(entry.getValue()))
                        .append(System.lineSeparator());
            }
        }
        return builder.toString();
    }

    private static String healthTable(Map<String, Object> body) {
        Object status = body.get("status");
        if (!(status instanceof String text) || text.isBlank()) {
            throw new IllegalArgumentException("status required");
        }
        return "STATUS " + text + System.lineSeparator();
    }

    private static List<Map<String, Object>> topics(Map<String, Object> body) {
        Object value = body.get("topics");
        if (!(value instanceof List<?> list)) {
            throw new IllegalArgumentException("topics array required");
        }
        ArrayList<Map<String, Object>> rows = new ArrayList<>();
        for (Object item : list) {
            rows.add(object(item));
        }
        return rows;
    }

    private static Map<String, Object> object(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("JSON object required");
        }
        LinkedHashMap<String, Object> object = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw new IllegalArgumentException("JSON object keys must be strings");
            }
            object.put(key, entry.getValue());
        }
        return object;
    }

    private static String pad(String value, int width) {
        if (value.length() >= width) {
            return value;
        }
        return value + " ".repeat(width - value.length());
    }

    private static String text(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("string required");
        }
        return String.valueOf(value);
    }

    private static String number(Object value) {
        if (!(value instanceof Number number)) {
            throw new IllegalArgumentException("number required");
        }
        if (value instanceof Double || value instanceof Float) {
            return Double.toString(number.doubleValue());
        }
        return Long.toString(number.longValue());
    }

    private static String scalar(Object value) {
        return switch (value) {
            case null -> "null";
            case Double number -> Double.toString(number);
            case Float number -> Double.toString(number.doubleValue());
            case Number number -> Long.toString(number.longValue());
            case String text -> text;
            case Boolean flag -> Boolean.toString(flag);
            default -> throw new IllegalArgumentException("unsupported JSON value " + value.getClass().getName());
        };
    }
}
