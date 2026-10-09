package dev.dess.broker;

import dev.dess.common.logging.Logging;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TopicRegistry {

    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();

    public Topic open(String name) {
        return topics.computeIfAbsent(name, Topic::new);
    }

    public boolean isCurrent(Topic topic) {
        return topics.get(topic.name()) == topic;
    }

    /** Caller holds {@code topic}. */
    public void dropIfEmpty(Topic topic) {
        if (topic.isEmpty()) {
            topics.remove(topic.name(), topic);
        }
    }

    public void subscribe(ClientConnection connection, String name) {
        while (true) {
            Topic topic = open(name);
            synchronized (topic) {
                if (!isCurrent(topic)) {
                    continue;
                }
                if (topic.add(connection)) {
                    event("subscription.added", Map.of("clientId", id(connection), "topic", name));
                }
                return;
            }
        }
    }

    public void remove(ClientConnection connection, String name, String reason) {
        Topic topic = topics.get(name);
        if (topic == null) {
            return;
        }
        synchronized (topic) {
            if (topic.remove(connection.id())) {
                Map<String, String> properties = new LinkedHashMap<>();
                properties.put("clientId", id(connection));
                properties.put("topic", name);
                properties.put("reason", reason);
                event("subscription.removed", properties);
            }
            dropIfEmpty(topic);
        }
    }

    private static void event(String name, Map<String, String> properties) {
        Logging.event("dev.dess.broker", name, properties);
    }

    private static String id(ClientConnection connection) {
        return Long.toString(connection.id());
    }

    public static final class Topic {

        private final String name;

        /** Guarded by this topic, which is the delivery lock. */
        private final ConcurrentHashMap<Long, ClientConnection> subscribers = new ConcurrentHashMap<>();

        private Topic(String name) {
            this.name = name;
        }

        public String name() {
            return name;
        }

        boolean add(ClientConnection connection) {
            return subscribers.putIfAbsent(connection.id(), connection) == null;
        }

        boolean remove(long id) {
            return subscribers.remove(id) != null;
        }

        boolean isEmpty() {
            return subscribers.isEmpty();
        }

        List<ClientConnection> snapshot() {
            return List.copyOf(subscribers.values());
        }
    }
}
