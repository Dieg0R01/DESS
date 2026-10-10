package dev.dess.broker;

import dev.dess.common.logging.Logging;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TopicRegistry {

    private final int topicQueueCapacity;

    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();

    public TopicRegistry(int topicQueueCapacity) {
        this.topicQueueCapacity = topicQueueCapacity;
    }

    public Topic open(String name) {
        return topics.computeIfAbsent(name, topic -> new Topic(topic, topicQueueCapacity));
    }

    public void subscribe(ClientConnection connection, String name) {
        Topic topic = open(name);
        if (topic.add(connection)) {
            event("subscription.added", Map.of("clientId", Long.toString(connection.id()), "topic", name));
        }
    }

    public void remove(ClientConnection connection, String name, String reason) {
        Topic topic = topics.get(name);
        if (topic == null) {
            return;
        }
        if (!topic.remove(connection)) {
            return;
        }
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("clientId", Long.toString(connection.id()));
        properties.put("topic", name);
        properties.put("reason", reason);
        event("subscription.removed", properties);
    }

    private static void event(String name, Map<String, String> properties) {
        Logging.event("dev.dess.broker", name, properties);
    }
}
