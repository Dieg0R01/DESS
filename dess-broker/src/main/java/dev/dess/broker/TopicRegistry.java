package dev.dess.broker;

import dev.dess.common.logging.Logging;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public final class TopicRegistry {

    private final int topicQueueCapacity;

    private final int outboundCapacity;

    private final FlowControl flow;

    private final Consumer<Subscription> onFull;

    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();

    /**
     * Mode {@code ALL} confirmations, keyed by topic and offset.
     * Inserted under the topic lock before that entry's subscriptions are marked ready.
     * Lookups and removal do not take the topic lock.
     */
    private final ConcurrentHashMap<PendingKey, PendingConfirmation> pending = new ConcurrentHashMap<>();

    public TopicRegistry(int topicQueueCapacity, int outboundCapacity, FlowControl flow, Consumer<Subscription> onFull) {
        this.topicQueueCapacity = topicQueueCapacity;
        this.outboundCapacity = outboundCapacity;
        this.flow = flow;
        this.onFull = onFull;
    }

    public Topic open(String name) {
        return topics.computeIfAbsent(name, topic -> new Topic(topic, topicQueueCapacity, outboundCapacity, flow, this, onFull));
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

    void register(String topic, long offset, PendingConfirmation confirmation) {
        pending.put(new PendingKey(topic, offset), confirmation);
    }

    PendingConfirmation confirmation(String topic, long offset) {
        return pending.get(new PendingKey(topic, offset));
    }

    void discard(String topic, long offset) {
        pending.remove(new PendingKey(topic, offset));
    }

    void dropExpecting(long clientId) {
        for (PendingConfirmation confirmation : pending.values()) {
            confirmation.remove(clientId);
        }
    }

    private static void event(String name, Map<String, String> properties) {
        Logging.event("dev.dess.broker", name, properties);
    }

    private record PendingKey(String topic, long offset) {
    }
}
