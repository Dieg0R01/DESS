package dev.dess.broker;

import dev.dess.common.logging.Logging;
import dev.dess.common.protocol.StartPosition;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public final class TopicRegistry {

    private final int topicQueueCapacity;

    private final int outboundCapacity;

    private final FlowControl flow;

    private final boolean autoCreate;

    private final Consumer<Subscription> onFull;

    private final TopicLogs logs;

    private final ConfigWriter writer;

    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();

    /**
     * Mode {@code ALL} confirmations, keyed by topic and offset.
     * Inserted under the topic lock before that entry's subscriptions are marked ready.
     * Lookups and removal do not take the topic lock.
     */
    private final ConcurrentHashMap<PendingKey, PendingConfirmation> pending = new ConcurrentHashMap<>();

    public TopicRegistry(int topicQueueCapacity, int outboundCapacity, FlowControl flow, boolean autoCreate, Consumer<Subscription> onFull, TopicLogs logs, ConfigWriter writer) {
        this.topicQueueCapacity = topicQueueCapacity;
        this.outboundCapacity = outboundCapacity;
        this.flow = flow;
        this.autoCreate = autoCreate;
        this.onFull = onFull;
        this.logs = logs;
        this.writer = writer;
        for (TopicLog recovered : logs.recovered()) {
            topics.put(recovered.topic(), topic(recovered));
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put("topic", recovered.topic());
            properties.put("source", "RECOVERED");
            event("topic.created", properties);
        }
    }

    public Topic open(String name) throws IOException {
        return resolve(name, autoCreate, "AUTO");
    }

    void declare(String name) throws IOException {
        resolve(name, true, "CONFIG");
    }

    public SubscribeResult subscribe(ClientConnection connection, String name, StartPosition start) throws IOException {
        Topic topic = open(name);
        if (topic == null) {
            return SubscribeResult.UNKNOWN;
        }
        Topic.Membership membership = topic.add(connection, start);
        switch (membership) {
            case ADDED -> {
                Map<String, String> properties = new LinkedHashMap<>();
                properties.put("clientId", Long.toString(connection.id()));
                properties.put("topic", name);
                properties.put("startPosition", startPosition(start));
                event("subscription.added", properties);
                return SubscribeResult.SUBSCRIBED;
            }
            case UNCHANGED -> {
                return SubscribeResult.SUBSCRIBED;
            }
            case INVALID_OFFSET -> {
                return SubscribeResult.INVALID_OFFSET;
            }
            default -> {
                Topic.Membership unexpected = membership;
                throw new IllegalStateException("Unhandled membership " + unexpected);
            }
        }
    }

    boolean hasTopic(String name) {
        return topics.containsKey(name);
    }

    Set<String> names() {
        return Set.copyOf(topics.keySet());
    }

    Set<Long> subscribers(String name) {
        Topic topic = topics.get(name);
        if (topic == null) {
            return Set.of();
        }
        return topic.subscribers();
    }

    private Topic resolve(String name, boolean create, String source) throws IOException {
        Topic existing = topics.get(name);
        if (existing != null) {
            writer.remember(name);
            return existing;
        }
        if (!create) {
            return null;
        }
        writer.lock();
        try {
            existing = topics.get(name);
            if (existing != null) {
                writer.remember(name);
                return existing;
            }
            TopicLog log = logs.openOrCreate(name);
            Topic created = topic(log);
            topics.put(name, created);
            try {
                writer.remember(name);
            } catch (IOException ex) {
                topics.remove(name, created);
                throw ex;
            }
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put("topic", name);
            properties.put("source", source);
            event("topic.created", properties);
            return created;
        } finally {
            writer.unlock();
        }
    }

    private Topic topic(TopicLog log) {
        return new Topic(log.topic(), topicQueueCapacity, outboundCapacity, flow, this, onFull, log);
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

    private static String startPosition(StartPosition start) {
        switch (start.kind()) {
            case LATEST -> {
                return "LATEST";
            }
            case EARLIEST -> {
                return "EARLIEST";
            }
            case OFFSET -> {
                return Long.toString(start.offset());
            }
            default -> {
                StartPosition.Kind unexpected = start.kind();
                throw new IllegalStateException("Unhandled start position " + unexpected);
            }
        }
    }

    public enum SubscribeResult {
        SUBSCRIBED,
        UNKNOWN,
        INVALID_OFFSET
    }

    private record PendingKey(String topic, long offset) {
    }
}
