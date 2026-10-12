package dev.dess.broker;

import dev.dess.common.logging.Logging;
import dev.dess.common.protocol.ErrorCode;
import dev.dess.common.protocol.StartPosition;
import dev.dess.common.protocol.TopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
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

    private final MetricsRecorder metrics;

    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();

    /**
     * Mode {@code ALL} confirmations, keyed by topic and offset.
     * Inserted under the topic lock before that entry's subscriptions are marked ready.
     * Lookups and removal do not take the topic lock.
     */
    private final ConcurrentHashMap<PendingKey, PendingConfirmation> pending = new ConcurrentHashMap<>();

    public TopicRegistry(int topicQueueCapacity, int outboundCapacity, FlowControl flow, boolean autoCreate, Consumer<Subscription> onFull, TopicLogs logs, ConfigWriter writer, MetricsRecorder metrics) {
        this.topicQueueCapacity = topicQueueCapacity;
        this.outboundCapacity = outboundCapacity;
        this.flow = flow;
        this.autoCreate = autoCreate;
        this.onFull = onFull;
        this.logs = logs;
        this.writer = writer;
        this.metrics = metrics;
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
            case SEALED -> {
                return SubscribeResult.UNKNOWN;
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

    List<TopicSummary> summaries() {
        ArrayList<String> ordered = new ArrayList<>(topics.keySet());
        Collections.sort(ordered);
        ArrayList<TopicSummary> summaries = new ArrayList<>(ordered.size());
        for (String name : ordered) {
            Topic topic = topics.get(name);
            if (topic != null) {
                summaries.add(topic.summary());
            }
        }
        return summaries;
    }

    Set<Long> subscribers(String name) {
        Topic topic = topics.get(name);
        if (topic == null) {
            return Set.of();
        }
        return topic.subscribers();
    }

    Created create(String name) throws IOException {
        if (!TopicName.isValid(name)) {
            return Created.invalid();
        }
        writer.lock();
        try {
            if (topics.containsKey(name)) {
                return Created.exists();
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
            properties.put("source", "ADMIN");
            event("topic.created", properties);
            return Created.created(created.summary());
        } finally {
            writer.unlock();
        }
    }

    DeleteStatus delete(String name) throws IOException {
        if (!TopicName.isValid(name)) {
            return DeleteStatus.INVALID;
        }
        writer.lock();
        try {
            Topic topic = topics.remove(name);
            if (topic == null) {
                return DeleteStatus.NOT_FOUND;
            }
            List<ClientConnection> subscribers = topic.seal();
            pending.keySet().removeIf(key -> key.topic().equals(name));
            for (ClientConnection connection : subscribers) {
                connection.enqueueError(ErrorCode.TOPIC_DELETED, name);
                Map<String, String> removed = new LinkedHashMap<>();
                removed.put("clientId", Long.toString(connection.id()));
                removed.put("topic", name);
                removed.put("reason", "DELETED");
                event("subscription.removed", removed);
            }
            logs.delete(name);
            writer.forget(name);
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put("topic", name);
            properties.put("subscribersNotified", Integer.toString(subscribers.size()));
            event("topic.deleted", properties);
            return DeleteStatus.DELETED;
        } finally {
            writer.unlock();
        }
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
        return new Topic(log.topic(), topicQueueCapacity, outboundCapacity, flow, this, onFull, log, metrics);
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

    enum CreateStatus {
        CREATED,
        EXISTS,
        INVALID
    }

    enum DeleteStatus {
        DELETED,
        NOT_FOUND,
        INVALID
    }

    record Created(CreateStatus status, TopicSummary summary) {

        static Created created(TopicSummary summary) {
            return new Created(CreateStatus.CREATED, summary);
        }

        static Created exists() {
            return new Created(CreateStatus.EXISTS, null);
        }

        static Created invalid() {
            return new Created(CreateStatus.INVALID, null);
        }
    }

    private record PendingKey(String topic, long offset) {
    }
}
