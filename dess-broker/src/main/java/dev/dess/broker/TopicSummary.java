package dev.dess.broker;

import java.util.LinkedHashMap;
import java.util.Map;

record TopicSummary(String name, long retainedCount, int subscriberCount, int queueDepth) {

    Map<String, Object> toJson() {
        LinkedHashMap<String, Object> object = new LinkedHashMap<>();
        object.put("name", name);
        object.put("retainedCount", retainedCount);
        object.put("subscriberCount", (long) subscriberCount);
        object.put("queueDepth", (long) queueDepth);
        return object;
    }
}
