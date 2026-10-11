package dev.dess.common.protocol;

public record SubscriptionRequest(String topic, StartPosition position) {
}
