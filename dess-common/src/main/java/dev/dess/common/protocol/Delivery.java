package dev.dess.common.protocol;

public record Delivery(String topic, long offset, byte[] digest) {
}
