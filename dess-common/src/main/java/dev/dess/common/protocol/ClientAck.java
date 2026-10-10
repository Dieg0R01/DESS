package dev.dess.common.protocol;

public record ClientAck(String topic, long offset) {
}
