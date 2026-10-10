package dev.dess.common.protocol;

public record AckTimeout(String code, String reason, String topic, long offset, int expected, int received) {
}
