package dev.dess.common.protocol;

public record Publication(String topic, ConfirmationMode mode, byte[] digest) {
}
