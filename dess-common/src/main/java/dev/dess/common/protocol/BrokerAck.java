package dev.dess.common.protocol;

public record BrokerAck(String topic, long offset, ConfirmationMode mode, int acknowledgements) {
}
