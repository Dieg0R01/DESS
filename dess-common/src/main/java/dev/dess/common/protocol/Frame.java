package dev.dess.common.protocol;

public final class Frame {

    private final Command command;
    private final byte[] header;
    private final byte[] payload;

    public Frame(Command command, byte[] header, byte[] payload) {
        if (command == null) {
            throw new IllegalArgumentException("command is required");
        }
        this.command = command;
        this.header = header == null ? new byte[0] : header.clone();
        this.payload = payload == null ? new byte[0] : payload.clone();
    }

    public Command command() {
        return command;
    }

    public byte[] header() {
        return header.clone();
    }

    public byte[] payload() {
        return payload.clone();
    }
}
