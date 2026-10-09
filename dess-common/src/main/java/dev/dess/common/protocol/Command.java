package dev.dess.common.protocol;

public enum Command {
    HELLO(1),
    PUBLISH(2),
    SUBSCRIBE(3),
    UNSUBSCRIBE(4),
    DELIVER(5),
    ACK(6),
    PING(7),
    PONG(8),
    ERROR(9);

    private final int code;

    Command(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static Command fromCode(int code) {
        for (Command command : values()) {
            if (command.code == code) {
                return command;
            }
        }
        throw new FrameException(ErrorCode.UNKNOWN_COMMAND, "Unknown command " + code);
    }
}
