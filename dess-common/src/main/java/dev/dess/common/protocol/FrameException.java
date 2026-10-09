package dev.dess.common.protocol;

public final class FrameException extends IllegalArgumentException {

    private final ErrorCode code;

    private final boolean fatal;

    private final String topic;

    public FrameException(ErrorCode code, String message) {
        this(code, message, code != ErrorCode.UNKNOWN_COMMAND, null);
    }

    public FrameException(ErrorCode code, String message, boolean fatal, String topic) {
        super(message);
        if (code == null) {
            throw new IllegalArgumentException("code is required");
        }
        this.code = code;
        this.fatal = fatal;
        this.topic = topic;
    }

    public ErrorCode code() {
        return code;
    }

    public boolean fatal() {
        return fatal;
    }

    public String topic() {
        return topic;
    }
}
