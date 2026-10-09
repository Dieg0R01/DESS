package dev.dess.common.protocol;

public final class FrameException extends IllegalArgumentException {

    private final ErrorCode code;

    public FrameException(ErrorCode code, String message) {
        super(message);
        if (code == null) {
            throw new IllegalArgumentException("code is required");
        }
        this.code = code;
    }

    public ErrorCode code() {
        return code;
    }
}
