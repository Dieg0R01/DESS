package dev.dess.common.protocol;

public enum ConfirmationMode {
    BROKER(0),
    ALL(1);

    private final int wire;

    ConfirmationMode(int wire) {
        this.wire = wire;
    }

    public int wire() {
        return wire;
    }

    public static ConfirmationMode fromWire(int value) {
        return switch (value) {
            case 0 -> BROKER;
            case 1 -> ALL;
            default -> throw new FrameException(ErrorCode.MALFORMED_FRAME, "unknown confirmation mode");
        };
    }
}
