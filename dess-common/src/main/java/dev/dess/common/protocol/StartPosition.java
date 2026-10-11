package dev.dess.common.protocol;

public record StartPosition(Kind kind, long offset) {

    public enum Kind {
        LATEST(0),
        EARLIEST(1),
        OFFSET(2);

        private final int wire;

        Kind(int wire) {
            this.wire = wire;
        }

        public int wire() {
            return wire;
        }
    }

    public StartPosition {
        if (kind == null) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "start position is required");
        }
        if (kind != Kind.OFFSET && offset != 0) {
            throw new FrameException(ErrorCode.MALFORMED_FRAME, "only an offset start carries an offset");
        }
    }

    public static StartPosition latest() {
        return new StartPosition(Kind.LATEST, 0);
    }

    public static StartPosition earliest() {
        return new StartPosition(Kind.EARLIEST, 0);
    }

    public static StartPosition at(long offset) {
        return new StartPosition(Kind.OFFSET, offset);
    }
}
