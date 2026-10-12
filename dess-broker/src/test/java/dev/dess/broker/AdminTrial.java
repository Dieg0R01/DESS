package dev.dess.broker;

import dev.dess.common.logging.Logging;
import java.util.Map;

public final class AdminTrial {

    private AdminTrial() {
    }

    public static void main(String[] args) throws Exception {
        Logging.configure("INFO");
        Broker broker = Broker.start(TestConfigs.of(0, 8, TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES, 30_000));
        Logging.event("dev.dess.broker", "admin.ready", Map.of("adminPort", Integer.toString(broker.adminPort())));
        Thread.currentThread().join();
    }
}
