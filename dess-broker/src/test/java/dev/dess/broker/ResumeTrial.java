package dev.dess.broker;

import dev.dess.common.logging.Logging;
import java.nio.file.Path;
import java.util.List;

public final class ResumeTrial {

    private ResumeTrial() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("usage: ResumeTrial logDirectory");
            System.exit(2);
        }
        Logging.configure("INFO");
        Broker.start(new BrokerConfig(
                0,
                32,
                TestConfigs.DEFAULT_MAX_PAYLOAD_BYTES,
                120_000,
                20_000,
                8_192,
                8_192,
                60_000,
                30_000,
                30_000,
                true,
                List.of(),
                Path.of(args[0]),
                Durability.INTERVAL,
                60_000,
                "INFO",
                1_000,
                5_000,
                TestPki.brokerStore(),
                TestPki.trustStore(),
                TestPki.passwordFile(),
                false,
                55124,
                false,
                null));
    }
}
