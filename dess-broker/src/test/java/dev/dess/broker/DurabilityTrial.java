package dev.dess.broker;

import dev.dess.common.logging.Logging;
import java.nio.file.Path;

public final class DurabilityTrial {

    private DurabilityTrial() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("usage: DurabilityTrial logDirectory ALWAYS|INTERVAL syncIntervalMillis");
            System.exit(2);
        }
        Path logDirectory = Path.of(args[0]);
        Durability durability = Durability.valueOf(args[1]);
        int syncIntervalMillis = Integer.parseInt(args[2]);
        Logging.configure("DEBUG");
        Broker.start(TestConfigs.durable(logDirectory, durability, syncIntervalMillis));
    }
}
