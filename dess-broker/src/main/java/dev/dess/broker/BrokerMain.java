package dev.dess.broker;

import dev.dess.common.cli.Help;
import dev.dess.common.logging.Logging;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

public final class BrokerMain {

    static final String USAGE = "Usage: dess-broker [--config PATH]";

    private BrokerMain() {
    }

    public static void main(String[] args) {
        System.exit(execute(args, System.out, System.err));
    }

    static int execute(String[] args, PrintStream out, PrintStream err) {
        String[] safeArgs = args == null ? new String[0] : args;
        if (asksForHelp(safeArgs)) {
            String flag = contains(safeArgs, "--help") ? "--help" : "-h";
            return Help.execute("dess-broker", BrokerMain.class, new String[] {flag}, out, err, USAGE);
        }
        Path configFile = configFile(safeArgs);
        if (configFile == null) {
            return Help.execute("dess-broker", BrokerMain.class, safeArgs, out, err, USAGE);
        }
        if (!Files.isRegularFile(configFile)) {
            err.println("Missing configuration file " + configFile.toAbsolutePath());
            return 1;
        }
        ConfigLoader.Result loaded;
        try {
            loaded = ConfigLoader.load(configFile);
        } catch (IOException ex) {
            err.println(configFile.toAbsolutePath() + ": " + (ex.getMessage() == null ? "unreadable" : ex.getMessage()));
            return 1;
        }
        if (!loaded.problems().isEmpty()) {
            for (String problem : loaded.problems()) {
                err.println(problem);
            }
            return 1;
        }
        Logging.configure(loaded.config().logLevel());
        Broker broker;
        try {
            broker = Broker.start(loaded.config());
        } catch (IOException ex) {
            err.println(ex.getMessage() == null ? ex.toString() : ex.getMessage());
            return 1;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            broker.close("SIGNAL");
            System.out.flush();
            Runtime.getRuntime().halt(0);
        }, "dess-broker-shutdown"));
        try {
            Thread.currentThread().join();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        return 0;
    }

    private static boolean asksForHelp(String[] args) {
        return contains(args, "--help") || contains(args, "-h");
    }

    private static boolean contains(String[] args, String flag) {
        for (String arg : args) {
            if (flag.equals(arg)) {
                return true;
            }
        }
        return false;
    }

    private static Path configFile(String[] args) {
        String path = null;
        for (int index = 0; index < args.length; index++) {
            if (!"--config".equals(args[index])) {
                return null;
            }
            if (path != null || index + 1 >= args.length) {
                return null;
            }
            String value = args[++index];
            if (value.isEmpty() || value.startsWith("-")) {
                return null;
            }
            path = value;
        }
        return Path.of(path == null ? "config.xml" : path);
    }
}
