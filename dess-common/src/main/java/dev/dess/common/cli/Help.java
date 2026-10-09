package dev.dess.common.cli;

import dev.dess.common.logging.Logging;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public final class Help {

    private Help() {
    }

    public static int execute(String artifact, Class<?> owner, String[] args, PrintStream out, PrintStream err) {
        return execute(artifact, owner, args, out, err, usage(artifact));
    }

    public static int execute(String artifact, Class<?> owner, String[] args, PrintStream out, PrintStream err, String usage) {
        String requested = requestedLevel(args);
        if (requested == null) {
            requested = System.getProperty("dess.log.level", "INFO");
        }
        Logging.configure(requested);
        String component = owner.getPackageName();
        if (isHelp(withoutLevel(args))) {
            out.println(Logging.format(Logging.level(), component, artifact));
            out.println(Logging.format(Logging.level(), component, version(owner)));
            out.println(Logging.format(Logging.level(), component, usage));
            return 0;
        }
        err.println(Logging.format("INFO", component, usage));
        return 2;
    }

    private static String requestedLevel(String[] args) {
        if (args == null) {
            return null;
        }
        for (int index = 0; index < args.length; index++) {
            if ("--log-level".equals(args[index])) {
                if (index + 1 >= args.length) {
                    return "";
                }
                return args[index + 1];
            }
        }
        return null;
    }

    private static String[] withoutLevel(String[] args) {
        if (args == null) {
            return new String[0];
        }
        List<String> remaining = new ArrayList<>();
        for (int index = 0; index < args.length; index++) {
            if ("--log-level".equals(args[index])) {
                if (index + 1 < args.length) {
                    index++;
                }
                continue;
            }
            remaining.add(args[index]);
        }
        return remaining.toArray(String[]::new);
    }

    private static boolean isHelp(String[] args) {
        return args.length == 1 && ("--help".equals(args[0]) || "-h".equals(args[0]));
    }

    private static String version(Class<?> owner) {
        Package ownerPackage = owner.getPackage();
        if (ownerPackage == null) {
            return "dev";
        }
        String version = ownerPackage.getImplementationVersion();
        if (version == null || version.isBlank()) {
            return "dev";
        }
        return version;
    }

    private static String usage(String artifact) {
        return "Usage: " + artifact + " --help | -h | --log-level LEVEL";
    }
}
