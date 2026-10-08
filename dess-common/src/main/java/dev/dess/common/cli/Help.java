package dev.dess.common.cli;

import java.io.PrintStream;

public final class Help {

    private Help() {
    }

    public static int execute(String artifact, Class<?> owner, String[] args, PrintStream out, PrintStream err) {
        if (isHelp(args)) {
            out.println(artifact);
            out.println(version(owner));
            out.println(usage(artifact));
            return 0;
        }
        err.println(usage(artifact));
        return 2;
    }

    private static boolean isHelp(String[] args) {
        return args != null && args.length == 1 && ("--help".equals(args[0]) || "-h".equals(args[0]));
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
        return "Usage: " + artifact + " --help | -h";
    }
}
