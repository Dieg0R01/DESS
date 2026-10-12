package dev.dess.admin;

import dev.dess.common.cli.Help;
import dev.dess.common.logging.Logging;
import java.io.PrintStream;

public final class AdminMain {

    static final String USAGE =
            "Usage: dess-admin [--url URL] [--json] [--log-level LEVEL] topics list|create NAME|delete NAME|metrics|health";

    private AdminMain() {
    }

    public static void main(String[] args) {
        System.exit(execute(args, System.out, System.err));
    }

    static int execute(String[] args, PrintStream out, PrintStream err) {
        String[] safeArgs = args == null ? new String[0] : args;
        AdminRequest request = AdminRequest.parse(safeArgs);
        if (request == null) {
            return Help.execute("dess-admin", AdminMain.class, safeArgs, out, err, USAGE);
        }
        Logging.configure(request.logLevel());
        return AdminClient.execute(request, out, err);
    }
}
