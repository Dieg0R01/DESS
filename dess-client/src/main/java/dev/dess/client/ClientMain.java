package dev.dess.client;

import dev.dess.common.cli.Help;
import dev.dess.common.logging.Logging;
import java.io.InputStream;
import java.io.PrintStream;

public final class ClientMain {

    static final String USAGE = "Usage: dess-client produce|consume --topic NAME [--topic NAME] [--message TEXT] [--confirm BROKER|ALL] [--from latest|earliest|OFFSET] [--resume] [--resume-file PATH] [--host HOST] [--port N] [--identity PATH] [--trust-store PATH] [--password-file PATH] [--heartbeat-ms N] [--log-level LEVEL]";

    private ClientMain() {
    }

    public static void main(String[] args) {
        System.exit(execute(args, System.in, System.out, System.err));
    }

    static int execute(String[] args, InputStream in, PrintStream out, PrintStream err) {
        ClientRequest request = ClientRequest.parse(args);
        if (request == null) {
            String[] safeArgs = args == null ? new String[0] : args;
            return Help.execute("dess-client", ClientMain.class, safeArgs, out, err, USAGE);
        }
        Logging.configure(request.logLevel());
        if ("consume".equals(request.command())) {
            return ConsumeCommand.execute(request, out, err);
        }
        return ProduceCommand.execute(request, in, err);
    }
}
