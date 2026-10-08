package dev.dess.client;

import dev.dess.common.cli.Help;

public final class ClientMain {

    private ClientMain() {
    }

    public static void main(String[] args) {
        System.exit(Help.execute("dess-client", ClientMain.class, args, System.out, System.err));
    }
}
