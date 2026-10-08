package dev.dess.broker;

import dev.dess.common.cli.Help;

public final class BrokerMain {

    private BrokerMain() {
    }

    public static void main(String[] args) {
        System.exit(Help.execute("dess-broker", BrokerMain.class, args, System.out, System.err));
    }
}
