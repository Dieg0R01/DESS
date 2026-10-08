package dev.dess.admin;

import dev.dess.common.cli.Help;

public final class AdminMain {

    private AdminMain() {
    }

    public static void main(String[] args) {
        System.exit(Help.execute("dess-admin", AdminMain.class, args, System.out, System.err));
    }
}
