package dev.dess.common.protocol;

import java.util.regex.Pattern;

public final class TopicName {

    private static final Pattern NAME = Pattern.compile("[a-z0-9][a-z0-9._-]{0,63}");

    private TopicName() {
    }

    public static boolean isValid(String name) {
        return name != null && NAME.matcher(name).matches();
    }
}
