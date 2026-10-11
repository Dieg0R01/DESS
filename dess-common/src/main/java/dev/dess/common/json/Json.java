package dev.dess.common.json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Json {

    private Json() {
    }

    public static String encode(Object value) {
        StringBuilder builder = new StringBuilder();
        write(builder, value);
        return builder.toString();
    }

    public static Object parse(String text) {
        if (text == null) {
            throw new IllegalArgumentException("JSON text is required");
        }
        Reader reader = new Reader(text);
        Object value = reader.value();
        reader.skip();
        if (!reader.done()) {
            throw reader.fail("trailing characters");
        }
        return value;
    }

    public static Map<String, Object> object(String text) {
        Object value = parse(text);
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("JSON object required");
        }
        LinkedHashMap<String, Object> object = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            object.put((String) entry.getKey(), entry.getValue());
        }
        return object;
    }

    private static void write(StringBuilder builder, Object value) {
        switch (value) {
            case null -> builder.append("null");
            case String text -> writeString(builder, text);
            case Boolean flag -> builder.append(flag);
            case Double number -> writeDouble(builder, number);
            case Float number -> writeDouble(builder, number.doubleValue());
            case Number number -> builder.append(number.longValue());
            case Map<?, ?> map -> writeObject(builder, map);
            case List<?> list -> writeArray(builder, list);
            default -> throw new IllegalArgumentException("unsupported JSON value " + value.getClass().getName());
        }
    }

    private static void writeDouble(StringBuilder builder, double number) {
        if (Double.isNaN(number) || Double.isInfinite(number)) {
            throw new IllegalArgumentException("JSON numbers must be finite");
        }
        builder.append(number);
    }

    private static void writeObject(StringBuilder builder, Map<?, ?> map) {
        builder.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw new IllegalArgumentException("JSON object keys must be strings");
            }
            if (!first) {
                builder.append(',');
            }
            first = false;
            writeString(builder, key);
            builder.append(':');
            write(builder, entry.getValue());
        }
        builder.append('}');
    }

    private static void writeArray(StringBuilder builder, List<?> list) {
        builder.append('[');
        boolean first = true;
        for (Object element : list) {
            if (!first) {
                builder.append(',');
            }
            first = false;
            write(builder, element);
        }
        builder.append(']');
    }

    private static void writeString(StringBuilder builder, String text) {
        builder.append('"');
        for (int index = 0; index < text.length(); index++) {
            char character = text.charAt(index);
            switch (character) {
                case '"' -> builder.append("\\\"");
                case '\\' -> builder.append("\\\\");
                case '\b' -> builder.append("\\b");
                case '\f' -> builder.append("\\f");
                case '\n' -> builder.append("\\n");
                case '\r' -> builder.append("\\r");
                case '\t' -> builder.append("\\t");
                default -> {
                    if (character < 0x20) {
                        builder.append(String.format("\\u%04x", (int) character));
                    } else {
                        builder.append(character);
                    }
                }
            }
        }
        builder.append('"');
    }

    private static final class Reader {

        private final String text;

        private int at;

        private Reader(String text) {
            this.text = text;
        }

        private Object value() {
            skip();
            if (done()) {
                throw fail("truncated JSON");
            }
            char character = text.charAt(at);
            return switch (character) {
                case '{' -> object();
                case '[' -> array();
                case '"' -> string();
                case 't' -> literal("true", Boolean.TRUE);
                case 'f' -> literal("false", Boolean.FALSE);
                case 'n' -> literal("null", null);
                case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' -> number();
                default -> throw fail("unexpected character");
            };
        }

        private Map<String, Object> object() {
            at++;
            skip();
            LinkedHashMap<String, Object> object = new LinkedHashMap<>();
            if (take('}')) {
                return object;
            }
            while (true) {
                skip();
                if (done() || text.charAt(at) != '"') {
                    throw fail("object key required");
                }
                String key = string();
                skip();
                if (!take(':')) {
                    throw fail("colon required");
                }
                object.put(key, value());
                skip();
                if (take('}')) {
                    return object;
                }
                if (!take(',')) {
                    throw fail("comma or closing brace required");
                }
                skip();
                if (peek('}')) {
                    throw fail("trailing comma");
                }
            }
        }

        private List<Object> array() {
            at++;
            skip();
            ArrayList<Object> values = new ArrayList<>();
            if (take(']')) {
                return values;
            }
            while (true) {
                values.add(value());
                skip();
                if (take(']')) {
                    return values;
                }
                if (!take(',')) {
                    throw fail("comma or closing bracket required");
                }
                skip();
                if (peek(']')) {
                    throw fail("trailing comma");
                }
            }
        }

        private String string() {
            if (!take('"')) {
                throw fail("string required");
            }
            StringBuilder builder = new StringBuilder();
            while (!done()) {
                char character = text.charAt(at++);
                if (character == '"') {
                    return builder.toString();
                }
                if (character == '\\') {
                    builder.append(escape());
                    continue;
                }
                if (character < 0x20) {
                    throw fail("unescaped control character");
                }
                builder.append(character);
            }
            throw fail("truncated JSON");
        }

        private char escape() {
            if (done()) {
                throw fail("truncated JSON");
            }
            char character = text.charAt(at++);
            return switch (character) {
                case '"', '\\', '/' -> character;
                case 'b' -> '\b';
                case 'f' -> '\f';
                case 'n' -> '\n';
                case 'r' -> '\r';
                case 't' -> '\t';
                case 'u' -> unicode();
                default -> throw fail("unknown escape");
            };
        }

        private char unicode() {
            if (at + 4 > text.length()) {
                throw fail("truncated JSON");
            }
            int value = 0;
            for (int index = 0; index < 4; index++) {
                int digit = hex(text.charAt(at++));
                value = (value << 4) | digit;
            }
            return (char) value;
        }

        private int hex(char character) {
            if (character >= '0' && character <= '9') {
                return character - '0';
            }
            if (character >= 'a' && character <= 'f') {
                return character - 'a' + 10;
            }
            if (character >= 'A' && character <= 'F') {
                return character - 'A' + 10;
            }
            throw fail("invalid hex digit");
        }

        private Object number() {
            int start = at;
            if (take('-')) {
                if (done() || !digit(text.charAt(at))) {
                    throw fail("truncated JSON");
                }
            }
            if (take('0')) {
                if (!done() && digit(text.charAt(at))) {
                    throw fail("leading zero");
                }
            } else if (!done() && digit(text.charAt(at))) {
                while (!done() && digit(text.charAt(at))) {
                    at++;
                }
            } else {
                throw fail("number required");
            }
            boolean fraction = false;
            if (take('.')) {
                fraction = true;
                if (done() || !digit(text.charAt(at))) {
                    throw fail("truncated JSON");
                }
                while (!done() && digit(text.charAt(at))) {
                    at++;
                }
            }
            if (!done()) {
                char exponent = text.charAt(at);
                if (exponent == 'e' || exponent == 'E') {
                    fraction = true;
                    at++;
                    if (!done()) {
                        char sign = text.charAt(at);
                        if (sign == '+' || sign == '-') {
                            at++;
                        }
                    }
                    if (done() || !digit(text.charAt(at))) {
                        throw fail("truncated JSON");
                    }
                    while (!done() && digit(text.charAt(at))) {
                        at++;
                    }
                }
            }
            String token = text.substring(start, at);
            try {
                if (fraction) {
                    return Double.valueOf(token);
                }
                return Long.valueOf(token);
            } catch (NumberFormatException ex) {
                throw fail("invalid number");
            }
        }

        private Object literal(String expected, Object value) {
            if (at + expected.length() > text.length() || !text.startsWith(expected, at)) {
                throw fail("truncated JSON");
            }
            at += expected.length();
            return value;
        }

        private void skip() {
            while (!done()) {
                char character = text.charAt(at);
                if (character != ' ' && character != '\t' && character != '\n' && character != '\r') {
                    return;
                }
                at++;
            }
        }

        private boolean take(char expected) {
            if (peek(expected)) {
                at++;
                return true;
            }
            return false;
        }

        private boolean peek(char expected) {
            return !done() && text.charAt(at) == expected;
        }

        private boolean done() {
            return at >= text.length();
        }

        private static boolean digit(char character) {
            return character >= '0' && character <= '9';
        }

        private IllegalArgumentException fail(String message) {
            return new IllegalArgumentException(message);
        }
    }
}
