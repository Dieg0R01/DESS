package dev.dess.common.protocol;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TopicNameTest {

    @Test
    void acceptsNamesInsideThePattern() {
        assertTrue(TopicName.isValid("sales"));
        assertTrue(TopicName.isValid("a"));
        assertTrue(TopicName.isValid("sensor.temp_1"));
        assertTrue(TopicName.isValid("a".repeat(64)));
    }

    @Test
    void rejectsNamesOutsideThePattern() {
        assertFalse(TopicName.isValid(""));
        assertFalse(TopicName.isValid("Sales"));
        assertFalse(TopicName.isValid("has space"));
        assertFalse(TopicName.isValid("a".repeat(65)));
        assertFalse(TopicName.isValid(null));
    }
}
