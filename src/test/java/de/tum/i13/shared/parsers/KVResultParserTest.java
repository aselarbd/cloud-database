package de.tum.i13.shared.parsers;

import de.tum.i13.shared.KVResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class KVResultParserTest {
    final KVResultParser parser = new KVResultParser();

    @Test
    public void parseKeyValue() {
        KVResult result;

        // when
        result = parser.parse("get_success key value");
        // then
        assertEquals("get_success", result.getMessage());
        assertEquals("key", result.getItem().getKey());
        assertEquals("value", result.getItem().getValue());

        // when
        result = parser.parse("something key2 value  space");
        // then
        assertEquals("something", result.getMessage());
        assertEquals("key2", result.getItem().getKey());
        assertEquals("value  space", result.getItem().getValue());
    }

    @Test
    public void parseKeyOnly() {
        KVResult result;

        // when
        result = parser.parse("put_success keyX");
        // then
        assertEquals("put_success", result.getMessage());
        assertEquals("keyX", result.getItem().getKey());
        assertNull(result.getItem().getValue());
    }
}
