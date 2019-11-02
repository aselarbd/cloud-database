package de.tum.i13.shared.parsers;

import de.tum.i13.shared.KVItem;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class KVItemParserTest {
    @Test
    public void parseKeyOnly() {
        KVItemParser parser = new KVItemParser(false);

        String[] testKeys = {"foo", "1", "aTestKey"};
        for (String key : testKeys) {
            // when
            KVItem result = parser.parse("item " + key);
            // then
            assertEquals(key, result.getKey());
            assertNull(result.getValue());
        }
    }

    @Test
    public void parseKeyOnlyWithValue() {
        KVItemParser parser = new KVItemParser(false);

        // when
        KVItem result = parser.parse("item key value");
        // then
        assertNull(result);
    }

    @Test
    public void parseKeyValue() {
        KVItemParser parser = new KVItemParser(true);
        KVItem result;

        // when
        result = parser.parse("get key value");
        // then
        assertEquals("key", result.getKey());
        assertEquals("value", result.getValue());

        // when
        result = parser.parse("get x   1");
        // then
        assertEquals("x", result.getKey());
        assertEquals("  1", result.getValue());

        // when
        result = parser.parse("get key value with spaces");
        // then
        assertEquals("key", result.getKey());
        assertEquals("value with spaces", result.getValue());
    }

    @Test
    public void parseKeyValueKeyOnly() {
        KVItemParser parser = new KVItemParser(true);
        KVItem result;

        // when
        result = parser.parse("get key");
        // then
        assertNull(result);
    }
}
