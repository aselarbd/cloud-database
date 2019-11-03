package de.tum.i13.shared;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class KVItemTest {
    @Test
    public void keyInvalid() {
        String testKey = new String(new byte[21]);

        // when
        KVItem testItem = new KVItem(testKey, "val");
        // then
        assertFalse(testItem.isValid());
    }

    @Test
    public void keyNullOrEmpty() {
        KVItem testItem;

        // when
        testItem = new KVItem(null, "val");
        // then
        assertFalse(testItem.isValid());

        // when
        testItem = new KVItem("", "val");
        // then
        assertFalse(testItem.isValid());
    }


    @Test
    public void valueInvalid() {
        String testVal = new String(new byte[120001]);

        // when
        KVItem testItem = new KVItem("key", testVal);
        // then
        assertFalse(testItem.isValid());
    }

    @Test
    public void validItem() {
        // when
        KVItem testItem = new KVItem("keyTest1", "valueItem 2");
        // then
        assertTrue(testItem.isValid());
    }

    @Test
    public void encodeDecode() {
        final String testValue = "a test value";

        // when
        KVItem src = new KVItem("key", testValue);
        // then
        assertNotEquals(testValue, src.getValueAs64());

        // when
        KVItem dest = new KVItem("key2");
        dest.setValueFrom64(src.getValueAs64());
        // then
        assertEquals(testValue, dest.getValue());
    }

    @Test
    public void nullValDecode() {
        // when
        KVItem testItem = new KVItem("key2");
        testItem.setValueFrom64(null);
        // then
        assertNull(testItem.getValue());
    }

    @Test
    public void stringRepresentation() {
        KVItem testItem;

        // when
        testItem = new KVItem("key", "and a value with spaces");
        // then
        assertEquals("key and a value with spaces", testItem.toString());

        // when
        testItem = new KVItem("key");
        // then
        assertEquals("key", testItem.toString());
    }
}
