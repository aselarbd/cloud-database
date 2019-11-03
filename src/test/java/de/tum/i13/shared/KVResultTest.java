package de.tum.i13.shared;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KVResultTest {
    @Test
    public void stringRepresentation() {
        KVResult testRes;

        // when
        testRes = new KVResult("test");
        // then
        assertEquals("test", testRes.toString());

        // when
        testRes = new KVResult("foo", new KVItem("bar"));
        // then
        assertEquals("foo bar", testRes.toString());

        // when
        testRes = new KVResult("foo", new KVItem("bar", "baz 123"));
        // then
        assertEquals("foo bar baz 123", testRes.toString());
    }
}
