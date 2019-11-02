package de.tum.i13.shared.parsers;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class StringArrayParserTest {
    @Test
    public void parseFixedArgs() {
        StringArrayParser parser = new StringArrayParser(2, false);
        String[] result;

        // when
        result = parser.parse("command arg1 arg2");
        // then
        assertArrayEquals(new String[]{"arg1", "arg2"}, result);

        // when
        result = parser.parse("command arg1");
        // then
        assertNull(result);

        // when
        result = parser.parse("command arg1 arg2 arg3");
        // then
        assertNull(result);
    }

    @Test
    public void parseVarArgs() {
        StringArrayParser parser = new StringArrayParser(3, true);
        String[] result;

        // when
        result = parser.parse("command arg1 arg2 arg3 arg4");
        // then
        assertArrayEquals(new String[]{"arg1", "arg2", "arg3", "arg4"}, result);

        // when
        result = parser.parse("command arg1 arg2");
        // then
        assertNull(result);
    }
}
