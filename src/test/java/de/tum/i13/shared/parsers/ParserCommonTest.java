package de.tum.i13.shared.parsers;

import de.tum.i13.shared.Parser;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests some common behavior of all parsers, i.e. behavior of the base class which should stay consistent
 * among implementations.
 */
public class ParserCommonTest {
    public Parser<?>[] parsers = {
            new StringArrayParser(1, false),
            new StringArrayParser(3, true),
            new KVItemParser(false),
            new KVItemParser(true),
            new KVResultParser(),
            new ECSMessageParser()
    };

    @Test
    public void parseNull() {
        Object result;

        for (Parser<?> parser : parsers) {
            // when
            result = parser.parse(null);
            // then
            assertNull(result);
        }
    }

    @Test
    public void parseEmpty() {
        Object result;

        for (Parser<?> parser : parsers) {
            // when
            result = parser.parse("");
            // then
            assertNull(result);
        }
    }

    @Test
    public void parseCmdOnly() {
        Object result;

        for (Parser<?> parser : parsers) {
            if (parser.getClass().equals(ECSMessageParser.class)) {
                // skip ECSMessages as they may have no arguments
                continue;
            }
            // when
            result = parser.parse("cmd");
            // then
            assertNull(result);
        }
    }
}
