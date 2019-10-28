package de.tum.i13;

import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStore;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestKVCommandProcessor {

    @TestFactory
    Stream<DynamicTest> testPutCommandParsing() {
        return DynamicTest.stream(
                createPutCommandTestData().entrySet().iterator(),
                t -> "testing " + t.getKey(),
                t -> testParsePutCommand(t.getKey(), t.getValue()[0], t.getValue()[1])
        );
    }

    private void testParsePutCommand(String cmd, String key, String value) {
        KVStore kvStoreMock = mock(KVStore.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kvStoreMock);
        kvcp.process(cmd);
        verify(kvStoreMock).put(key, value);
    }

    private Map<String, String[]> createPutCommandTestData() {
        return Map.ofEntries(
                new AbstractMap.SimpleEntry<>("put key value", new String[]{"key", "value"}),
                new AbstractMap.SimpleEntry<>("PUT key hello", new String[]{"key", "hello"}),
                new AbstractMap.SimpleEntry<>("put      key value    ", new String[]{"key", "value    "}),
                new AbstractMap.SimpleEntry<>("PUT key     value    ", new String[]{"key", "    value    "})

        );
    }

    @TestFactory
    Stream<DynamicTest> testGetCommandParsing() {
        return DynamicTest.stream(
                createGetCommandTestData().entrySet().iterator(),
                t -> "testing " + t.getKey(),
                t -> testParseGetCommand(t.getKey(), t.getValue())
        );
    }

    private void testParseGetCommand(String cmd, String key) {
        KVStore kvStoreMock = mock(KVStore.class);
        KVCommandProcessor kvcp = new KVCommandProcessor(kvStoreMock);
        kvcp.process(cmd);
        verify(kvStoreMock).get(key);
    }

    private Map<String, String> createGetCommandTestData() {
        return Map.ofEntries(
                new AbstractMap.SimpleEntry<>("get key", "key"),
                new AbstractMap.SimpleEntry<>("GET key", "key"),
                new AbstractMap.SimpleEntry<>("get   key  ", "key")
        );
    }
}
