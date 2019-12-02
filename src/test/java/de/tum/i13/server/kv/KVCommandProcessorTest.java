package de.tum.i13.server.kv;

import de.tum.i13.TestConstants;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.KVItem;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KVCommandProcessorTest {

    private static class TestCase {

        TestCase(
                String action,
                String key,
                String value,
                String result,
                Exception exception,
                KVCache cache,
                KVStore store
        ) {
            this.action = action;
            this.key = key;
            this.value = value;
            this.result = result;
            this.exception = exception;

            this.cache = cache;
            this.store = store;
        }

        private String action;
        private String key;
        private String value;
        private String result;
        private Exception exception;

        private KVCache cache;
        private KVStore store;
    }

    @TestFactory
    Stream<DynamicTest> testProcessPutAndGet() throws IOException {

        List<TestCase> cases = Arrays.asList(
                new TestCase(
                        "get",
                        "key1",
                        "value",
                        "get_success key1 value",
                        null,
                        null,
                        mock(KVStore.class)
                ),
                new TestCase(
                        "get",
                        "key2",
                        null,
                        "get_error key2 Key not found in the Database",
                        null,
                        null,
                        mock(KVStore.class)
                ),
                new TestCase(
                        "get",
                        "exception",
                        null,
                        "get_error exception Could not get value from Database",
                        new IOException(),
                        null,
                        mock(KVStore.class)
                ),
                new TestCase(
                        "get",
                        "key1",
                        "value",
                        "get_success key1 value",
                        null,
                        mock(KVCache.class),
                        mock(KVStore.class)
                ),
                new TestCase(
                        "get",
                        "key2",
                        null,
                        "get_error key2 Key not found in the Database",
                        null,
                        mock(KVCache.class),
                        mock(KVStore.class)
                ),
                new TestCase(
                        "get",
                        "",
                        null,
                        "key needed",
                        null,
                        mock(KVCache.class),
                        mock(KVStore.class)
                ),
                new TestCase(
                        "put",
                        "key",
                        "value",
                        "put_success key",
                        null,
                        mock(KVCache.class),
                        mock(KVStore.class)
                ),
                new TestCase(
                        "put",
                        "key",
                        "exception",
                        "put_error key exception",
                        new IOException(),
                        mock(KVCache.class),
                        mock(KVStore.class)
                ),
                new TestCase(
                        "put",
                        "",
                        null,
                        "key needed",
                        null,
                        mock(KVCache.class),
                        mock(KVStore.class)
                ),
                new TestCase(
                        "delete",
                        "key",
                        "value",
                        "delete_success key",
                        null,
                        mock(KVCache.class),
                        mock(KVStore.class)
                ),
                new TestCase(
                        "delete",
                        "key",
                        null,
                        "delete_error key",
                        null,
                        mock(KVCache.class),
                        mock(KVStore.class)
                ),
                new TestCase(
                        "delete",
                        "key",
                        "value",
                        "delete_error key",
                        new IOException(),
                        mock(KVCache.class),
                        mock(KVStore.class)
                ),
                new TestCase(
                        "delete",
                        "",
                        null,
                        "key needed",
                        null,
                        mock(KVCache.class),
                        mock(KVStore.class)
                ),
                new TestCase(
                        "",
                        "",
                        "",
                        "unknown command",
                        null,
                        mock(KVCache.class),
                        mock(KVStore.class)
                ),
                new TestCase(
                        "foo bar",
                        "some long",
                        "unknown values",
                        "unknown command",
                        null,
                        mock(KVCache.class),
                        mock(KVStore.class)
                )
        );

        // setup mocks
        for (TestCase c : cases) {
            if (c.exception != null) {
                when(c.store.get(c.key)).thenThrow(c.exception);
                when(c.store.put(any())).thenThrow(c.exception);
            } else {
                when(c.store.get(c.key)).thenReturn(c.value == null ? null : new KVItem(c.key, c.value));
                when(c.store.put(any())).thenReturn("success");
            }

            if (c.cache != null) {
                when(c.cache.get(c.key)).thenReturn(c.value == null ? null : new KVItem(c.key, c.value));
            } else {
                c.cache = mock(KVCache.class);
            }
        }

        return cases.stream()
                .map(c -> DynamicTest.dynamicTest(
                    c.action + " " + c.key + ":" + c.value,
                    () -> {
                        ConsistentHashMap testRange = ConsistentHashMap
                                .fromKeyrangeString(TestConstants.KEYRANGE_SIMPLE);
                        // use get with any string as the simple test string only contains one address
                        KVCommandProcessor kvcp = new KVCommandProcessor(testRange.getSuccessor("any"), c.cache, c.store);
                        kvcp.setKeyRange(testRange);
                        String processed = kvcp.process(
                                null,
                                c.action + " " + c.key + (c.value != null ? " " + c.value : "")
                        );
                        assertEquals(c.result, processed);
                    })
                );
    }
}