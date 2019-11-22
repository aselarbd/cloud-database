package de.tum.i13.shared;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConsistentHashMapTest {
    InetSocketAddress ip1 = new InetSocketAddress("192.168.1.1", 80); // be8e4f546de43337d7f0d4637a796478
    InetSocketAddress ip2 = new InetSocketAddress("192.168.1.2", 80); // c8088b91cb0f2fbcbdf107bd31e3d195
    InetSocketAddress ip3 = new InetSocketAddress("192.168.1.3", 80); // 0da0828d3687114976e0edb80e0c54d5

    // keyrange representation of a consistent hash map with the above entries
    final String keyrangeString = "0da0828d3687114976e0edb80e0c54d5,be8e4f546de43337d7f0d4637a796478,192.168.1.3:80;" +
            "be8e4f546de43337d7f0d4637a796478,c8088b91cb0f2fbcbdf107bd31e3d195,192.168.1.1:80;" +
            "c8088b91cb0f2fbcbdf107bd31e3d195,0da0828d3687114976e0edb80e0c54d5,192.168.1.2:80;";
    @Test
    void testConsistentHashMap() {
        ConsistentHashMap consistentHashMap = new ConsistentHashMap();

        String key = "key"; // 3c6e0b8a9c15224a8228b9a98ca1531d

        consistentHashMap.put(ip1);
        consistentHashMap.put(ip2);
        consistentHashMap.put(ip3);

        assertEquals(ip1, consistentHashMap.get(key));

        InetSocketAddress ip6 = new InetSocketAddress("192.168.1.6", 80); // 4ae5405a223af78c2466769f0b2cf838
        consistentHashMap.put(ip6);

        assertEquals(ip6, consistentHashMap.get(key));

        consistentHashMap.remove(ip6);
        consistentHashMap.remove(ip1);

        assertEquals(ip2, consistentHashMap.get(key));
    }

    @Test
    void testGetKeyrangeString() {
        ConsistentHashMap consistentHashMap = new ConsistentHashMap();
        consistentHashMap.put(ip1);
        consistentHashMap.put(ip2);
        consistentHashMap.put(ip3);

        String generatedKrString = consistentHashMap.getKeyrangeString();

        // expect keyrange to contain the elements ordered by their start hashes
        assertEquals(keyrangeString, generatedKrString);
    }

    @Test
    void testFromKeyrangeString() {
        ConsistentHashMap parsedMap = ConsistentHashMap.fromKeyrangeString(keyrangeString);

        // IP1 is the closest to the hash of "key", cf. test case above
        assertEquals(ip1, parsedMap.get("key"));
        // test addresses themselves
        assertEquals(ip1, parsedMap.get("192.168.1.1:80"));
        assertEquals(ip2, parsedMap.get("192.168.1.2:80"));
        assertEquals(ip3, parsedMap.get("192.168.1.3:80"));
    }

    @Test
    void testFromKeyrangeStringInvalidInput() {
        String[] illegalInputs = new String[] {
                "",
                "be8e4f546de43337d7f0d4637a796478,be8e4f546de43337d7f0d4637a796478,192.168.1.1.foo:asdf", // invalid IP
                "be8e4f546de43337d7f0d4637a796478,be8e4f546de43337d7f0d4637a796478,192.168.1.1:80", // no semicolon
                "be8e4f546de43337d7f0d4637a796478,be8e4f546de43337d7f0d4637a796478,192.168.1.1:80,foobar;", // additional arg
                "a,b,192.168.1.1:80", // invalid hashes
                "be8e4f546de43337d7f0d4637a796478,be8e4f546de43337d7f0d4637a796478,192.168.1.1:80;a,s,127.0.0.1:80,d;", // invalid 2nd item
                "foobar;be8e4f546de43337d7f0d4637a796478,be8e4f546de43337d7f0d4637a796478,192.168.1.1:80;" // invalid 1st item
        };
        for (String input : illegalInputs) {
            assertThrows(IllegalArgumentException.class, () -> {ConsistentHashMap.fromKeyrangeString(input);});
        }
    }
}