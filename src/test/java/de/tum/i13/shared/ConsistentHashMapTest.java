package de.tum.i13.shared;

import de.tum.i13.TestConstants;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashMapTest {
    InetSocketAddress ip1 = new InetSocketAddress("192.168.1.1", 80); // be8e4f546de43337d7f0d4637a796478
    InetSocketAddress ip2 = new InetSocketAddress("192.168.1.2", 80); // c8088b91cb0f2fbcbdf107bd31e3d195
    InetSocketAddress ip3 = new InetSocketAddress("192.168.1.3", 80); // 0da0828d3687114976e0edb80e0c54d5

    // keyrange representation of a consistent hash map with the above entries
    final String keyrangeString = TestConstants.KEYRANGE_EXT;
    @Test
    void testConsistentHashMap() {
        ConsistentHashMap consistentHashMap = new ConsistentHashMap();

        String key = "key"; // 3c6e0b8a9c15224a8228b9a98ca1531d

        consistentHashMap.put(ip1);
        consistentHashMap.put(ip2);
        consistentHashMap.put(ip3);

        assertEquals(ip1, consistentHashMap.getSuccessor(key));

        InetSocketAddress ip6 = new InetSocketAddress("192.168.1.6", 80); // 4ae5405a223af78c2466769f0b2cf838
        consistentHashMap.put(ip6);

        assertEquals(ip6, consistentHashMap.getSuccessor(key));

        consistentHashMap.remove(ip6);
        consistentHashMap.remove(ip1);

        assertEquals(ip2, consistentHashMap.getSuccessor(key));
    }

    @Test
    void testGetKeyrangeString() {
        ConsistentHashMap consistentHashMap = new ConsistentHashMap();
        consistentHashMap.put(ip1);
        consistentHashMap.put(ip2);
        consistentHashMap.put(ip3);
        // ignore replica items in generated hash
        consistentHashMap.putReplica(ip1, ip2);

        String generatedKrString = consistentHashMap.getKeyrangeString();

        // expect keyrange to contain the elements ordered by their start hashes
        assertEquals(keyrangeString, generatedKrString);
    }

    @Test
    void testGetKeyrangeStringEmptyMap() {
        ConsistentHashMap consistentHashMap = new ConsistentHashMap();

        String generatedKrString = consistentHashMap.getKeyrangeString();

        assertEquals("", generatedKrString);
    }

    private void assertBaseKeyrange(ConsistentHashMap parsedMap) {
        // IP1 is the closest to the hash of "key", cf. test case above
        assertEquals(ip1, parsedMap.getSuccessor("key"));
        // test addresses themselves
        assertEquals(ip1, parsedMap.getSuccessor("192.168.1.1:80"));
        assertEquals(ip2, parsedMap.getSuccessor("192.168.1.2:80"));
        assertEquals(ip3, parsedMap.getSuccessor("192.168.1.3:80"));
    }

    @Test
    void testFromKeyrangeString() {
        ConsistentHashMap parsedMap = ConsistentHashMap.fromKeyrangeString(keyrangeString);

        assertBaseKeyrange(parsedMap);
        assertEquals(1, parsedMap.getAllSuccessors("192.168.1.1:80").size());
        assertEquals(1, parsedMap.getAllSuccessors("192.168.1.2:80").size());
        assertEquals(1, parsedMap.getAllSuccessors("192.168.1.3:80").size());
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
                "foobar;be8e4f546de43337d7f0d4637a796478,be8e4f546de43337d7f0d4637a796478,192.168.1.1:80;", // invalid 1st item
                keyrangeString.replace("192.168.1.1:80", "192.168.1.5:80"), // wrong address - hash mapping
                TestConstants.KEYRANGE_REPLICA_FULL, // no replica allowed in "normal" keyrange string
                TestConstants.KEYRANGE_REPLICA_PART, // no replica allowed in "normal" keyrange string
                TestConstants.KEYRANGE_REPLICA_UNORDERED // no replica allowed in "normal" keyrange string
        };
        for (String input : illegalInputs) {
            assertThrows(IllegalArgumentException.class, () -> {ConsistentHashMap.fromKeyrangeString(input);});
        }
    }

    private ConsistentHashMap getFullReplicaMap() {
        ConsistentHashMap consistentHashMap = new ConsistentHashMap();
        consistentHashMap.put(ip1);
        consistentHashMap.put(ip2);
        consistentHashMap.put(ip3);
        consistentHashMap.putReplica(ip1, ip2);
        consistentHashMap.putReplica(ip1, ip3);
        consistentHashMap.putReplica(ip2, ip3);
        consistentHashMap.putReplica(ip2, ip1);
        consistentHashMap.putReplica(ip3, ip1);
        consistentHashMap.putReplica(ip3, ip2);
        return consistentHashMap;
    }

    @Test
    void testGetKeyrangeReadString() {
        ConsistentHashMap consistentHashMap = getFullReplicaMap();

        assertEquals(TestConstants.KEYRANGE_REPLICA_FULL, consistentHashMap.getKeyrangeReadString());
    }

    @Test
    void testFromKeyrangeReadString() {
        ConsistentHashMap parsedMap = ConsistentHashMap.fromKeyrangeReadString(
                TestConstants.KEYRANGE_REPLICA_FULL);

        assertBaseKeyrange(parsedMap);
        List<InetSocketAddress> oneSuccessors = parsedMap.getAllSuccessors("192.168.1.1:80");
        assertEquals(3, oneSuccessors.size());
        assertEquals(3, parsedMap.getAllSuccessors("192.168.1.2:80").size());
        assertEquals(3, parsedMap.getAllSuccessors("192.168.1.3:80").size());
        assertEquals(ip1, oneSuccessors.get(0));
        assertTrue(oneSuccessors.contains(ip2));
        assertTrue(oneSuccessors.contains(ip3));
    }
}