package de.tum.i13.shared;

import de.tum.i13.TestConstants;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashMapTest {
    final InetSocketAddress ip1 = TestConstants.IP_1; // be8e4f546de43337d7f0d4637a796478
    final InetSocketAddress ip2 = TestConstants.IP_2; // c8088b91cb0f2fbcbdf107bd31e3d195
    final InetSocketAddress ip3 = TestConstants.IP_3; // 0da0828d3687114976e0edb80e0c54d5
    final InetSocketAddress ip6 = new InetSocketAddress("192.168.1.6", 80); // 4ae5405a223af78c2466769f0b2cf838

    private ConsistentHashMap getBaseMap() {
        ConsistentHashMap consistentHashMap = new ConsistentHashMap();
        consistentHashMap.put(ip1);
        consistentHashMap.put(ip2);
        consistentHashMap.put(ip3);
        return consistentHashMap;
    }

    private ConsistentHashMap getFullReplicaMap() {
        ConsistentHashMap consistentHashMap = getBaseMap();
        consistentHashMap.putReplica(ip1, ip2);
        consistentHashMap.putReplica(ip1, ip3);
        consistentHashMap.putReplica(ip2, ip3);
        consistentHashMap.putReplica(ip2, ip1);
        consistentHashMap.putReplica(ip3, ip1);
        consistentHashMap.putReplica(ip3, ip2);
        return consistentHashMap;
    }

    // keyrange representation of a consistent hash map with the above entries
    final String keyrangeString = TestConstants.KEYRANGE_EXT;
    @Test
    void testConsistentHashMap() {
        ConsistentHashMap consistentHashMap = getBaseMap();

        String key = "key"; // 3c6e0b8a9c15224a8228b9a98ca1531d

        assertEquals(ip1, consistentHashMap.getSuccessor(key));

        consistentHashMap.put(ip6);

        assertEquals(ip6, consistentHashMap.getSuccessor(key));

        consistentHashMap.remove(ip6);
        consistentHashMap.remove(ip1);

        assertEquals(ip2, consistentHashMap.getSuccessor(key));
    }

    @Test
    void testGetKeyrangeString() {
        ConsistentHashMap consistentHashMap = getBaseMap();
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

    private void assertFullReplicaKeyrange(ConsistentHashMap parsedMap) {
        assertBaseKeyrange(parsedMap);
        List<InetSocketAddress> oneSuccessors = parsedMap.getAllSuccessors("192.168.1.1:80");
        List<InetSocketAddress> twoSuccessors = parsedMap.getAllSuccessors("192.168.1.2:80");
        List<InetSocketAddress> threeSuccessors = parsedMap.getAllSuccessors("192.168.1.3:80");
        assertEquals(3, oneSuccessors.size());
        assertEquals(3, twoSuccessors.size());
        assertEquals(3, threeSuccessors.size());
        assertEquals(ip1, oneSuccessors.get(0));
        assertTrue(oneSuccessors.contains(ip2));
        assertTrue(oneSuccessors.contains(ip3));
        assertEquals(ip2, twoSuccessors.get(0));
        assertTrue(twoSuccessors.contains(ip1));
        assertTrue(twoSuccessors.contains(ip3));
        assertEquals(ip3, threeSuccessors.get(0));
        assertTrue(threeSuccessors.contains(ip1));
        assertTrue(threeSuccessors.contains(ip2));
    }

    private void assertIllegalInputException(Consumer<String> runFunction) {
        String[] illegalInputs = new String[] {
                "be8e4f546de43337d7f0d4637a796478,be8e4f546de43337d7f0d4637a796478,192.168.1.1.foo:asdf", // invalid IP
                "be8e4f546de43337d7f0d4637a796478,be8e4f546de43337d7f0d4637a796478,192.168.1.1:80", // no semicolon
                "be8e4f546de43337d7f0d4637a796478,be8e4f546de43337d7f0d4637a796478,192.168.1.1:80,foobar;", // additional arg
                "a,b,192.168.1.1:80", // invalid hashes
                "be8e4f546de43337d7f0d4637a796478,be8e4f546de43337d7f0d4637a796478,192.168.1.1:80;a,s,127.0.0.1:80,d;", // invalid 2nd item
                "foobar;be8e4f546de43337d7f0d4637a796478,be8e4f546de43337d7f0d4637a796478,192.168.1.1:80;", // invalid 1st item
                keyrangeString.replace("192.168.1.1:80", "192.168.1.5:80"), // wrong address - hash mapping
        };
        for (String input : illegalInputs) {
            assertThrows(IllegalArgumentException.class, () -> runFunction.accept(input));
        }
    }

    @Test
    void testParseEmptyInputNewInstance() {
        // not null and empty instance for empty input. For null, this would just thrown NPEs
        assertEquals(0, ConsistentHashMap.fromKeyrangeString("").size());
        assertEquals(0, ConsistentHashMap.fromKeyrangeReadString("").size());
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
        // check base cases
        assertIllegalInputException(ConsistentHashMap::fromKeyrangeString);
        // check additional cases for "normal" keyrange strings
        String[] illegalInputs = new String[] {
                TestConstants.KEYRANGE_REPLICA_FULL, // no replica allowed in "normal" keyrange string
                TestConstants.KEYRANGE_REPLICA_PART,
                TestConstants.KEYRANGE_REPLICA_UNORDERED
        };
        for (String input : illegalInputs) {
            assertThrows(IllegalArgumentException.class, () -> ConsistentHashMap.fromKeyrangeString(input));
        }
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

        assertFullReplicaKeyrange(parsedMap);
    }

    @Test
    void testFromKeyrangeReadStringUnordered() {
        ConsistentHashMap parsedMap = ConsistentHashMap.fromKeyrangeReadString(
                TestConstants.KEYRANGE_REPLICA_UNORDERED);

        assertFullReplicaKeyrange(parsedMap);
    }

    @Test
    void testFromKeyrangeReadStringPartial() {
        ConsistentHashMap parsedMap = ConsistentHashMap.fromKeyrangeReadString(
                TestConstants.KEYRANGE_REPLICA_PART);

        assertBaseKeyrange(parsedMap);
        List<InetSocketAddress> threeSuccessors = parsedMap.getAllSuccessors("192.168.1.3:80");
        assertEquals(1, parsedMap.getAllSuccessors("192.168.1.1:80").size());
        assertEquals(3, parsedMap.getAllSuccessors("192.168.1.2:80").size());
        assertEquals(2, threeSuccessors.size());
        assertEquals(ip3, threeSuccessors.get(0));
        assertTrue(threeSuccessors.contains(ip1));
    }

    @Test
    void testIllegalInputKeyrangeReadStringInvalidInput() {
        // check base cases
        assertIllegalInputException(ConsistentHashMap::fromKeyrangeReadString);
        // check additional cases for read keyrange strings
        String[] illegalInputs = new String[] {
            TestConstants.KEYRANGE_SIMPLE + "ce8e4f546de43337d7f0d4637a796478,be8e4f546de43337d7f0d4637a796478," +
                    "192.168.1.2:80;" // replica for a non-existent start hash (starts with c instead of b)
        };
        for (String input : illegalInputs) {
            assertThrows(IllegalArgumentException.class, () -> ConsistentHashMap.fromKeyrangeReadString(input));
        }
    }

    @Test
    void testSuccessors() {
        ConsistentHashMap consistentHashMap = getFullReplicaMap();
        // successor is >=
        assertEquals(ip1, consistentHashMap.getSuccessor(ip1));
        assertEquals(ip2, consistentHashMap.getSuccessor(ip2));
        assertEquals(ip3, consistentHashMap.getSuccessor(ip3));
        // ensure every node has two replica, i.e. everything replicates everything in the full map case
        InetSocketAddress[] addrs = {ip1, ip2, ip3};
        for (InetSocketAddress addr : addrs) {
            List<InetSocketAddress> allSuccessors = consistentHashMap.getAllSuccessors(addr);
            assertEquals(addr, allSuccessors.get(0));
            assertTrue(allSuccessors.contains(ip1));
            assertTrue(allSuccessors.contains(ip2));
            assertTrue(allSuccessors.contains(ip3));
        }
    }

    @Test
    void testPredecessors() {
        ConsistentHashMap consistentHashMap = getFullReplicaMap();
        // predecessor is <
        assertEquals(ip1, consistentHashMap.getPredecessor(ip2));
        assertEquals(ip2, consistentHashMap.getPredecessor(ip3));
        assertEquals(ip3, consistentHashMap.getPredecessor(ip1));
    }

    @Test
    void testRemoval() {
        ConsistentHashMap consistentHashMap = getFullReplicaMap();
        assertEquals(ip1, consistentHashMap.getSuccessor("key"));

        consistentHashMap.remove(ip1);

        assertEquals(ip2, consistentHashMap.getSuccessor("key"));
        List<InetSocketAddress> twoSuccessors = consistentHashMap.getAllSuccessors("192.168.1.2:80");
        List<InetSocketAddress> threeSuccessors = consistentHashMap.getAllSuccessors("192.168.1.3:80");
        assertEquals(2, twoSuccessors.size());
        assertEquals(2, threeSuccessors.size());
        assertEquals(ip2, twoSuccessors.get(0));
        assertTrue(twoSuccessors.contains(ip3));
        assertEquals(ip3, threeSuccessors.get(0));
        assertTrue(threeSuccessors.contains(ip2));
    }

    @Test
    void testNullInputs() {
        ConsistentHashMap consistentHashMap = getFullReplicaMap();

        // should be empty lists, not null values (this would throw for null values, so check is sufficient)
        assertTrue(consistentHashMap.getAllSuccessors((String) null).isEmpty());
        assertTrue(consistentHashMap.getAllSuccessors((InetSocketAddress) null).isEmpty());
        // the other calls should return null
        assertNull(consistentHashMap.getSuccessor((String) null));
        assertNull(consistentHashMap.getSuccessor((InetSocketAddress) null));
        assertNull(consistentHashMap.getPredecessor(null));
    }

    @Test
    void testGetReplicaSmallMap() {
        ConsistentHashMap consistentHashMap = new ConsistentHashMap();
        consistentHashMap.put(ip1);
        consistentHashMap.put(ip2);

        ConsistentHashMap replicatedMap = consistentHashMap.getInstanceWithReplica();
        assertEquals(2, replicatedMap.size());
        List<InetSocketAddress> oneSuccessors = replicatedMap.getAllSuccessors("192.168.1.1:80");
        List<InetSocketAddress> twoSuccessors = replicatedMap.getAllSuccessors("192.168.1.2:80");
        assertEquals(1, oneSuccessors.size());
        assertEquals(1, twoSuccessors.size());
        assertEquals(ip1, oneSuccessors.get(0));
        assertEquals(ip2, twoSuccessors.get(0));
    }

    @Test
    void testGetReplica() {
        ConsistentHashMap consistentHashMap = getBaseMap();

        ConsistentHashMap replicatedMap = consistentHashMap.getInstanceWithReplica();

        assertFullReplicaKeyrange(replicatedMap);
    }

    @Test
    void testGetReplicaLarger() {
        ConsistentHashMap consistentHashMap = getBaseMap();
        consistentHashMap.put(ip6);

        ConsistentHashMap replicatedMap = consistentHashMap.getInstanceWithReplica();

        List<InetSocketAddress> oneSuccessors = replicatedMap.getAllSuccessors("192.168.1.1:80");
        List<InetSocketAddress> twoSuccessors = replicatedMap.getAllSuccessors("192.168.1.2:80");
        List<InetSocketAddress> threeSuccessors = replicatedMap.getAllSuccessors("192.168.1.3:80");
        List<InetSocketAddress> sixSuccessors = replicatedMap.getAllSuccessors("192.168.1.6:80");
        assertEquals(3, oneSuccessors.size());
        assertEquals(3, twoSuccessors.size());
        assertEquals(3, threeSuccessors.size());
        assertEquals(3, sixSuccessors.size());
        assertEquals(ip6, sixSuccessors.get(0));
        assertTrue(sixSuccessors.contains(ip1));
        assertTrue(sixSuccessors.contains(ip2));
        assertTrue(twoSuccessors.contains(ip6));
        assertTrue(threeSuccessors.contains(ip6));
    }

    @Test
    void testGetReplicaSomePresent() {
        ConsistentHashMap consistentHashMap = getBaseMap();
        consistentHashMap.putReplica(ip1, ip3);

        ConsistentHashMap replicatedMap = consistentHashMap.getInstanceWithReplica();

        // still a full keyrange expected
        assertFullReplicaKeyrange(replicatedMap);
    }
}