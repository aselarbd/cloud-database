package de.tum.i13.shared;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConsistentHashMapTest {

    @Test
    void testConsistentHashMap() throws NoSuchAlgorithmException {
        ConsistentHashMap consistentHashMap = new ConsistentHashMap();
        InetSocketAddress ip1 = new InetSocketAddress("192.168.1.1", 80); // be8e4f546de43337d7f0d4637a796478
        InetSocketAddress ip2 = new InetSocketAddress("192.168.1.2", 80); // c8088b91cb0f2fbcbdf107bd31e3d195
        InetSocketAddress ip3 = new InetSocketAddress("192.168.1.3", 80); // 0da0828d3687114976e0edb80e0c54d5

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
}