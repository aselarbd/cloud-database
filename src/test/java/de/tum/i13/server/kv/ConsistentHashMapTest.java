package de.tum.i13.server.kv;

import org.junit.jupiter.api.Test;

import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashMapTest {

    @Test
    void testConsistentHashMap() throws NoSuchAlgorithmException {
        ConsistentHashMap consistentHashMap = new ConsistentHashMap();
        String ip1 = "192.168.1.1"; // 66efff4c945d3c3b87fc271b47d456db
        String ip2 = "192.168.1.2"; // 8a120ff3e2c86713f4d346d20f763ee7
        String ip3 = "192.168.1.3"; // cc9d4028d80b7d9c2242cf5fc8cb25f2

        String key = "key"; // 3c6e0b8a9c15224a8228b9a98ca1531d

        consistentHashMap.put(ip1);
        consistentHashMap.put(ip2);
        consistentHashMap.put(ip3);

        assertEquals(ip1, consistentHashMap.get(key));

        String ip6 = "192.168.1.6"; // 428e996031cc52e6948e992887dd9330
        consistentHashMap.put(ip6);

        assertEquals(ip6, consistentHashMap.get(key));

        consistentHashMap.remove(ip6);
        consistentHashMap.remove(ip1);

        assertEquals(ip2, consistentHashMap.get(key));
    }
}