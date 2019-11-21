package de.tum.i13.shared;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.*;

class ECSUtilsTest {

    ConsistentHashMap consistentHashMap = new ConsistentHashMap();

    ECSUtilsTest() throws NoSuchAlgorithmException {
    }

    @Test
    void rebalancedServerRangesRemove() {

        InetSocketAddress ip1 = new InetSocketAddress("192.168.1.1", 80); // be8e4f546de43337d7f0d4637a796478
        InetSocketAddress ip2 = new InetSocketAddress("192.168.1.2", 80); // c8088b91cb0f2fbcbdf107bd31e3d195
        InetSocketAddress ip3 = new InetSocketAddress("192.168.1.3", 80); // 0da0828d3687114976e0edb80e0c54d5

        consistentHashMap.put(ip1);
        consistentHashMap.put(ip2);
        consistentHashMap.put(ip3);

        ECSUtils ecsUtils =  new ECSUtils();
        ecsUtils.rebalancedServerRangesRemove(ip1,consistentHashMap);
    }

    @Test
    void rebalancedServerRangesAdd() {


        InetSocketAddress ip1 = new InetSocketAddress("192.168.1.1", 80); // be8e4f546de43337d7f0d4637a796478
        InetSocketAddress ip2 = new InetSocketAddress("192.168.1.2", 80); // c8088b91cb0f2fbcbdf107bd31e3d195
        InetSocketAddress ip3 = new InetSocketAddress("192.168.1.3", 80); // 0da0828d3687114976e0edb80e0c54d5

        consistentHashMap.put(ip1);
        consistentHashMap.put(ip2);

        ECSUtils ecsUtils =  new ECSUtils();
        ecsUtils.rebalancedServerRangesAdd(ip3,consistentHashMap);
    }
}