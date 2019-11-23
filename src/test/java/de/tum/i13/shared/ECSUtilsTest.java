package de.tum.i13.shared;

import de.tum.i13.kvtp.Server;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

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

        Server testSender = mock(Server.class);
        ECSUtils ecsUtils =  new ECSUtils(testSender);
        ecsUtils.rebalancedServerRangesRemove(ip1,consistentHashMap);

        verify(testSender, never()).sendTo(eq(ip1), anyString());
        verify(testSender).sendTo(eq(ip2), anyString());
        verify(testSender).sendTo(eq(ip3), anyString());

        // TODO actually check keyrange
    }

    @Test
    void rebalancedServerRangesAdd() {


        InetSocketAddress ip1 = new InetSocketAddress("192.168.1.1", 80); // be8e4f546de43337d7f0d4637a796478
        InetSocketAddress ip2 = new InetSocketAddress("192.168.1.2", 80); // c8088b91cb0f2fbcbdf107bd31e3d195
        InetSocketAddress ip3 = new InetSocketAddress("192.168.1.3", 80); // 0da0828d3687114976e0edb80e0c54d5

        consistentHashMap.put(ip1);
        consistentHashMap.put(ip2);

        Server testSender = mock(Server.class);
        ECSUtils ecsUtils =  new ECSUtils(testSender);
        ecsUtils.rebalancedServerRangesAdd(ip3,consistentHashMap);

        verify(testSender).sendTo(eq(ip1), anyString());
        verify(testSender).sendTo(eq(ip2), anyString());
        verify(testSender).sendTo(eq(ip3), anyString());

        // TODO actually check keyrange
    }
}