package de.tum.i13;

import java.net.InetSocketAddress;

/**
 * Holds static test strings which are used in several test cases.
 */
public class TestConstants {
    public static final String KEYRANGE_SIMPLE = "be8e4f546de43337d7f0d4637a796478," +
            "be8e4f546de43337d7f0d4637a796478,192.168.1.1:80;";

    public static final String KEYRANGE_INVALID_IP = "be8e4f546de43337d7f0d4637a796478," +
            "be8e4f546de43337d7f0d4637a796478,127.0.0.1:80;"; // IP does not match hashes, hence invalid

    public static final String KEYRANGE_EXT = "0da0828d3687114976e0edb80e0c54d5," +
            "be8e4f546de43337d7f0d4637a796478,192.168.1.3:80;" +
            "be8e4f546de43337d7f0d4637a796478,c8088b91cb0f2fbcbdf107bd31e3d195,192.168.1.1:80;" +
            "c8088b91cb0f2fbcbdf107bd31e3d195,0da0828d3687114976e0edb80e0c54d5,192.168.1.2:80;";

    public static final String KEYRANGE_REPLICA_FULL = "0da0828d3687114976e0edb80e0c54d5," +
            "be8e4f546de43337d7f0d4637a796478,192.168.1.3:80;" +
            "0da0828d3687114976e0edb80e0c54d5,be8e4f546de43337d7f0d4637a796478,192.168.1.1:80;" +
            "0da0828d3687114976e0edb80e0c54d5,be8e4f546de43337d7f0d4637a796478,192.168.1.2:80;" +
            "be8e4f546de43337d7f0d4637a796478,c8088b91cb0f2fbcbdf107bd31e3d195,192.168.1.1:80;" +
            "be8e4f546de43337d7f0d4637a796478,c8088b91cb0f2fbcbdf107bd31e3d195,192.168.1.2:80;" +
            "be8e4f546de43337d7f0d4637a796478,c8088b91cb0f2fbcbdf107bd31e3d195,192.168.1.3:80;" +
            "c8088b91cb0f2fbcbdf107bd31e3d195,0da0828d3687114976e0edb80e0c54d5,192.168.1.2:80;" +
            "c8088b91cb0f2fbcbdf107bd31e3d195,0da0828d3687114976e0edb80e0c54d5,192.168.1.3:80;" +
            "c8088b91cb0f2fbcbdf107bd31e3d195,0da0828d3687114976e0edb80e0c54d5,192.168.1.1:80;";

    public static final String KEYRANGE_REPLICA_PART = "0da0828d3687114976e0edb80e0c54d5," +
            "be8e4f546de43337d7f0d4637a796478,192.168.1.3:80;" +
            "0da0828d3687114976e0edb80e0c54d5,be8e4f546de43337d7f0d4637a796478,192.168.1.1:80;" +
            "be8e4f546de43337d7f0d4637a796478,c8088b91cb0f2fbcbdf107bd31e3d195,192.168.1.1:80;" +
            "c8088b91cb0f2fbcbdf107bd31e3d195,0da0828d3687114976e0edb80e0c54d5,192.168.1.2:80;" +
            "c8088b91cb0f2fbcbdf107bd31e3d195,0da0828d3687114976e0edb80e0c54d5,192.168.1.3:80;" +
            "c8088b91cb0f2fbcbdf107bd31e3d195,0da0828d3687114976e0edb80e0c54d5,192.168.1.1:80;";

    public static final String KEYRANGE_REPLICA_UNORDERED = "0da0828d3687114976e0edb80e0c54d5," +
            "be8e4f546de43337d7f0d4637a796478,192.168.1.3:80;" +
            "c8088b91cb0f2fbcbdf107bd31e3d195,0da0828d3687114976e0edb80e0c54d5,192.168.1.3:80;" +
            "0da0828d3687114976e0edb80e0c54d5,be8e4f546de43337d7f0d4637a796478,192.168.1.1:80;" +
            "be8e4f546de43337d7f0d4637a796478,c8088b91cb0f2fbcbdf107bd31e3d195,192.168.1.3:80;" +
            "c8088b91cb0f2fbcbdf107bd31e3d195,0da0828d3687114976e0edb80e0c54d5,192.168.1.2:80;" +
            "0da0828d3687114976e0edb80e0c54d5,be8e4f546de43337d7f0d4637a796478,192.168.1.2:80;" +
            "be8e4f546de43337d7f0d4637a796478,c8088b91cb0f2fbcbdf107bd31e3d195,192.168.1.1:80;" +
            "be8e4f546de43337d7f0d4637a796478,c8088b91cb0f2fbcbdf107bd31e3d195,192.168.1.2:80;" +
            "c8088b91cb0f2fbcbdf107bd31e3d195,0da0828d3687114976e0edb80e0c54d5,192.168.1.1:80;";

    public static final InetSocketAddress IP_1 = new InetSocketAddress("192.168.1.1", 80); // be8e4f546de43337d7f0d4637a796478
    public static final InetSocketAddress IP_2 = new InetSocketAddress("192.168.1.2", 80); // c8088b91cb0f2fbcbdf107bd31e3d195
    public static final InetSocketAddress IP_3 = new InetSocketAddress("192.168.1.3", 80); // 0da0828d3687114976e0edb80e0c54d5

}
