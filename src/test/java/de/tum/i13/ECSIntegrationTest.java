package de.tum.i13;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ECSIntegrationTest {
    public static Integer kv1Port = 5154;
    public static Integer kv2Port = 5155;
    public static Integer kv3Port = 5156;
    public static Integer ecsPort = 5150;

    @Test
    public void shutdownOneServer(@TempDir Path tmpDir1, @TempDir Path tmpDir2, @TempDir Path tmpDir3) throws InterruptedException, IOException {
        Thread ecsThread = IntegrationTestHelpers.startECS(ecsPort);
        Thread kvThread1 = IntegrationTestHelpers.startKVServer(tmpDir1.toString(), kv1Port, ecsPort);
        Thread kvThread2 = IntegrationTestHelpers.startKVServer(tmpDir2.toString(), kv2Port, ecsPort);
        Thread kvThread3 = IntegrationTestHelpers.startKVServer(tmpDir3.toString(), kv3Port, ecsPort);

        // start with server 1, which is not responsible for the test value
        Socket s = IntegrationTestHelpers.connectToTestSvr(kv1Port);
        String res;

        res = RequestUtils.doRequest(s, "put 127.0.0.1:5155 test val");
        assertEquals("server_not_responsible", res);

        res = RequestUtils.doRequest(s, "get 127.0.0.1:5155");
        assertEquals("server_not_responsible", res);
        s.close();

        // now use the server which actually is responsible
        s = IntegrationTestHelpers.connectToTestSvr(kv2Port);
        res = RequestUtils.doRequest(s, "put 127.0.0.1:5155 test val");
        assertEquals("put_success 127.0.0.1:5155", res);

        res = RequestUtils.doRequest(s, "get 127.0.0.1:5155");
        assertEquals("get_success 127.0.0.1:5155 test val", res);
        s.close();

        // shutdown the server
        kvThread2.interrupt();
        kvThread2.join(2000);
        // wait for rebalance
        Thread.sleep(2000);

        // now use the server which actually is responsible
        s = IntegrationTestHelpers.connectToTestSvr(kv3Port);
        res = RequestUtils.doRequest(s, "get 127.0.0.1:5155");
        assertEquals("get_success 127.0.0.1:5155 test val", res);
        s.close();

        kvThread1.interrupt();
        kvThread3.interrupt();
        ecsThread.interrupt();
    }
}
