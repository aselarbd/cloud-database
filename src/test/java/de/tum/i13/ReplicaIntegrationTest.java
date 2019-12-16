package de.tum.i13;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReplicaIntegrationTest {
    public static final Integer kv1Port = 5161;
    public static final Integer kv2Port = 5162;
    public static final Integer kv3Port = 5163;
    public static final Integer kv4Port = 5164;
    public static final Integer kv5Port = 5165;
    public static final Integer ecsPort = 5160;
    public static final Integer WAIT_TIME_ITEM_REPL = 2000;
    public static final Integer WAIT_TIME_BALANCE = 2000;

    private void assertReplica(int[] kvPorts, String key, String value) throws IOException {
        for (int port : kvPorts) {
            Socket s = IntegrationTestHelpers.connectToTestSvr(port);
            String res = RequestUtils.doRequest(s, "get " + key);
            s.close();
            assertEquals("get_success " + key + " " + value, res);
        }
    }

    private void assertNotFound(int[] kvPorts, String key) throws IOException {
        for (int port : kvPorts) {
            Socket s = IntegrationTestHelpers.connectToTestSvr(port);
            String res = RequestUtils.doRequest(s, "get " + key);
            s.close();
            assertEquals("get_error " + key + " not found", res);
        }
    }

    @Test
    public void testPutDeleteReplication(@TempDir Path tmpDir1, @TempDir Path tmpDir2, @TempDir Path tmpDir3,
                    @TempDir Path tmpDir4, @TempDir Path tmpDir5) throws InterruptedException, IOException {
        Thread ecsThread = IntegrationTestHelpers.startECS(ecsPort);
        Thread kvThread1 = IntegrationTestHelpers.startKVServer(tmpDir1.toString(), kv1Port, ecsPort, 1);
        Thread kvThread2 = IntegrationTestHelpers.startKVServer(tmpDir2.toString(), kv2Port, ecsPort, 1);
        Thread kvThread3 = IntegrationTestHelpers.startKVServer(tmpDir3.toString(), kv3Port, ecsPort, 1);
        Thread kvThread4 = IntegrationTestHelpers.startKVServer(tmpDir4.toString(), kv4Port, ecsPort, 1);
        Thread kvThread5 = IntegrationTestHelpers.startKVServer(tmpDir5.toString(), kv5Port, ecsPort, 1);
        final String testKey = "127.0.0.1:5161";
        final String testVal = "test val";
        final String testUpdate = "update val";

        Socket s = IntegrationTestHelpers.connectToTestSvr(kv1Port);
        String res;

        res = RequestUtils.doRequest(s, "put " + testKey + " " + testVal);
        assertEquals("put_success " + testKey, res);
        // wait for replication
        Thread.sleep(WAIT_TIME_ITEM_REPL);

        res = RequestUtils.doRequest(s, "get " + testKey);
        assertEquals("get_success " + testKey + " " + testVal, res);
        s.close();

        // replica should be on server 2 and 3
        assertReplica(new int[]{kv2Port, kv3Port}, testKey, testVal);

        // other one should not be responsible
        s = IntegrationTestHelpers.connectToTestSvr(kv5Port);
        res = RequestUtils.doRequest(s, "get " + testKey);
        assertEquals("server_not_responsible", res);
        s.close();

        // shutdown one of the replica servers
        kvThread2.interrupt();
        kvThread2.join(WAIT_TIME_BALANCE);
        // wait for rebalance
        Thread.sleep(WAIT_TIME_BALANCE);

        s = IntegrationTestHelpers.connectToTestSvr(kv1Port);
        res = RequestUtils.doRequest(s, "put " + testKey + " " + testUpdate);
        assertEquals("put_update " + testKey, res);
        Thread.sleep(WAIT_TIME_ITEM_REPL);

        // replica should now be on server 3 and 4
        assertReplica(new int[]{kv3Port, kv4Port}, testKey, testUpdate);

        // still connected with kv1, now delete the value
        res = RequestUtils.doRequest(s, "delete " + testKey);
        assertEquals("delete_success " + testKey, res);
        s.close();

        Thread.sleep(WAIT_TIME_ITEM_REPL);
        assertNotFound(new int[]{kv1Port, kv3Port, kv4Port}, testKey);

        kvThread1.interrupt();
        kvThread3.interrupt();
        kvThread4.interrupt();
        kvThread5.interrupt();
        kvThread1.join(WAIT_TIME_BALANCE);
        kvThread3.join(WAIT_TIME_BALANCE);
        kvThread4.join(WAIT_TIME_BALANCE);
        kvThread5.join(WAIT_TIME_BALANCE);
        ecsThread.interrupt();
        ecsThread.join(WAIT_TIME_BALANCE);
    }
}
