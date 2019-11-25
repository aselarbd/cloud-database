package de.tum.i13;

import de.tum.i13.server.ecs.ECSMain;
import de.tum.i13.server.kv.KVMain;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KVServerIntegrationTest {
    public static Integer kvPort = 5154;
    public static Integer ecsPort = 5150;

    @Test
    public void putAndGet(@TempDir Path tmpDir) throws InterruptedException, IOException {
        Thread ecsThread = IntegrationTestHelpers.startECS(ecsPort);
        Thread kvThread = IntegrationTestHelpers.startKVServer(tmpDir.toString(), kvPort, ecsPort, 1);

        Socket s = IntegrationTestHelpers.connectToTestSvr(kvPort);

        String res;
        res = RequestUtils.doRequest(s, "put key some  value");
        assertEquals("put_success key", res);

        res = RequestUtils.doRequest(s, "get key");
        assertEquals("get_success key some  value", res);

        s.close();
        kvThread.interrupt();
        ecsThread.interrupt();
    }
}
