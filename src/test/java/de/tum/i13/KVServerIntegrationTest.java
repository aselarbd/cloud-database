package de.tum.i13;

import de.tum.i13.server.ecs.ECSMain;
import de.tum.i13.server.kv.KVMain;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KVServerIntegrationTest {
    public static Integer kvPort = 5144;
    public static Integer ecsPort = 5140;

    static Thread ecsThread, kvThread;
    static Socket s;

    @BeforeAll
    public static void startup(@TempDir Path tmpDir) throws InterruptedException, IOException {
        ecsThread = IntegrationTestHelpers.startECS(ecsPort);
        kvThread = IntegrationTestHelpers.startKVServer(tmpDir.toString(), kvPort, ecsPort, 1);

        s = IntegrationTestHelpers.connectToTestSvr(kvPort);
    }

    @AfterAll
    public static void shutdown() throws InterruptedException, IOException {
        s.close();
        kvThread.interrupt();
        ecsThread.interrupt();
        kvThread.join(2 * IntegrationTestHelpers.EXIT_WAIT);
        ecsThread.join(2 * IntegrationTestHelpers.EXIT_WAIT);
    }

    @Test
    public void putAndGet() throws IOException {
        String res;
        res = RequestUtils.doRequest(s, "put key some  value");
        assertEquals("put_success key", res);

        res = RequestUtils.doRequest(s, "get key");
        assertEquals("get_success key some  value", res);
    }

    @Test
    public void invalidCommands(@TempDir Path tmpDir) throws InterruptedException, IOException {
        String res;
        res = RequestUtils.doRequest(s, "bogus request 12");
        assertEquals("unknown command", res);

        // TODO: kvtp does not seem to reply on that. Comment out for kvtp2
        //res = RequestUtils.doRequest(s, "");
        //assertEquals("unknown command", res);

        res = RequestUtils.doRequest(s, "get");
        assertEquals("key needed", res);

        res = RequestUtils.doRequest(s, "put");
        assertEquals("key needed", res);

        res = RequestUtils.doRequest(s, "delete");
        assertEquals("key needed", res);
    }
}
