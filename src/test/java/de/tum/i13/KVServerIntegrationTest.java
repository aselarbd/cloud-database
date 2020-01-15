package de.tum.i13;

import de.tum.i13.shared.TaskRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KVServerIntegrationTest {
    public static final Integer kvPort = 5144;
    public static final Integer ecsPort = 5140;

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
        kvThread.join(2 * IntegrationTestHelpers.EXIT_WAIT);
        ecsThread.interrupt();
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
    public void invalidCommands(@TempDir Path tmpDir) throws IOException {
        String res;
        res = RequestUtils.doRequest(s, "bogus request 12");
        assertEquals("error invalid command \"bogus request 12\"", res);

        res = RequestUtils.doRequest(s, "");
        assertEquals("error invalid command \"\"", res);

        res = RequestUtils.doRequest(s, "get");
        assertEquals("error no key given", res);

        res = RequestUtils.doRequest(s, "put");
        assertEquals("error invalid command \"put\"", res);

        res = RequestUtils.doRequest(s, "delete");
        assertEquals("error no key given", res);
    }
}
