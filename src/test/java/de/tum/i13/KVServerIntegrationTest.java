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
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KVServerIntegrationTest {
    // use different ports than tests for EchoServer
    public static Integer kvPort = 5154;
    public static Integer ecsPort = 5150;

    private Thread startECS() throws InterruptedException {
        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    // start server
                    ECSMain.main(new String[]{"-p", ecsPort.toString()});
                    // wait until interrupted
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            // all fine
                        }
                    }
                    // shutdown server
                    Runtime.getRuntime().exit(0);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        th.start(); // started the server
        Thread.sleep(2000);
        return th;
    }

    private Thread startKVServer(String folder) throws InterruptedException {
        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    // start server
                    KVMain.main(new String[]{"-p", kvPort.toString(), "-d", folder,
                            "-b", "127.0.0.1:" + ecsPort.toString()});
                    // wait until interrupted
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            // all fine
                        }
                    }
                    // shutdown server
                    Runtime.getRuntime().exit(0);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        th.start(); // started the server
        Thread.sleep(2000);
        return th;
    }

    @Test
    public void putAndGet(@TempDir Path tmpDir) throws InterruptedException, IOException {
        Thread ecsThread = startECS();
        Thread kvThread = startKVServer(tmpDir.toString());

        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", kvPort));
        String welcome = RequestUtils.readMessage(s);
        assertTrue(welcome.contains("connected"));

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
