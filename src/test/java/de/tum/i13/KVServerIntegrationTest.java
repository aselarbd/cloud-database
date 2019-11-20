package de.tum.i13;

import de.tum.i13.server.kv.Main;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KVServerIntegrationTest {
    // use a different port than tests for EchoServer
    public static Integer port = 5154;

    private Thread startServer(String folder) throws InterruptedException {
        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    // start server
                    Main.main(new String[]{"-p", port.toString(), "-d", folder});
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
                } catch (IOException | NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        };
        th.start(); // started the server
        Thread.sleep(2000);
        return th;
    }

    @Disabled
    @Test
    public void putAndGet(@TempDir Path tmpDir) throws InterruptedException, IOException {
        Thread th = startServer(tmpDir.toString());

        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", port));
        String welcome = RequestUtils.readMessage(s);
        assertTrue(welcome.contains("connected"));

        String res;
        res = RequestUtils.doRequest(s, "put key some  value");
        assertEquals("put_success key some  value", res);

        res = RequestUtils.doRequest(s, "get key");
        assertEquals("get_success key some  value", res);

        s.close();
        th.interrupt();
    }
}
