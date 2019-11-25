package de.tum.i13;

import de.tum.i13.server.ecs.ECSMain;
import de.tum.i13.server.kv.KVMain;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Common helper functions for integration tests
 */
public class IntegrationTestHelpers {
    public static final int START_WAIT = 4000;

    public static Thread startECS(int ecsPort) throws InterruptedException {
        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    // start server
                    ECSMain.main(new String[]{"-p", Integer.toString(ecsPort)});
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
        Thread.sleep(START_WAIT);
        return th;
    }

    public static Thread startKVServer(String folder, int kvPort,  int ecsPort) throws InterruptedException {
        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    // start server
                    KVMain.main(new String[]{"-p", Integer.toString(kvPort), "-d", folder,
                            "-b", "127.0.0.1:" + ecsPort, "-ll=INFO"});
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
        Thread.sleep(START_WAIT);
        return th;
    }

    /**
     * Connects to a local test server instance and checks for a welcome message.
     *
     * @param port
     * @return
     * @throws IOException
     */
    public static Socket connectToTestSvr(int port) throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", port));
        String welcome = RequestUtils.readMessage(s);
        assertTrue(welcome.contains("connected"));
        return s;
    }
}
