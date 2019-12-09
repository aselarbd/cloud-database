package de.tum.i13;

import de.tum.i13.server.ecs.ECSMain;
import de.tum.i13.server.kv.KVMain;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Common helper functions for integration tests
 */
public class IntegrationTestHelpers {
    public static final int ECS_START_WAIT = 500;
    public static final int BASE_START_WAIT = 1000;
    public static final int EXIT_WAIT = 500;

    public static Thread startECS(int ecsPort) throws InterruptedException {
        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    // start server
                    ECSMain main = new ECSMain(new String[]{"-p", Integer.toString(ecsPort)});
                    // run in another thread and periodically check for exit signals
                    Thread server = new Thread(()-> {
                        try {
                            main.run();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                    server.start();
                    // wait until interrupted
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Thread.sleep(EXIT_WAIT);
                        } catch (InterruptedException e) {
                            // all fine
                            break;
                        }
                    }
                    // shutdown server
                    main.shutdown();
                    server.join(EXIT_WAIT);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        th.start(); // started the server
        Thread.sleep(ECS_START_WAIT);
        return th;
    }

    public static Thread startKVServer(String folder, int kvPort,  int ecsPort, int delay) throws InterruptedException {
        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    // start server
                    KVMain main = new KVMain(new String[]{"-p", Integer.toString(kvPort), "-d", folder,
                            "-b", "127.0.0.1:" + ecsPort, "-ll=INFO"});
                    // run in another thread and periodically check for exit signals
                    Thread server = new Thread(() -> {
                        try {
                            main.run();
                        } catch (IOException | InterruptedException | ExecutionException e) {
                            e.printStackTrace();
                        }
                    });
                    server.start();
                    // wait until interrupted
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Thread.sleep(EXIT_WAIT);
                        } catch (InterruptedException e) {
                            // all fine
                            break;
                        }
                    }
                    // shutdown server
                    main.shutdown();
                    server.join(EXIT_WAIT);
                } catch (IOException | InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        };
        th.start(); // started the server
        Thread.sleep(delay * BASE_START_WAIT);
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
