package de.tum.i13.server.kv;

import de.tum.i13.shared.Config;
import de.tum.i13.shared.Log;
import de.tum.i13.shared.TaskRunner;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

public class KVMain {
    public static final Log logger = new Log(KVMain.class);

    private final KVServer kvServer;
    private final ECSServer ecsServer;

    public KVMain(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);
        setupLogging(cfg.logfile, cfg.loglevel);

        logger.info("Starting KV Server");
        logger.info("Config: " + cfg.toString());

        this.kvServer = new KVServer(cfg);
        this.ecsServer = new ECSServer(cfg, this.kvServer);
    }

    public void run() throws IOException, ExecutionException, InterruptedException {
        Thread ecsAPI = new Thread(() -> {
            try {
                this.ecsServer.start();
            } catch (IOException e) {
                logger.severe("ecs control API of kvServer crashed", e);
            }
        });
        ecsAPI.start();
        Thread.sleep(3000); // TODO: replace by wait for startup mechanism
        logger.info("started ecs API for kvServer at " + ecsServer.getLocalAddress() + ":" + ecsServer.getLocalPort());
        kvServer.register(this.ecsServer);

        kvServer.start();
    }

    public void shutdown() {
        try {
            logger.info("stopping server");
            kvServer.stop();
            int max = 3;
            int attempts = 0;
            while(!kvServer.stopped()) {
                logger.info("waiting for kvServer shutdown...");
                Thread.sleep(3000);
                attempts++;
            }
            ecsServer.stop();
        } catch (IOException | InterruptedException e) {
            logger.severe("Failed to gracefully shut down servers", e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        KVMain main = new KVMain(args);
        Runtime.getRuntime().addShutdownHook(new Thread(main::shutdown));
        main.run();
    }
}
