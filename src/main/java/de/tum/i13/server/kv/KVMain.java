package de.tum.i13.server.kv;

import de.tum.i13.kvtp.Server;
import de.tum.i13.server.kv.stores.LSMStore;
import de.tum.i13.shared.Config;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

public class KVMain {
    public static Logger logger = Logger.getLogger(KVMain.class.getName());

    private Server server;
    private ECSClientProcessor ecsClientProcessor;
    private KVCommandProcessor kvCommandProcessor;

    public KVMain(String[] args) throws IOException, InterruptedException {
        Config cfg = parseCommandlineArgs(args);  //Do not change this
        setupLogging(cfg.logfile, cfg.loglevel);

        logger.info("Starting KV Server");
        logger.info("Config: " + cfg.toString());

        server = new Server();


        KVStore store = new LSMStore(cfg.dataDir);
        KVCache cache = CacheBuilder.newBuilder()
                .size(cfg.cachesize)
                .algorithm(CacheBuilder.Algorithm.valueOf(cfg.cachedisplacement))
                .build();

        InetSocketAddress isa = new InetSocketAddress(cfg.listenaddr, cfg.port);
        kvCommandProcessor = new KVCommandProcessor(isa, cache, store);
        server.bindSockets(cfg.listenaddr, cfg.port, kvCommandProcessor);

        ecsClientProcessor = new ECSClientProcessor(server, cfg.bootstrap, kvCommandProcessor);

        boolean connected = false;
        while (!connected) {
            try {
                server.connectTo(cfg.bootstrap, ecsClientProcessor);
            } catch (Exception e) {
                logger.warning("Could not connect to ecs, retrying");
                Thread.sleep(3000);
                continue;
            }
            connected = true;
        }
    }

    public void run() throws IOException {
        ecsClientProcessor.register();

        server.start();
    }

    public void shutdown() {
        System.out.println("Closing NioServer");
        Future shutdown = ecsClientProcessor.shutdown(server::close);
        while (!shutdown.isDone()) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                logger.info("interrupted while waiting for shutdown");
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        KVMain main = new KVMain(args);
        Runtime.getRuntime().addShutdownHook(new Thread(main::shutdown));
        main.run();
    }
}
