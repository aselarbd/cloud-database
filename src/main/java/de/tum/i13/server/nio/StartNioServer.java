package de.tum.i13.server.nio;

import de.tum.i13.server.kv.CacheBuilder;
import de.tum.i13.server.kv.KVCache;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.kv.stores.LSMStore;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Config;

import java.io.IOException;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

public class StartNioServer {

    public static Logger logger = Logger.getLogger(StartNioServer.class.getName());

    public static void main(String[] args) throws IOException {
        Config cfg = parseCommandlineArgs(args);  //Do not change this
        setupLogging(cfg.logfile, cfg.loglevel);
        logger.info("Config: " + cfg.toString());

        logger.info("starting server");

        KVCache cache = CacheBuilder.newBuilder()
                .size(cfg.cachesize)
                .algorithm(CacheBuilder.Algorithm.valueOf(cfg.cachedisplacement))
                .build();

        KVStore store = new LSMStore(cfg.dataDir);

        CommandProcessor kvCommandProcessor = new KVCommandProcessor(cache, store);

        NioServer sn = new NioServer(kvCommandProcessor);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Closing NioServer");
            sn.close();
        }));

        sn.bindSockets(cfg.listenaddr, cfg.port);
        System.out.println("KV Server started");
        sn.start();
    }
}
