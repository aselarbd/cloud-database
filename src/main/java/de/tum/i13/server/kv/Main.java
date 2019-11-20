package de.tum.i13.server.kv;

import de.tum.i13.kvtp.CommandProcessor;
import de.tum.i13.kvtp.Server;
import de.tum.i13.server.kv.stores.LSMStore;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.InetSocketAddressTypeConverter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

import static de.tum.i13.shared.Config.parseCommandlineArgs;
import static de.tum.i13.shared.LogSetup.setupLogging;

public class Main {
    public static Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        Config cfg = parseCommandlineArgs(args);  //Do not change this
        setupLogging(cfg.logfile, cfg.loglevel);

        logger.info("Starting KV Server");
        logger.info("Config: " + cfg.toString());

        Server server = new Server();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Closing NioServer");
            server.close();
        }));

        KVStore store = new LSMStore(cfg.dataDir);
        KVCache cache = CacheBuilder.newBuilder()
                .size(cfg.cachesize)
                .algorithm(CacheBuilder.Algorithm.valueOf(cfg.cachedisplacement))
                .build();

        InetSocketAddress isa = new InetSocketAddress(cfg.listenaddr, cfg.port);
        KVCommandProcessor kvCommandProcessor = new KVCommandProcessor(isa, cache, store);
        server.bindSockets(cfg.listenaddr, cfg.port, kvCommandProcessor);


        ECSClientProcessor ecsClientProcessor = new ECSClientProcessor(server, cfg.bootstrap, kvCommandProcessor);
        server.connectTo(cfg.bootstrap, ecsClientProcessor);
        ecsClientProcessor.register();


        server.start();
    }
}
