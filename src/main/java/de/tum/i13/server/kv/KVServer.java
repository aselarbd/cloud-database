package de.tum.i13.server.kv;

import de.tum.i13.kvtp2.KVTP2Server;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.NonBlockingKVTP2Client;
import de.tum.i13.server.kv.handlers.kv.*;
import de.tum.i13.server.kv.stores.LSMStore;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.HeartbeatListener;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

public class KVServer {

    public static Logger logger = Logger.getLogger(KVServer.class.getName());

    private final KVTP2Server kvtp2Server;
    private final ServerWriteLockHandler serverWriteLockHandler;
    private final Config config;

    private final KeyRange keyRangeHandler;
    private ServerStoppedHandler serverStoppedHandlerWrapper;

    public KVServer(Config cfg) throws IOException, ExecutionException, InterruptedException {
        this.config = cfg;
        kvtp2Server = new KVTP2Server();
        kvtp2Server.setDecoder(new NullDecoder());
        kvtp2Server.setEncoder(new NullEncoder());

        KVStore store = new LSMStore(cfg.dataDir);
        KVCache cache = CacheBuilder.newBuilder()
                .size(cfg.cachesize)
                .algorithm(CacheBuilder.Algorithm.valueOf(cfg.cachedisplacement))
                .build();

        HeartbeatListener heartbeatListener = new HeartbeatListener();
        heartbeatListener.start(cfg.port, new InetSocketAddress(cfg.listenaddr, cfg.port).getAddress());

        serverStoppedHandlerWrapper = new ServerStoppedHandler();
        serverWriteLockHandler = new ServerWriteLockHandler();

        keyRangeHandler = new KeyRange();

        ResponsibilityHandler responsibilityHandler =
                new ResponsibilityHandler(
                        keyRangeHandler,
                        new InetSocketAddress(cfg.listenaddr, cfg.port)
                );

        kvtp2Server.handle(
                "get",
                serverStoppedHandlerWrapper.wrap(
                        serverWriteLockHandler.wrap(
                            responsibilityHandler.wrap(new Get(cache, store))
                        )
                )
        );

        kvtp2Server.handle(
                "put",
                serverStoppedHandlerWrapper.wrap(
                        serverWriteLockHandler.wrap(
                            responsibilityHandler.wrap(new Put(cache, store))
                        )
                )
        );

        kvtp2Server.handle(
                "delete",
                serverStoppedHandlerWrapper.wrap(
                        serverWriteLockHandler.wrap(
                            responsibilityHandler.wrap(new Delete(cache, store))
                        )
                )
        );

        kvtp2Server.handle(
            "keyrange",
            serverStoppedHandlerWrapper.wrap(
                    keyRangeHandler
            )
        );
    }

    public void register() throws InterruptedException, ExecutionException, IOException {
        NonBlockingKVTP2Client ecsClient = new NonBlockingKVTP2Client();
        Thread ecsClientThread = new Thread(() -> {
            try {
                ecsClient.start();
            } catch (IOException e) {
                logger.warning("could not start ecs client");
            }
        });
        Future<Boolean> connected = ecsClient.connect(config.bootstrap.getHostString(), config.bootstrap.getPort());
        ecsClientThread.start();

        Message registerMsg = new Message("register");
        registerMsg.put("kvip", config.listenaddr);
        registerMsg.put("kvport", "" + config.port);
        registerMsg.put("ecsip", config.listenaddr);
        registerMsg.put("ecsport", "" + config.port);

        connected.get();
        ecsClient.send(registerMsg, (w, m) -> {
            String keyrangeString = m.get("keyrange");
            setKeyRange(ConsistentHashMap.fromKeyrangeString(keyrangeString));
            serverStoppedHandlerWrapper.setServerStopped(false);
            try {
                w.close();
            } catch (IOException e) {
                logger.warning(e.getMessage());
            }
            logger.info("successfully registered new kvServer at ecs");
        });
    }

    private void setKeyRange(ConsistentHashMap keyRange) {
        this.keyRangeHandler.setKeyRange(keyRange);
    }

    public void start() throws IOException {
        kvtp2Server.start(config.listenaddr, config.port);
    }

}
