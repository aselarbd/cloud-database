package de.tum.i13.server.kv;

import de.tum.i13.kvtp2.*;
import de.tum.i13.kvtp2.middleware.DefaultError;
import de.tum.i13.kvtp2.middleware.LogRequest;
import de.tum.i13.server.kv.handlers.kv.*;
import de.tum.i13.server.kv.stores.LSMStore;
import de.tum.i13.shared.Config;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.HeartbeatListener;
import de.tum.i13.shared.KVItem;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.logging.Logger;

public class KVServer {

    public static final Logger logger = Logger.getLogger(KVServer.class.getName());

    private final KVTP2Server kvtp2Server;
    private KVStore kvStore;
    private final Config config;

    private final InetSocketAddress address;

    private final KeyRange keyRangeHandler;
    private final KeyRangeRead keyRangeReadHandler;
    private ServerStoppedHandler serverStoppedHandlerWrapper;
    private final ServerWriteLockHandler serverWriteLockHandler;
    private NonBlockingKVTP2Client ecsClient;
    private ECSServer controlAPIServer;

    public KVServer(Config cfg) throws IOException {
        this.config = cfg;
        this.address = new InetSocketAddress(cfg.listenaddr, cfg.port);
        kvtp2Server = new KVTP2Server();
        kvtp2Server.setDecoder(new NullDecoder());
        kvtp2Server.setEncoder(new NullEncoder());

        kvStore = new LSMStore(cfg.dataDir);
        KVCache cache = CacheBuilder.newBuilder()
                .size(cfg.cachesize)
                .algorithm(CacheBuilder.Algorithm.valueOf(cfg.cachedisplacement))
                .build();

        serverStoppedHandlerWrapper = new ServerStoppedHandler();
        serverWriteLockHandler = new ServerWriteLockHandler();

        keyRangeHandler = new KeyRange();
        keyRangeReadHandler = new KeyRangeRead();

        ResponsibilityHandler responsibilityHandler =
                new ResponsibilityHandler(
                        keyRangeReadHandler,
                        new InetSocketAddress(cfg.listenaddr, cfg.port)
                );

        kvtp2Server.handle(
                "get",
                new LogRequest(logger).wrap(
                serverStoppedHandlerWrapper.wrap(
                serverWriteLockHandler.wrap(
                responsibilityHandler.wrap(
                        new Get(cache, kvStore)
                ))))
        );

        kvtp2Server.handle(
                "put",
                new LogRequest(logger).wrap(
                serverStoppedHandlerWrapper.wrap(
                serverWriteLockHandler.wrap(
                responsibilityHandler.wrap(
                        new Put(cache, kvStore)
                ))))
        );

        kvtp2Server.handle(
                "delete",
                new LogRequest(logger).wrap(
                serverStoppedHandlerWrapper.wrap(
                serverWriteLockHandler.wrap(
                responsibilityHandler.wrap(
                        new Delete(cache, kvStore)
                ))))
        );

        kvtp2Server.handle(
            "keyrange",
            new LogRequest(logger).wrap(
            serverStoppedHandlerWrapper.wrap(
            keyRangeHandler
            ))
        );

        kvtp2Server.handle(
            "keyrange_read",
            new LogRequest(logger).wrap(
            serverStoppedHandlerWrapper.wrap(
            keyRangeReadHandler
            ))
        );

        kvtp2Server.handle(
                "connected",
                new LogRequest(logger).wrap(
                        MessageWriter::write)
        );

        kvtp2Server.setDefaultHandler(new DefaultError());
    }

    public void register(ECSServer controlAPIServer) throws InterruptedException, ExecutionException, IOException {
        this.controlAPIServer = controlAPIServer;
        ecsClient = new NonBlockingKVTP2Client();
        ecsClient.setDefaultHandler(new DefaultError());

        while (!ecsClient.isConnected()) {
            logger.info("waiting for ecs connection...");
            Future<Boolean> connected = ecsClient.connect(config.bootstrap.getHostString(), config.bootstrap.getPort());
            Executors.newSingleThreadExecutor().submit(() -> {
                try {
                    ecsClient.start();
                } catch (IOException e) {
                    logger.warning("could not start ecs client");
                }
            });
            Thread.sleep(1000);
            connected.get();
        }

        Message registerMsg = new Message("register");
        registerMsg.put("kvip", config.listenaddr);
        registerMsg.put("kvport", Integer.toString(config.port));
        registerMsg.put("ecsip", controlAPIServer.getLocalAddress());
        registerMsg.put("ecsport", Integer.toString(controlAPIServer.getLocalPort()));

        HeartbeatListener heartbeatListener = new HeartbeatListener();
        heartbeatListener.start(config.port, new InetSocketAddress(config.listenaddr, config.port).getAddress());

        ecsClient.send(registerMsg, (w, m) -> {
            String keyRangeString = m.get("keyrange");
            setKeyRange(ConsistentHashMap.fromKeyrangeString(keyRangeString));
            serverStoppedHandlerWrapper.setServerStopped(false);
            try {
                w.close();
            } catch (IOException e) {
                logger.warning(e.getMessage());
            }
            logger.info("successfully registered new kvServer at ecs");
        });
    }

    public KVTP2Client getBlockingECSClient() throws IOException {
        KVTP2Client blockingECSClient = new KVTP2Client(config.bootstrap.getHostString(), config.bootstrap.getPort());
        blockingECSClient.connect();
        return blockingECSClient;
    }

    public void setKeyRange(ConsistentHashMap keyRange) {
        this.keyRangeReadHandler.setKeyRangeRead(keyRange.getInstanceWithReplica());
        this.keyRangeHandler.setKeyRange(keyRange);
    }

    public ConsistentHashMap getKeyRange() {
        return this.keyRangeHandler.getKeyRange();
    }

    public InetSocketAddress getAddress() {
        return this.address;
    }

    public Set<String> getAllKeys(Predicate<String> predicate) {
        try {
            return kvStore.getAllKeys(predicate);
        } catch (IOException e) {
            logger.warning("Could not read all keys for predicate from disk");
            return null;
        }
    }

    public void put(KVItem kvItem) throws IOException {
        kvStore.put(kvItem);
    }

    public void start() throws IOException {
        kvtp2Server.start(config.listenaddr, config.port);
    }

    public void stop() throws IOException {
        logger.info("sending shutdown announcement");
        Message shutdownMsg = new Message("announce_shutdown");
        shutdownMsg.put("ecsip", controlAPIServer.getLocalAddress());
        shutdownMsg.put("ecsport", Integer.toString(controlAPIServer.getLocalPort()));
        KVTP2Client blockingECSClient = getBlockingECSClient();
        Message send = blockingECSClient.send(shutdownMsg);
        logger.info("shutdown announcement response: " + send.toString());
    }

    public KVItem getItem(String key) throws IOException {
        return kvStore.get(key);
    }

    public void setLocked(boolean locked) {
        this.serverWriteLockHandler.setLocked(locked);
    }

    public void setStopped(boolean stopped) {
        this.serverStoppedHandlerWrapper.setServerStopped(stopped);
    }

    public InetSocketAddress getControlAPIServerAddress() {
        return new InetSocketAddress(
                this.controlAPIServer.getLocalAddress(),
                this.controlAPIServer.getLocalPort()
        );
    }

    public boolean stopped() {
        return serverStoppedHandlerWrapper.getServerStopped();
    }
}
