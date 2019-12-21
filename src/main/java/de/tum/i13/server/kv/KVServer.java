package de.tum.i13.server.kv;

import de.tum.i13.kvtp2.*;
import de.tum.i13.kvtp2.middleware.LogRequest;
import de.tum.i13.server.kv.handlers.kv.*;
import de.tum.i13.server.kv.replication.Replicator;
import de.tum.i13.server.kv.stores.LSMStore;
import de.tum.i13.shared.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.logging.Logger;

public class KVServer {

    public static final Logger logger = Logger.getLogger(KVServer.class.getName());

    private final KVTP2Server kvtp2Server;
    private KVStore kvStore;
    private KVCache kvCache;
    private Replicator replicator;
    private final Config config;

    private InetSocketAddress address;

    private final KeyRange keyRangeHandler;
    private final KeyRangeRead keyRangeReadHandler;
    private final ResponsibilityHandler responsibilityHandler;
    private ServerStoppedHandler serverStoppedHandlerWrapper;
    private final ServerWriteLockHandler serverWriteLockHandler;
    private final ReplicationHandler replicationHandler;
    private final LogLevelHandler logLevelHandler;
    private ECSServer controlAPIServer;

    private KVTP2Client blockingECSClient;

    private ExecutorService shutdownService;

    public KVServer(Config cfg) throws IOException {
        this.config = cfg;
        kvtp2Server = new KVTP2Server();
        kvtp2Server.setDecoder(new NullDecoder());
        kvtp2Server.setEncoder(new NullEncoder());

        kvStore = new LSMStore(cfg.dataDir);
        kvCache = CacheBuilder.newBuilder()
                .size(cfg.cachesize)
                .algorithm(CacheBuilder.Algorithm.valueOf(cfg.cachedisplacement))
                .build();

        this.replicator = new Replicator(
                kvStore
        );

        serverStoppedHandlerWrapper = new ServerStoppedHandler();
        serverWriteLockHandler = new ServerWriteLockHandler();
        replicationHandler = new ReplicationHandler(replicator);

        keyRangeHandler = new KeyRange();
        keyRangeReadHandler = new KeyRangeRead();

        logLevelHandler = new LogLevelHandler();

        responsibilityHandler = new ResponsibilityHandler(
                keyRangeReadHandler
        );

        kvtp2Server.handle(
                "get",
                new LogRequest(logger).wrap(
                serverStoppedHandlerWrapper.wrap(
                responsibilityHandler.wrap(
                        new Get(kvCache, kvStore)
                )))
        );

        kvtp2Server.handle(
                "put",
                new LogRequest(logger).wrap(
                serverStoppedHandlerWrapper.wrap(
                serverWriteLockHandler.wrap(
                responsibilityHandler.wrap(
                replicationHandler.wrap(
                        new Put(this)
                )))))
        );

        kvtp2Server.handle(
                "delete",
                new LogRequest(logger).wrap(
                serverStoppedHandlerWrapper.wrap(
                serverWriteLockHandler.wrap(
                responsibilityHandler.wrap(
                replicationHandler.wrap(
                        new Delete(this)
                )))))
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
                "serverLogLevel",
                new LogRequest(logger).wrap(
                logLevelHandler
                )
        );

        kvtp2Server.handle(
                "connected",
                new LogRequest(logger).wrap(
                        MessageWriter::write)
        );

        kvtp2Server.setDefaultHandler(
            (w, m) -> {
                Message response = Message.getResponse(m);
                response.setCommand("error");
                response.setVersion(Message.Version.V1);
                response.put("msg", "invalid command \"" + m.get("original") + "\"");
                w.write(response);
                w.flush();
            }
        );
    }

    public void register(ECSServer controlAPIServer) throws InterruptedException, SocketException {
        this.controlAPIServer = controlAPIServer;
        KVTP2Client blockingECSClient = null;

        boolean connected = false;
        while (!connected) {
            try {
                blockingECSClient = getBlockingECSClient();
                connected = true;
            } catch (IOException e) {
                logger.warning("could not start ecs client: " + e.getMessage());
                Thread.sleep(3000);
            }
        }

        Message registerMsg = new Message("register");
        registerMsg.put("kvport", Integer.toString(config.port));
        registerMsg.put("ecsport", Integer.toString(controlAPIServer.getLocalPort()));

        HeartbeatListener heartbeatListener = new HeartbeatListener();
        heartbeatListener.start(config.port, new InetSocketAddress(config.listenaddr, config.port).getAddress());

        try {
            Message send = blockingECSClient.send(registerMsg);
            setAddress(new InetSocketAddress(send.get("ip"), config.port));
        } catch (IOException e) {
            logger.warning("failed to send register message to ecs: " + e.getMessage());
        }
    }

    private void setAddress(InetSocketAddress address) {
        this.address = address;
        this.responsibilityHandler.setKvAddress(address);
        this.replicator.setAddress(address);
    }

    public KVTP2Client getBlockingECSClient() throws IOException {
        if (blockingECSClient != null) {
            return blockingECSClient;
        }

        KVTP2Client newClient = new KVTP2Client(config.bootstrap.getHostString(), config.bootstrap.getPort());
        newClient.connect();
        blockingECSClient = newClient;
        return blockingECSClient;
    }

    public void setKeyRange(ConsistentHashMap keyRange) {
        this.keyRangeHandler.setKeyRange(keyRange);
        final ConsistentHashMap replicated = keyRange.getInstanceWithReplica();
        this.keyRangeReadHandler.setKeyRangeRead(replicated);
        try {
            this.replicator.setEcsClient(getBlockingECSClient());
            this.replicator.setReplicaSets(replicated);
        } catch (IOException | InterruptedException e) {
            logger.warning("Failed to update replicator: " + e.getMessage());
        }
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

    public String put(KVItem kvItem, boolean ensureCache) throws IOException {
        final String res = kvStore.put(kvItem);
        if (ensureCache || kvCache.get(kvItem.getKey()) != null) {
            kvCache.put(kvItem);
        }
        return res;
    }

    public boolean delete(String key) throws IOException {
        if (kvStore.get(key) != null) {
            KVItem kvItem = new KVItem(key, Constants.DELETE_MARKER);
            kvStore.put(kvItem);
            kvCache.delete(kvItem.getKey());
            return true;
        }
        return false;
    }

    public void start() throws IOException {
        logger.info("starting kvserver on " + config.listenaddr + ":" + config.port);
        kvtp2Server.start(config.listenaddr, config.port);
    }

    public void stop() throws IOException {
        logger.info("sending shutdown announcement");
        Message shutdownMsg = new Message("announce_shutdown");
        shutdownMsg.put("ecsip", controlAPIServer.getLocalAddress());
        shutdownMsg.put("ecsport", Integer.toString(controlAPIServer.getLocalPort()));
        KVTP2Client blockingECSClient = getBlockingECSClient();

        shutdownService = Executors.newSingleThreadExecutor();
        shutdownService.submit(() -> {
            Message send;
            try {
                send = blockingECSClient.send(shutdownMsg);
            } catch (IOException e) {
                logger.warning("failed to send shutdown message");
                return;
            }
            logger.info("shutdown announcement response: " + send.toString());
        });
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
        boolean serverStopped = serverStoppedHandlerWrapper.getServerStopped();
        if (serverStopped) {
            cleanup();
        }
        return serverStopped;
    }

    private void cleanup() {
        try {
            shutdownService.shutdownNow();
            blockingECSClient.close();
        } catch (IOException e) {
            logger.warning("failed to close ecs client: " + e.getMessage());
        }
    }
}
