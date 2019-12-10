package de.tum.i13.server.kv;

import de.tum.i13.kvtp2.KVTP2Server;
import de.tum.i13.server.kv.handlers.ecs.KeyRange;
import de.tum.i13.server.kv.handlers.ecs.Put;
import de.tum.i13.server.kv.handlers.ecs.SetLockHandler;
import de.tum.i13.shared.Config;

import java.io.IOException;

public class ECSServer {

    private Config config;
    private KVTP2Server ecsServer;

    public ECSServer(Config config, KVServer kvServer) throws IOException {
        ecsServer = new KVTP2Server();
        this.config = config;

        SetLockHandler setLockHandlerHandler = new SetLockHandler();
        ecsServer.handle("lock", setLockHandlerHandler);
        ecsServer.handle("keyrange", new KeyRange(kvServer));
        ecsServer.handle("put", new Put(kvServer));
    }

    public int getPort() {
        return ecsServer.getLocalPort();
    }

    public void start() throws IOException {
        ecsServer.start(config.listenaddr, 0);
    }
}
