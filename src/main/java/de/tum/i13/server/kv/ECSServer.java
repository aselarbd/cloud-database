package de.tum.i13.server.kv;

import de.tum.i13.kvtp2.KVTP2Server;
import de.tum.i13.server.kv.handlers.ecs.Keyrange;
import de.tum.i13.server.kv.handlers.ecs.SetLockHandler;
import de.tum.i13.shared.Config;

import java.io.IOException;

public class ECSServer {

    private Config config;
    private KVTP2Server ecsServer;
    private KVServer kvServer;

    private SetLockHandler setLockHandlerHandler;

    public ECSServer(Config config, KVServer kvServer) throws IOException {
        ecsServer = new KVTP2Server();
        this.config = config;
        this.kvServer = kvServer;

        setLockHandlerHandler = new SetLockHandler();
        ecsServer.handle("lock", setLockHandlerHandler);
        ecsServer.handle("keyrange", new Keyrange());

//        ecsServer.handle("put", new Put());
    }

    public int getPort() {
        return ecsServer.getLocalPort();
    }

    public void start() throws IOException {
        ecsServer.start(config.listenaddr, 0);
    }
}
