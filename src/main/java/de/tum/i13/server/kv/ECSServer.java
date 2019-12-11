package de.tum.i13.server.kv;

import de.tum.i13.kvtp2.KVTP2Server;
import de.tum.i13.kvtp2.middleware.LogRequest;
import de.tum.i13.server.kv.handlers.ecs.KeyRange;
import de.tum.i13.server.kv.handlers.ecs.Put;
import de.tum.i13.server.kv.handlers.ecs.SetLockHandler;
import de.tum.i13.shared.Config;

import java.io.IOException;
import java.util.logging.Logger;

public class ECSServer {

    public static Logger logger = Logger.getLogger(ECSServer.class.getName());

    private Config config;
    private KVTP2Server ecsServer;

    public ECSServer(Config config, KVServer kvServer) throws IOException {
        ecsServer = new KVTP2Server();
        this.config = config;

        SetLockHandler setLockHandlerHandler = new SetLockHandler(kvServer);
        ecsServer.handle(
                "lock",
                new LogRequest(logger).wrap(
                        setLockHandlerHandler
                )
        );

        ecsServer.handle(
                "keyrange",
                new LogRequest(logger).wrap(
                        new KeyRange(kvServer)
                )
        );

        ecsServer.handle(
                "put",
                new LogRequest(logger).wrap(
                        new Put(kvServer)
                )
        );
    }

    public String getLocalAddress() {
        return this.config.listenaddr;
    }

    public int getLocalPort() {
        return ecsServer.getLocalPort();
    }

    public void start() throws IOException {
        ecsServer.start(config.listenaddr, 0);
    }
}
