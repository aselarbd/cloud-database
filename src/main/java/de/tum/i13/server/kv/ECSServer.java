package de.tum.i13.server.kv;

import de.tum.i13.kvtp2.KVTP2Server;
import de.tum.i13.kvtp2.middleware.DefaultError;
import de.tum.i13.kvtp2.middleware.LogRequest;
import de.tum.i13.server.kv.handlers.ecs.*;
import de.tum.i13.shared.Config;

import java.io.IOException;
import java.util.logging.Logger;

public class ECSServer {

    public static final Logger logger = Logger.getLogger(ECSServer.class.getName());

    private Config config;
    private KVTP2Server ecsServer;

    public ECSServer(Config config, KVServer kvServer) throws IOException {
        ecsServer = new KVTP2Server();
        this.config = config;

        SetLockHandler setLockHandlerHandler = new SetLockHandler(kvServer);
        ecsServer.handle(
                "lock",
                new LogRequest(logger).next(
                        setLockHandlerHandler
                )
        );

        ecsServer.handle(
                "keyrange",
                new LogRequest(logger).next(
                        new KeyRange(kvServer)
                )
        );

        ecsServer.handle(
                "shutdown_keyrange",
                new LogRequest(logger).next(
                        new ShutdownKeyRange(kvServer)
                )
        );

        ecsServer.handle(
                "put",
                new LogRequest(logger).next(
                        new Put(kvServer)
                )
        );

        ecsServer.handle(
                "delete",
                new LogRequest(logger).next(
                        new Delete(kvServer)
                )
        );

        ecsServer.setDefaultHandler(new DefaultError());
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

    public void stop() throws IOException {
        ecsServer.shutdown();
    }
}
