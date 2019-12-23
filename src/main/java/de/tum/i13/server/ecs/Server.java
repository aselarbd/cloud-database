package de.tum.i13.server.ecs;

import de.tum.i13.kvtp2.KVTP2Server;
import de.tum.i13.kvtp2.middleware.DefaultError;
import de.tum.i13.kvtp2.middleware.LogRequest;
import de.tum.i13.server.ecs.handlers.AddressConverter;
import de.tum.i13.server.ecs.handlers.Finish;
import de.tum.i13.server.ecs.handlers.Register;
import de.tum.i13.server.ecs.handlers.Shutdown;
import de.tum.i13.shared.Log;

import java.io.IOException;

public class Server {

    public static final Log logger = new Log(Server.class);

    private final String address;
    private final int port;
    private KVTP2Server kvtp2Server;

    public Server(String address, int port) throws IOException {
        this.address = address;
        this.port = port;
        ServerStateMap ssm = new ServerStateMap();

        kvtp2Server = new KVTP2Server();

        kvtp2Server.handle(
                "register",
                new LogRequest(logger).next(
                new Register(ssm)
                )
        );
        kvtp2Server.handle(
                "announce_shutdown",
                new LogRequest(logger).next(
                new Shutdown(ssm)
                )
        );
        kvtp2Server.handle(
                "finish",
                new LogRequest(logger).next(
                new Finish(ssm)
                )
        );

        AddressConverter addressConverter = new AddressConverter(ssm);
        kvtp2Server.handle(
                "ecs_to_kv",
                new LogRequest(logger).next(
                        addressConverter
                )
        );
        kvtp2Server.handle(
                "kv_to_ecs",
                new LogRequest(logger).next(
                        addressConverter
                )
        );

        kvtp2Server.setDefaultHandler(new DefaultError());
    }

    public void start() throws IOException {
        kvtp2Server.start(address, port);
    }

    public void close() throws IOException {
        kvtp2Server.shutdown();
    }

}
