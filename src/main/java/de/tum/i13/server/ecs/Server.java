package de.tum.i13.server.ecs;

import de.tum.i13.kvtp2.KVTP2Server;
import de.tum.i13.kvtp2.middleware.LogRequest;
import de.tum.i13.server.ecs.handlers.AddressConverter;
import de.tum.i13.server.ecs.handlers.Finish;
import de.tum.i13.server.ecs.handlers.Register;
import de.tum.i13.server.ecs.handlers.Shutdown;

import java.io.IOException;
import java.util.logging.Logger;

public class Server {

    public static Logger logger = Logger.getLogger(Server.class.getName());

    private final String address;
    private final int port;
    private ServerStateMap ssm;
    private KVTP2Server kvtp2Server;

    public Server(String address, int port) throws IOException {
        this.address = address;
        this.port = port;
        this.ssm = new ServerStateMap();

        kvtp2Server = new KVTP2Server();

        kvtp2Server.handle(
                "register",
                new LogRequest(logger).wrap(
                new Register(ssm)
                )
        );
        kvtp2Server.handle(
                "shutdown",
                new LogRequest(logger).wrap(
                new Shutdown(ssm)
                )
        );
        kvtp2Server.handle(
                "finish",
                new LogRequest(logger).wrap(
                new Finish(ssm)
                )
        );

        AddressConverter addressConverter = new AddressConverter(ssm);
        kvtp2Server.handle(
                "ecs_to_kv",
                new LogRequest(logger).wrap(
                        addressConverter
                )
        );
        kvtp2Server.handle(
                "kv_to_ecs",
                new LogRequest(logger).wrap(
                        addressConverter
                )
        );
    }

    public void start() throws IOException {
        kvtp2Server.start(address, port);
    }

    public void close() {
        // TODO: implement:
        // kvtp2Server.close();
    }

}
