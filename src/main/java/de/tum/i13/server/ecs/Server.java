package de.tum.i13.server.ecs;

import de.tum.i13.kvtp2.KVTP2Server;
import de.tum.i13.server.ecs.handlers.Register;
import de.tum.i13.server.ecs.handlers.Shutdown;

import java.io.IOException;

public class Server {

    private final String address;
    private final int port;
    private ServerStateMap ssm;
    private KVTP2Server kvtp2Server;

    public Server(String address, int port) throws IOException {
        this.address = address;
        this.port = port;
        this.ssm = new ServerStateMap();

        kvtp2Server = new KVTP2Server();

        kvtp2Server.handle("register", new Register(ssm));
        kvtp2Server.handle("shutdown", new Shutdown(ssm));
    }

    public void start() throws IOException {
        kvtp2Server.start(address, port);
    }

    public void close() {
        // TODO: implement:
        // kvtp2Server.close();
    }

}
