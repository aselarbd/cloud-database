package de.tum.i13.server.kv;

import de.tum.i13.kvtp2.KVTP2Server;
import de.tum.i13.server.kv.handlers.Delete;
import de.tum.i13.server.kv.handlers.Get;
import de.tum.i13.server.kv.handlers.Put;

import java.io.IOException;

public class KVServer {

    private final String address;
    private final int port;
    private final KVTP2Server kvtp2Server;

    public KVServer(String address, int port) throws IOException {
        this.address = address;
        this.port = port;
        kvtp2Server = new KVTP2Server();


        kvtp2Server.handle("get", new Get());
        kvtp2Server.handle("put", new Put());
        kvtp2Server.handle("delete", new Delete());
    }

    public void start() throws IOException {
        kvtp2Server.start(address, port);
    }

}
