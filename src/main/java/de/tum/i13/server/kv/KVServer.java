package de.tum.i13.server.kv;

import de.tum.i13.kvtp2.KVTP2Server;
import de.tum.i13.server.kv.handlers.Delete;
import de.tum.i13.server.kv.handlers.Get;
import de.tum.i13.server.kv.handlers.Put;
import de.tum.i13.server.kv.stores.LSMStore;
import de.tum.i13.shared.Config;

import java.io.IOException;

public class KVServer {

    private final String address;
    private final int port;
    private final KVTP2Server kvtp2Server;

    public KVServer(Config cfg) throws IOException {
        this.address = cfg.listenaddr;
        this.port = cfg.port;
        kvtp2Server = new KVTP2Server();

        KVStore store = new LSMStore(cfg.dataDir);
        KVCache cache = CacheBuilder.newBuilder()
                .size(cfg.cachesize)
                .algorithm(CacheBuilder.Algorithm.valueOf(cfg.cachedisplacement))
                .build();


        kvtp2Server.handle("get", new Get(cache, store));
        kvtp2Server.handle("put", new Put(cache, store));
        kvtp2Server.handle("delete", new Delete(cache, store));
    }

    public void start() throws IOException {
        kvtp2Server.start(address, port);
    }

}
