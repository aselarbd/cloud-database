package de.tum.i13.server.ecs;

import de.tum.i13.kvtp2.KVTP2Client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class ServerState {

    public static Logger logger = Logger.getLogger(ServerState.class.getName());

    private InetSocketAddress ecs;
    private InetSocketAddress kv;
    private List<Runnable> shutdownHooks = new ArrayList<>();

    private KVTP2Client client;

    public ServerState(InetSocketAddress ecs, InetSocketAddress kv) throws IOException {
        this.ecs = ecs;
        this.kv = kv;
    }

    public InetSocketAddress getKV() {
        return kv;
    }

    public InetSocketAddress getECS() {
        return ecs;
    }

    public void addShutdownHook(Runnable runner) {
        this.shutdownHooks.add(runner);
    }

    public void shutdown() {
        for (Runnable r : shutdownHooks) {
            r.run();
        }
    }

    public KVTP2Client getClient() throws IOException {
        if (this.client == null) {
            this.client = new KVTP2Client(ecs.getHostString(), ecs.getPort());
            this.client.connect();
        }
        return client;
    }
}
