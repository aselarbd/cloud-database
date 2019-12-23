package de.tum.i13.server.ecs;

import de.tum.i13.kvtp2.KVTP2Client;
import de.tum.i13.shared.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class ServerState {

    public static Log logger = new Log(ServerState.class);

    private final InetSocketAddress ecs;
    private final InetSocketAddress kv;
    private final List<Runnable> shutdownHooks = new ArrayList<>();

    private final KVTP2Client client;

    public ServerState(InetSocketAddress ecs, InetSocketAddress kv, KVTP2Client client) {
        this.ecs = ecs;
        this.kv = kv;
        this.client = client;
        addShutdownHook(() -> {
            try {
                this.client.close();
            } catch (IOException e) {
                logger.warning("failed to close client", e);
            }
        });
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

    public KVTP2Client getClient() {
        return client;
    }
}
