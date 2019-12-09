package de.tum.i13.server.ecs;

import de.tum.i13.kvtp2.NonBlockingKVTP2Client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ServerState {

    private InetSocketAddress ecs;
    private InetSocketAddress kv;
    private List<Runnable> shutdownHooks = new ArrayList<>();

    private NonBlockingKVTP2Client client;

    public ServerState(InetSocketAddress ecs, InetSocketAddress kv) throws IOException {
        this.ecs = ecs;
        this.kv = kv;

        this.client = new NonBlockingKVTP2Client(new InetSocketAddress(ecs.getHostName(), ecs.getPort()));
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

    public NonBlockingKVTP2Client getClient() {
        return client;
    }
}
