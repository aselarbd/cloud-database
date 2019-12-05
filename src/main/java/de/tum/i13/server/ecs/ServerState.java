package de.tum.i13.server.ecs;

import de.tum.i13.kvtp2.MessageWriter;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class ServerState {

    private InetSocketAddress ecs;
    private MessageWriter messageWriter;
    private InetSocketAddress kv;
    private List<Runnable> shutdownHooks = new ArrayList<>();

    public ServerState(InetSocketAddress ecs, MessageWriter messageWriter, InetSocketAddress kv) {
        this.ecs = ecs;
        this.messageWriter = messageWriter;
        this.kv = kv;
    }

    public InetSocketAddress getKV() {
        return kv;
    }

    public InetSocketAddress getECS() {
        return ecs;
    }

    public MessageWriter getMessageWriter() {
        return this.messageWriter;
    }

    public void addShutdownHook(Runnable runner) {
        this.shutdownHooks.add(runner);
    }

    public void shutdown() {
        for (Runnable r : shutdownHooks) {
            r.run();
        }
    }
}
