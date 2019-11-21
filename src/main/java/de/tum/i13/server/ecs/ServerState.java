package de.tum.i13.server.ecs;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class ServerState {

    public enum State {
        BOOTSTRAPPING,
        ACTIVE,
        BALANCE,
        SHUTDOWN,
    }
    private InetSocketAddress ecs;
    private InetSocketAddress kv;
    private State state;

    private List<Runnable> shutdownHooks = new ArrayList<>();

    public ServerState(InetSocketAddress ecs, InetSocketAddress kv) {
        this.ecs = ecs;
        this.kv = kv;
        this.state = State.BOOTSTRAPPING;
    }

    public InetSocketAddress getKV() {
        return kv;
    }

    public InetSocketAddress getECS() {
        return ecs;
    }

    public void setState(State state) {
        this.state = state;
    }

    public State getState() {
        return this.state;
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
