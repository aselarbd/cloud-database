package de.tum.i13.server.ecs;

import java.net.InetSocketAddress;

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
}
