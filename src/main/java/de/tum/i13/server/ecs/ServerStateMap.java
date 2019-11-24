package de.tum.i13.server.ecs;

import de.tum.i13.shared.ConsistentHashMap;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class ServerStateMap {

    private Map<InetSocketAddress, ServerState> ecsAddrToServerState = new HashMap<>();
    private Map<InetSocketAddress, ServerState> kvAddrToServerState = new HashMap<>();
    private ConsistentHashMap keyRangeMap = new ConsistentHashMap();
    private boolean broadcasting;

    public ServerStateMap() {
    }

    public void add(ServerState serverState) {
        ecsAddrToServerState.put(serverState.getECS(), serverState);
        kvAddrToServerState.put(serverState.getKV(), serverState);
        keyRangeMap.put(serverState.getKV());
    }

    public void remove(ServerState serverState) {
        ecsAddrToServerState.remove(serverState.getECS());
        kvAddrToServerState.remove(serverState.getKV());
        keyRangeMap.remove(serverState.getKV());
        serverState.shutdown();
    }

    public ConsistentHashMap getKeyRanges() {
        return keyRangeMap;
    }

    public ServerState getKVPredecessor(ServerState serverState) {
        return kvAddrToServerState.get(keyRangeMap.getPredecessor(serverState.getKV()));
    }

    public ServerState getKVSuccessor(ServerState serverState) {
        return kvAddrToServerState.get(keyRangeMap.getSuccessor(serverState.getKV()));
    }

    public void setState(ServerState server, ServerState.State state) {
        ecsAddrToServerState.get(server.getECS()).setState(state);
        assert(kvAddrToServerState.get(server.getKV()).getState() == state); // TODO: shoud be updated by previous line via reference. If this doesn't crash, remove this line. Otherwise, set the state separately again.
    }

    public Collection<InetSocketAddress> getECSBroadcastSet() {
        return ecsAddrToServerState.keySet();
    }

    public ServerState getByECSAddress(InetSocketAddress addr) {
        return ecsAddrToServerState.get(addr);
    }

    public void startBroadcasting() {
        this.broadcasting = true;
    }

    public boolean isBroadcasting() {
        return broadcasting;
    }

    public void finishBroadcasting() {
        if (ecsAddrToServerState.values()
            .stream()
            .filter((s) ->
                    s.getState() == ServerState.State.BALANCE ||
                    s.getState() == ServerState.State.BOOTSTRAPPING
            )
            .collect(Collectors.toSet())
            .size() <= 0) {
            broadcasting = false;
        }
    }
}
