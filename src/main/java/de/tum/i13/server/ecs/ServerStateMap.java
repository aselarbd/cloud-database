package de.tum.i13.server.ecs;

import de.tum.i13.shared.ConsistentHashMap;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ServerStateMap {

    private Map<InetSocketAddress, ServerState> ecsAddrToServerState = new HashMap<>();
    private Map<InetSocketAddress, ServerState> kvAddrToServerState = new HashMap<>();
    private ConsistentHashMap keyRangeMap = new ConsistentHashMap();

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

    public void setState(ServerState predecessor, ServerState.State state) {
        ecsAddrToServerState.get(predecessor.getECS()).setState(state);
        assert(kvAddrToServerState.get(predecessor.getKV()).getState() == state); // TODO: shoud be updated by previous line via reference. If this doesn't crash, remove this line. Otherwise, set the state separately again.
    }

    public Collection<InetSocketAddress> getECSBroadcastSet() {
        return ecsAddrToServerState.keySet();
    }
}
