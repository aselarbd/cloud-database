package de.tum.i13.server.ecs;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.shared.ConsistentHashMap;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class ServerStateMap {

    public static Logger logger = Logger.getLogger(ServerStateMap.class.getName());

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

    public ServerState getKVSuccessor(ServerState serverState) {
        return kvAddrToServerState.get(keyRangeMap.getSuccessor(serverState.getKV()));
    }

    public ServerState getByECSAddress(InetSocketAddress addr) {
        return ecsAddrToServerState.get(addr);
    }

    public void broadcast(Message msg) {
        ecsAddrToServerState
                .forEach((k, v) -> {
                    v.getClient().send(msg, (m, w) -> {}); // ignore any responses
                });
    }
}
