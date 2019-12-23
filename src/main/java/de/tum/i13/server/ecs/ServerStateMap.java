package de.tum.i13.server.ecs;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.shared.ConsistentHashMap;
import de.tum.i13.shared.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class ServerStateMap {

    public static final Log logger = new Log(ServerStateMap.class);

    private final Map<InetSocketAddress, ServerState> ecsAddrToServerState = new HashMap<>();
    private final Map<InetSocketAddress, ServerState> kvAddrToServerState = new HashMap<>();
    private final ConsistentHashMap keyRangeMap = new ConsistentHashMap();

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

    public ServerState getKVSuccessor(ServerState serverState) {
        return kvAddrToServerState.get(keyRangeMap.getSuccessor(serverState.getKV()));
    }

    public ServerState getByECSAddress(InetSocketAddress addr) {
        return ecsAddrToServerState.get(addr);
    }

    public ServerState getByKVAddress(InetSocketAddress addr) {
        return kvAddrToServerState.get(addr);
    }

    public void broadcast(Message msg) {
        ecsAddrToServerState
                .forEach((k, v) -> {
                    try {
                        v.getClient().send(msg);
                    } catch (IOException e) {
                        logger.warning("could not send broadcast to " + k, e);
                    }
                });
    }
}
