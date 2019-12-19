package de.tum.i13.server.kv.state;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.server.kv.KVCache;
import de.tum.i13.server.kv.KVStore;
import de.tum.i13.server.kv.replication.Replicator;
import de.tum.i13.server.kv.state.requests.Request;
import de.tum.i13.shared.ConsistentHashMap;

import java.net.InetSocketAddress;
import java.util.logging.Logger;

public class Server {

    public static final Logger logger = Logger.getLogger(Server.class.getName());

    private State state;

    private ConsistentHashMap keyRange;
    private InetSocketAddress kvApiAddress;
    private InetSocketAddress ecsApiAddress;

    private KVStore kvStore;
    private KVCache kvCache;
    private Replicator replicator;

    public Server(ConsistentHashMap keyRange) {
        this.state = new StoppedState(this);
        this.keyRange = keyRange;
    }

    public void changeState(Request request) {
        if (state.accepts(request)) {
            state.handle(request);
        }
    }

    public void changeState(State state) {
        this.state = state;
    }

    public Message getState() {
        return new Message("server_state");
    }

    public ConsistentHashMap getKeyRange() {
        return keyRange;
    }

    public void setKeyRange(ConsistentHashMap keyRange) {
        this.keyRange = keyRange;
    }
}
