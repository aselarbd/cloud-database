package de.tum.i13.server.kv.state;

import de.tum.i13.server.kv.state.requests.Request;
import de.tum.i13.server.kv.state.requests.RequestType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class RunningState extends AbstractState {

    private static final Map<RequestType, BiConsumer<State, Request>> handlers = new HashMap<>(){
        {
            put(RequestType.QUERY, (s, r) -> ((RunningState) s).query(r));
            put(RequestType.LOCK, (s, r) -> ((RunningState) s).lock(r));
            put(RequestType.UNLOCK, (s, r) -> ((RunningState) s).unLock(r));
        }
    };

    public RunningState(Server server) {
        super(server);
    }

    @Override
    protected Map<RequestType, BiConsumer<State, Request>> setHandlers() {
        return handlers;
    }

    private void query(Request request) {
        System.out.println("Running State query server");
    }

    private void lock(Request request) {
        System.out.println("Running State lock server");
        server.changeState(new LockedState(server));
    }

    private void unLock(Request request) {
        System.out.println("Running State unLock server");
    }
}