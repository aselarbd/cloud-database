package de.tum.i13.server.kv.state;

import de.tum.i13.server.kv.state.requests.Request;
import de.tum.i13.server.kv.state.requests.RequestType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class LockedState extends AbstractState {


    private static final Map<RequestType, BiConsumer<State, Request>> handlers = new HashMap<>(){
        {
            put(RequestType.REBALANCE, (s, r) -> ((LockedState) s).reBalance(r));
            put(RequestType.QUERY, (s, r) -> ((LockedState) s).query(r));
            put(RequestType.LOCK, (s, r) -> ((LockedState) s).lock(r));
            put(RequestType.UNLOCK, (s, r) -> ((LockedState) s).unLock(r));
            put(RequestType.STOP, (s, r) -> ((LockedState) s).stop(r));
        }
    };

    public LockedState(Server server) {
        super(server);
    }

    @Override
    protected Map<RequestType, BiConsumer<State, Request>> setHandlers() {
        return handlers;
    }

    private void reBalance(Request request) {
        logger.info("rebalance server " + request);
        // TODO
    }

    private void query(Request request) {
        logger.info("query locked server " + request);
        // TODO
    }

    private void lock(Request request) {
        logger.info("Attempt to lock already locked server " + request);
    }

    private void unLock(Request request) {
        logger.info("unLock server " + request);
        server.changeState(new RunningState(server));
    }

    private void stop(Request request) {
        logger.info("stop server" + request);
        server.changeState(new StoppedState(server));
    }
}
