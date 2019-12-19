package de.tum.i13.server.kv.state;

import de.tum.i13.server.kv.state.requests.Request;
import de.tum.i13.server.kv.state.requests.RequestType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

public class StoppedState extends AbstractState {

    public static final Logger logger = Logger.getLogger(StoppedState.class.getName());

    private static final Map<RequestType, BiConsumer<State, Request>> handlers = new HashMap<>(){
        {
            put(RequestType.LOCK, (s, r) -> ((StoppedState) s).lock(r));
            put(RequestType.STOP, (s, r) -> ((StoppedState) s).stop(r));
            put(RequestType.SHUTDOWN, (s, r) -> ((StoppedState) s).shutdown(r));
        }
    };

    protected StoppedState(Server server) {
        super(server);
    }

    @Override
    protected Map<RequestType, BiConsumer<State, Request>> setHandlers() {
        return handlers;
    }

    private void lock(Request request) {
        logger.info("Lock Server Request " + request);
        server.changeState(new LockedState(server));
    }

    private void stop(Request request) {
        logger.info("Attempt to stop already stopped server " + request);
    }

    private void shutdown(Request request) {
        logger.info("Shutdown server " + request);
    }
}
