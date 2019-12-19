package de.tum.i13.server.kv.state;

import de.tum.i13.server.kv.state.requests.Request;
import de.tum.i13.server.kv.state.requests.RequestType;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

public abstract class AbstractState implements State {

    public static final Logger logger = Logger.getLogger(AbstractState.class.getName());

    protected final Map<RequestType, BiConsumer<State, Request>> handlers = setHandlers();

    protected Server server;

    public AbstractState(Server server) {
        this.server = server;
    }

    protected abstract Map<RequestType, BiConsumer<State, Request>> setHandlers();

    @Override
    public void handle(Request request) {
        if (!accepts(request)) throw new IllegalArgumentException("Can't transition to new state from this state");
        logger.info("");
        handlers.get(request.getType()).accept(this, request);
    }

    @Override
    public boolean accepts(Request request) {
        return handlers.containsKey(request.getType());
    }
}
