package de.tum.i13.server.kv.state;

import de.tum.i13.server.kv.state.requests.Request;

public interface State {

    void handle(Request request);

    boolean accepts(Request request);

}
