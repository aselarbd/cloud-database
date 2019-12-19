package de.tum.i13.server.kv.state.requests;

public class Request {

    private final RequestType type;

    public Request(RequestType type) {
        this.type = type;
    }

    public RequestType getType() {
        return type;
    }
}
