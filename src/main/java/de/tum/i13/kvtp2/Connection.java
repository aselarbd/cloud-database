package de.tum.i13.kvtp2;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public abstract class Connection {

    protected SelectionKey key;

    void accept() throws IOException {
        throw new UnsupportedOperationException("accept not supported on " + this);
    }

    void connect() {
        throw new UnsupportedOperationException("accept not supported on " + this);
    }

    void read() throws IOException {
        throw new UnsupportedOperationException("accept not supported on " + this);
    }

    void write() throws IOException {
        throw new UnsupportedOperationException("accept not supported on " + this);
    }

}
