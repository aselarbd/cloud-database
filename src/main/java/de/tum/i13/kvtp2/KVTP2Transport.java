package de.tum.i13.kvtp2;

import java.io.IOException;

public interface KVTP2Transport {
    void connect() throws IOException;
    String send(String request) throws IOException;
    void close() throws IOException;
}
