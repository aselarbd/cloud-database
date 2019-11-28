package de.tum.i13.kvtp2;

import java.io.IOException;

public interface MessageWriter {

    void write(Message message);

    void flush();

    void close() throws IOException;

}
