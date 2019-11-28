package de.tum.i13.kvtp2;

import java.io.IOException;

public interface StringWriter {

    void write(String string);

    void flush();

    void close() throws IOException;

}
