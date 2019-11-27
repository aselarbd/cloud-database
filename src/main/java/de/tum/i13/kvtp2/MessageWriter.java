package de.tum.i13.kvtp2;

public interface MessageWriter {

    void write(Message message);

    void flush();

    void close();

}
