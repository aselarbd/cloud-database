package de.tum.i13.client.communication;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * StreamCloser provides an interface to create, close and
 * get Input- and OutputStreams to some remote end
 */
public interface StreamCloser {

    /**
     * Connect to a remote end with address and port
     *
     * @param address address to connect to
     * @param port port on which to connect
     *
     * @throws IOException if the connection fails.
     */
    void connect(String address, int port) throws IOException;

    /**
     * returns whether the socket is connected
     * @return
     */
    boolean isConnected();

    /**
     * Returns an OutputStream to write to the remote end
     *
     * @return The OutputStream to write to
     * @throws IOException if the creation of the OutputStream fails
     */
    OutputStream getOutputStream() throws IOException;

    /**
     * Returns an InputStream to read from the remote end
     *
     * @return The InputStream to read from
     * @throws IOException if the creation of the InputStream fails
     */
    InputStream getInputStream() throws IOException;

    /**
     * Closes the connection to the remote end
     *
     * @throws IOException if an error occurs when closing the connection
     */
    void close() throws IOException;
}
