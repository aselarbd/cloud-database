package de.tum.i13.communication;

/**
 * TCP socket communication interface.
 */
public interface SocketCommunicator {
    /**
     * Initializes the communication library.
     *
     * @param encoding The encoding used for sending and receiving.
     */
    void init(StreamCloserFactory streamCloserFactory, String encoding);

    /**
     * Connect to the given server.
     *
     * @param address The server IP address as String
     * @param port The port number as int
     *
     * @return the message returned by the server or
     * a useful error message if no connection could be established
     *
     * @throws SocketCommunicatorException if the connection fails.
     */
    String connect(String address, int port) throws SocketCommunicatorException;

    /**
     * Close the connection. Does nothing if the client isn't connected.
     *
     * @throws SocketCommunicatorException if an error occurs while closing the connection.
     */
    void disconnect() throws SocketCommunicatorException;

    /**
     * Send a message
     *
     * @param message Message to send, having the given encoding.
     *
     * @return Response from the server
     *
     * @throws SocketCommunicatorException if sending fails.
     */
    String send(String message) throws SocketCommunicatorException;
}
