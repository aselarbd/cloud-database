package de.tum.i13.client.communication;

import java.net.SocketException;
import java.util.function.Supplier;

/**
 * TCP socket communication interface.
 */
public interface SocketCommunicator {
    /**
     * Initializes the communication library.
     *
     * @param encoding The encoding used for sending and receiving.
     */
    void init(Supplier<StreamCloser> streamCloserFactory, String encoding);

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
     * Checks if there is an active connection.
     *
     * @return true if connected
     */
    boolean isConnected();

    /**
     * Close the connection. Does nothing if the client isn't connected.
     *
     * @throws SocketCommunicatorException if an error occurs while closing the connection.
     */
    void disconnect() throws SocketCommunicatorException;

    /**
     * set a timeout
     * @param time : time in milliseconds
     * @throws SocketException: if error in setting timeout
     */
    void setTimeOut(int time) throws SocketException;

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
