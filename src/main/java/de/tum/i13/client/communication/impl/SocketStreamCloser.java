package de.tum.i13.client.communication.impl;

import de.tum.i13.client.communication.StreamCloser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;

/**
 * SocketStreamCloser provides an implementation of StreamCloser
 * using a java.net.Socket.
 */
@Deprecated
public class SocketStreamCloser implements StreamCloser {

    private Socket socket;

    /**
     * Creates and connects to a new Socket
     *
     * @param address address to connect to
     * @param port port on which to connect
     *
     * @throws IOException if an error occurs when creating or
     * connecting to the socket.
     */
    @Override
    public void connect(String address, int port) throws IOException {
        socket = new Socket(address, port);
    }

    /**
     * create timeout for a socket
     * @param time: timeout in milliseconds
     * @throws SocketException if error occurs in setting timeout
     */
    @Override
    public void setTimeOut(int time) throws SocketException {
        socket.setSoTimeout(time);
    }

    /**
     * returns whether the socket is connected and has not been closed.
     *
     * @return true if the socket is connected and not yet closed
     */
    @Override
    public boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }

    /**
     * Returns the OutputStream of the underlying socket to write
     * messages to the socket
     *
     * @return the OutputStream of the underlying socket
     * @throws IOException if an error occurs when creating the OutputStream
     */
    @Override
    public OutputStream getOutputStream() throws IOException {
        return socket.getOutputStream();
    }

    /**
     * Returns the InputStream of the underlying socket to read
     * messages from the socket
     *
     * @return the InputStream of the underlying socket
     * @throws IOException if an error occurs when creating the InputStream
     */
    @Override
    public InputStream getInputStream() throws IOException {
        return socket.getInputStream();
    }

    /**
     * Closes the connection to the socket
     *
     * @throws IOException if an error occurs when closing the connection
     */
    @Override
    public void close() throws IOException {
        socket.close();
    }
}
