package de.tum.i13.client.communication.impl;

import de.tum.i13.client.communication.SocketCommunicator;
import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.client.communication.StreamCloser;
import de.tum.i13.client.communication.StreamCloserFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Logger;

public class SocketCommunicatorImpl implements SocketCommunicator {

    private final static Logger LOGGER = Logger.getLogger(SocketCommunicatorImpl.class.getName());

    private String encoding;

    private StreamCloserFactory streamCloserFactory;

    private StreamCloser streamCloser;

    private OutputStream socketOutputStream;
    private InputStream socketInputStream;

    @Override
    public void init(StreamCloserFactory streamCloserFactory, String encoding) {
        this.streamCloserFactory = streamCloserFactory;
        this.encoding = encoding;
    }

    @Override
    public String connect(String address, int port) throws SocketCommunicatorException {

        if (isConnected()) {
            disconnect();
        }
        try {
            LOGGER.info("Connecting to server");
            streamCloser = streamCloserFactory.createStreamCloser();
            streamCloser.connect(address, port);

            LOGGER.info("Getting the outputstream and inputstream");
            socketOutputStream = streamCloser.getOutputStream();
            socketInputStream = streamCloser.getInputStream();

            return receiveResponse(1024);
        } catch (Exception e){
            LOGGER.throwing(SocketCommunicatorImpl.class.getName(), "connect", e);
            throw new SocketCommunicatorException(e);
        }
    }

    @Override
    public boolean isConnected() {
        return streamCloser != null && streamCloser.isConnected();
    }

    /**
     * Read a response of the given length from the socketInputStream
     * This method uses the encoding provided on initialization to decode
     * a received byte-array into a string.
     *
     * @param length Maximum amount of bytes to receive
     * @return the string created from the received byte array
     *
     * @throws IOException If there's a problem while reading from the inputStream
     */
    private String receiveResponse(int length) throws IOException {
        byte[] response = new byte[length];

        int count;
        count = socketInputStream.read(response);
        String s = new String(response, 0, count, encoding);
        if (s.endsWith("\r\n")) {
            return s.substring(0, s.length() - 2);
        }
        return s;
    }

    @Override
    public void disconnect() throws SocketCommunicatorException {
        if (!isConnected()) {
            LOGGER.info("No connection available, nothing to do");
            return;
        }
        try {
            LOGGER.info("Closing connection");
            socketOutputStream.close();
            socketInputStream.close();
            streamCloser.close();
        } catch (Exception e) {
            LOGGER.throwing(SocketCommunicatorImpl.class.getName(), "disconnect", e);
            throw new SocketCommunicatorException(e);
        }
    }

    @Override
    public String send(String message) throws SocketCommunicatorException {
        if (!isConnected()) {
            SocketCommunicatorException e = new SocketCommunicatorException("No connection");
            LOGGER.throwing(SocketCommunicatorImpl.class.getName(), "send", e);
            throw e;
        }
        // TODO: dynamic length check
        /*if (message.length() > Constants.MAX_MESSAGE_LENGTH) {
            SocketCommunicatorException e = new SocketCommunicatorException("Message too long");
            LOGGER.throwing(SocketCommunicatorImpl.class.getName(), "send", e);
            throw e;
        }*/
        String toSend = message + "\r\n";

        try {
            LOGGER.info("sending message: " + message);
            byte[] bytes = toSend.getBytes(encoding);
            socketOutputStream.write(bytes);
            socketOutputStream.flush();
            return receiveResponse(bytes.length);
        } catch(IOException e){
            LOGGER.throwing(SocketCommunicatorImpl.class.getName(), "send", e);
            throw new SocketCommunicatorException(e);
        }
    }
}
