package de.tum.i13.client.communication.impl;

import de.tum.i13.client.communication.SocketCommunicator;
import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.client.communication.StreamCloser;
import de.tum.i13.client.communication.StreamCloserFactory;

import java.io.*;
import java.net.SocketException;
import java.util.logging.Logger;

public class SocketCommunicatorImpl implements SocketCommunicator {

    private final static Logger LOGGER = Logger.getLogger(SocketCommunicatorImpl.class.getName());

    private String encoding;

    private StreamCloserFactory streamCloserFactory;

    private StreamCloser streamCloser;

    private OutputStream output;
    private BufferedReader input;

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
            output = streamCloser.getOutputStream();
            input = new BufferedReader(new InputStreamReader(streamCloser.getInputStream()));

            return receiveResponse();
        } catch (Exception e){
            LOGGER.throwing(SocketCommunicatorImpl.class.getName(), "connect", e);
            throw new SocketCommunicatorException(e);
        }
    }

    @Override
    public boolean isConnected() {
        return streamCloser != null && streamCloser.isConnected();
    }

    @Override
    public void setTimeOut(int time) throws SocketException {
        streamCloser.setTimeOut(time);
    }

    /**
     * Read a response from the socketInputStream
     *
     * @return the string read from the connection
     *
     * @throws IOException If there's a problem while reading from the sockets inputStream
     */
    private String receiveResponse() throws IOException {
        return input.readLine();
    }

    @Override
    public void disconnect() throws SocketCommunicatorException {
        if (!isConnected()) {
            LOGGER.info("No connection available, nothing to do");
            return;
        }
        try {
            LOGGER.info("Closing connection");
            output.close();
            input.close();
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
            output.write(bytes);
            output.flush();
            return receiveResponse();
        } catch(IOException e){
            LOGGER.throwing(SocketCommunicatorImpl.class.getName(), "send", e);
            throw new SocketCommunicatorException(e);
        }
    }
}
