package de.tum.i13.client.communication.impl;

import de.tum.i13.client.communication.SocketCommunicator;
import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.client.communication.StreamCloser;
import de.tum.i13.shared.Log;

import java.io.*;
import java.net.SocketException;
import java.util.function.Supplier;

// TODO move some of the tests over to kvtp2 transports

@Deprecated
public class SocketCommunicatorImpl implements SocketCommunicator {

    private final static Log logger = new Log(SocketCommunicatorImpl.class);

    private String encoding;

    private Supplier<StreamCloser> streamCloserFactory;

    private StreamCloser streamCloser;

    private OutputStream output;
    private BufferedReader input;

    @Override
    public void init(Supplier<StreamCloser> streamCloserFactory, String encoding) {
        this.streamCloserFactory = streamCloserFactory;
        this.encoding = encoding;
    }

    @Override
    public String connect(String address, int port) throws SocketCommunicatorException {

        if (isConnected()) {
            disconnect();
        }
        try {
            logger.info("Connecting to server");
            streamCloser = streamCloserFactory.get();
            streamCloser.connect(address, port);

            logger.info("Getting the outputstream and inputstream");
            output = streamCloser.getOutputStream();
            input = new BufferedReader(new InputStreamReader(streamCloser.getInputStream()));

            return send("connected");
        } catch (Exception e){
            logger.severe("SocketCommunicatorImpl::connect failed", e);
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
            logger.info("No connection available, nothing to do");
            return;
        }
        try {
            logger.info("Closing connection");
            output.close();
            input.close();
            streamCloser.close();
        } catch (Exception e) {
            logger.severe("SocketCommunicatorImpl::disconnect failed", e);
            throw new SocketCommunicatorException(e);
        }
    }

    @Override
    public String send(String message) throws SocketCommunicatorException {
        if (!isConnected()) {
            SocketCommunicatorException e = new SocketCommunicatorException("No connection");
            logger.severe("SocketCommunicatorImpl::send connection failed", e);
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
            logger.info("sending message: " + message);
            byte[] bytes = toSend.getBytes(encoding);
            output.write(bytes);
            output.flush();
            return receiveResponse();
        } catch(IOException e){
            logger.severe("SocketCommunicatorImpl::send failed", e);
            throw new SocketCommunicatorException(e);
        }
    }
}
