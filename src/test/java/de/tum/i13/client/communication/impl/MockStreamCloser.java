package de.tum.i13.client.communication.impl;

import de.tum.i13.client.communication.StreamCloser;

import java.io.*;
import java.net.SocketException;

public class MockStreamCloser implements StreamCloser {

    private String connectedResponse;
    private boolean isConnected;
    private OutputStream outputStream;
    private InputStream inputStream;

    void setConnectedResponse(String connectedResponse) {
        this.connectedResponse = connectedResponse;
    }

    @Override
    public void connect(String address, int port) throws IOException {
        isConnected = true;
        outputStream = new ByteArrayOutputStream(128);
        inputStream = new ByteArrayInputStream(connectedResponse.getBytes());
    }

    /**
     * set socket timeout
     *
     * @param time : time in milliseconds
     * @throws SocketException if error in setting timeout
     */
    @Override
    public void setTimeOut(int time) throws SocketException {
        //TODO implement
    }

    @Override
    public boolean isConnected() {
        return isConnected;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return outputStream;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return inputStream;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
        outputStream.close();
        isConnected = false;
    }
}
