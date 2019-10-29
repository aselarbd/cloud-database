package de.tum.i13.communication.impl;

import de.tum.i13.communication.StreamCloser;

import java.io.*;

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
