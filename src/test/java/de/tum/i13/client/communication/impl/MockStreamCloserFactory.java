package de.tum.i13.client.communication.impl;

import de.tum.i13.client.communication.StreamCloser;

import java.util.function.Supplier;


public class MockStreamCloserFactory implements Supplier<StreamCloser> {

    String connectedResponse;

    MockStreamCloser mockStreamCloser;

    public MockStreamCloserFactory(String connectedResponse) {
        this.connectedResponse = connectedResponse;
    }

    @Override
    public StreamCloser get() {
        MockStreamCloser mockStreamCloser = new MockStreamCloser();
        mockStreamCloser.setConnectedResponse(connectedResponse);
        this.mockStreamCloser = mockStreamCloser;
        return mockStreamCloser;
    }

    public MockStreamCloser getMockStreamCloser() {
        return mockStreamCloser;
    }
}
