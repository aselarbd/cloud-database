package de.tum.i13.client.communication.impl;

import de.tum.i13.client.communication.StreamCloser;
import de.tum.i13.client.communication.StreamCloserFactory;

public class MockStreamCloserFactory implements StreamCloserFactory {

    String connectedResponse;

    MockStreamCloser mockStreamCloser;

    public MockStreamCloserFactory(String connectedResponse) {
        this.connectedResponse = connectedResponse;
    }

    @Override
    public StreamCloser createStreamCloser() {
        MockStreamCloser mockStreamCloser = new MockStreamCloser();
        mockStreamCloser.setConnectedResponse(connectedResponse);
        this.mockStreamCloser = mockStreamCloser;
        return mockStreamCloser;
    }

    public MockStreamCloser getMockStreamCloser() {
        return mockStreamCloser;
    }
}
