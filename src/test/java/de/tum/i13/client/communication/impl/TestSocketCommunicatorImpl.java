package de.tum.i13.client.communication.impl;

import de.tum.i13.client.communication.SocketCommunicator;
import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.client.communication.StreamCloserFactory;
import de.tum.i13.shared.Constants;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class TestSocketCommunicatorImpl {

    private String connectedResponse = "ResponseMessage";
    private String dummyMessage = "hello world";

    @Test
    public void testConnect() throws SocketCommunicatorException {
        // given
        StreamCloserFactory mockStreamCloserFactory = new MockStreamCloserFactory(connectedResponse);
        SocketCommunicator socketCommunicator = new SocketCommunicatorImpl();

        // when
        socketCommunicator.init(mockStreamCloserFactory, Constants.TELNET_ENCODING);
        String result = socketCommunicator.connect("localhost", 1234);

        // then
        assertEquals(connectedResponse, result);
    }

    @Test
    public void testDoubleConnect() throws SocketCommunicatorException {
        // given
        StreamCloserFactory mockStreamCloserFactory = new MockStreamCloserFactory(connectedResponse);
        SocketCommunicator socketCommunicator = new SocketCommunicatorImpl();

        // when
        socketCommunicator.init(mockStreamCloserFactory, Constants.TELNET_ENCODING);
        String result1 = socketCommunicator.connect("localhost", 1234);
        String result2 = socketCommunicator.connect("localhost", 1234);

        // then
        assertEquals(connectedResponse, result1);
        assertEquals(connectedResponse, result2);
    }

    @Test
    public void testDisconnectUnconnected() throws SocketCommunicatorException {
        // given
        StreamCloserFactory mockStreamCloserFactory = new MockStreamCloserFactory(connectedResponse);
        SocketCommunicator socketCommunicator = new SocketCommunicatorImpl();

        // when
        socketCommunicator.init(mockStreamCloserFactory, Constants.TELNET_ENCODING);
        socketCommunicator.disconnect();

        // then
        // nothing, especially: no exception is thrown
    }

    @Test
    public void testDisconnect() throws SocketCommunicatorException {
        // given
        StreamCloserFactory mockStreamCloserFactory = new MockStreamCloserFactory(connectedResponse);
        SocketCommunicator socketCommunicator = new SocketCommunicatorImpl();

        // when
        socketCommunicator.init(mockStreamCloserFactory, Constants.TELNET_ENCODING);
        socketCommunicator.connect("localhost", 1234);
        socketCommunicator.disconnect();

        // then
        // nothing, no exceptions
    }

    @Test
    public void testSend() throws SocketCommunicatorException, IOException {
        // given
        MockStreamCloserFactory mockStreamCloserFactory = new MockStreamCloserFactory(dummyMessage);
        SocketCommunicator socketCommunicator = new SocketCommunicatorImpl();

        // when
        socketCommunicator.init(mockStreamCloserFactory, Constants.TELNET_ENCODING);
        socketCommunicator.connect("localhost", 1234);

        // reset the inputStream, to be able to read from it again
        // maybe there's a better solution using a more advanced mock
        ((ByteArrayInputStream) mockStreamCloserFactory.getMockStreamCloser().getInputStream()).reset();

        String response = socketCommunicator.send(dummyMessage);

        // then
        assertEquals(dummyMessage, response);
    }

    @Test
    public void testSendWithoutConnect() {
        // given
        MockStreamCloserFactory mockStreamCloserFactory = new MockStreamCloserFactory(dummyMessage);
        SocketCommunicator socketCommunicator = new SocketCommunicatorImpl();

        // when
        socketCommunicator.init(mockStreamCloserFactory, Constants.TELNET_ENCODING);

        // then
        assertThrows(SocketCommunicatorException.class, () -> {
            socketCommunicator.send(dummyMessage);
        });
    }

    /*
    @Test
    public void testSendLongMessage() throws SocketCommunicatorException, IOException {
        // given
        String msg = new String(new byte[256000]);
        MockStreamCloserFactory mockStreamCloserFactory = new MockStreamCloserFactory(msg);
        SocketCommunicator socketCommunicator = new SocketCommunicatorImpl();

        // when
        socketCommunicator.init(mockStreamCloserFactory, Constants.TELNET_ENCODING);
        socketCommunicator.connect("localhost", 1234);

        // then
        assertThrows(SocketCommunicatorException.class, () -> {
            socketCommunicator.send(msg);
        });
    }*/

    @Test
    public void testSendSpaces() throws SocketCommunicatorException, IOException {
        // given
        String msg = "hello     world";
        MockStreamCloserFactory mockStreamCloserFactory = new MockStreamCloserFactory(msg);
        SocketCommunicator socketCommunicator = new SocketCommunicatorImpl();

        // when
        socketCommunicator.init(mockStreamCloserFactory, Constants.TELNET_ENCODING);
        socketCommunicator.connect("localhost", 1234);

        // reset the inputStream, to be able to read from it again
        // maybe there's a better solution using a more advanced mock
        ((ByteArrayInputStream) mockStreamCloserFactory.getMockStreamCloser().getInputStream()).reset();

        String response = socketCommunicator.send(msg);

        // then
        assertEquals(msg, response);
    }

    @Test
    public void testUnexpectedDisconnect() throws SocketCommunicatorException, IOException {
        // given
        MockStreamCloserFactory mockStreamCloserFactory = new MockStreamCloserFactory(dummyMessage);
        SocketCommunicator socketCommunicator = new SocketCommunicatorImpl();

        // when
        socketCommunicator.init(mockStreamCloserFactory, Constants.TELNET_ENCODING);
        socketCommunicator.connect("localhost", 1234);

        // reset the inputStream, to be able to read from it again
        // maybe there's a better solution using a more advanced mock
        ((ByteArrayInputStream) mockStreamCloserFactory.getMockStreamCloser().getInputStream()).reset();

        mockStreamCloserFactory.getMockStreamCloser().close();

        // then
        assertThrows(SocketCommunicatorException.class, () -> {
            socketCommunicator.send(dummyMessage);
        });
    }
}