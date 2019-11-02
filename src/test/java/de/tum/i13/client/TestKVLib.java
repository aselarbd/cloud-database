package de.tum.i13.client;

import de.tum.i13.client.communication.SocketCommunicator;
import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.KVResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class TestKVLib {
    private SocketCommunicator communicatorMock = mock(SocketCommunicator.class);

    private KVLib library;

    @BeforeEach
    public void initializeMocks() {
        reset(communicatorMock);
        this.library = new KVLib(communicatorMock);
    }

    @Test
    public void connect() throws SocketCommunicatorException {
        // when
        this.library.connect("localhost", 80);

        // then - check if connection is properly forwarded to communicator
        verify(communicatorMock).connect("localhost", 80);
    }

    @Test
    public void putNotConnected() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(false);
        // when
        this.library.put(new KVItem("key", "value"));

        // then
        verify(communicatorMock, never()).send(anyString());
    }

    @Test
    public void putValue() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(true);
        when(communicatorMock.send(anyString())).thenReturn("put_success key");

        // when
        KVResult result = this.library.put(new KVItem("key", "val"));

        // then
        verify(communicatorMock).send("put key " + new String(Base64.getEncoder().encode("val".getBytes())));
        assertTrue(result.getMessage().contains("put_success"));
    }

    @Test
    public void putNullValue() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(true);

        // when
        this.library.put(new KVItem("key"));

        // then
        verify(communicatorMock, never()).send(anyString());
    }

    @Test
    public void putNullItem() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(true);

        // when
        this.library.put(null);

        // then
        verify(communicatorMock, never()).send(anyString());
    }

    @Test
    public void putKeyTooLong() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(true);
        String testKey = new String(new byte[21]);

        // when
        this.library.put(new KVItem(testKey, "value"));

        // then
        verify(communicatorMock, never()).send(anyString());
    }

    @Test
    public void putValueTooLong() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(true);
        String testVal = new String(new byte[200001]);

        // when
        this.library.put(new KVItem("key", testVal));

        // then
        verify(communicatorMock, never()).send(anyString());
    }

}
