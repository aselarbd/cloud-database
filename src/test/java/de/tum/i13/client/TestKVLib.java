package de.tum.i13.client;

import de.tum.i13.client.communication.SocketCommunicator;
import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.KVResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;
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
        String testVal = new String(new byte[120001]);

        // when
        this.library.put(new KVItem("key", testVal));

        // then
        verify(communicatorMock, never()).send(anyString());
    }

    @Test
    public void putValue64TooLong() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(true);
        // would be short enough, but Base 64 string of 0 bytes still exceeds the limit
        String testVal = new String(new byte[119900]);

        // when
        this.library.put(new KVItem("key", testVal));

        // then
        verify(communicatorMock, never()).send(anyString());
    }

    @Test
    public void putValueServerNull() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(true);
        when(communicatorMock.send(anyString())).thenReturn(null);

        // when
        KVResult result = this.library.put(new KVItem("key", "val"));

        // then
        verify(communicatorMock).send("put key " + new String(Base64.getEncoder().encode("val".getBytes())));
        assertTrue(result.getMessage().toLowerCase().contains("empty"));
    }

    @Test
    public void getNotConnected() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(false);

        // when
        KVResult res = this.library.get(new KVItem("key"));

        // then
        verify(communicatorMock, never()).send(anyString());
        assertNull(res.getItem());
    }

    @Test
    public void getValue() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(true);
        when(communicatorMock.send(anyString())).thenReturn("get_success key "
                + new String(Base64.getEncoder().encode("val".getBytes())));

        // when
        KVResult res = this.library.get(new KVItem("key"));

        // then
        verify(communicatorMock).send("get key");
        assertEquals("get_success", res.getMessage());
        assertEquals("key", res.getItem().getKey());
        assertEquals("val", res.getItem().getValue());
    }

    @Test
    public void getKeyTooLong() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(true);
        String testKey = new String(new byte[21]);

        // when
        KVResult res = this.library.get(new KVItem(testKey));

        // then
        verify(communicatorMock, never()).send(anyString());
        assertNull(res.getItem());
    }

    @Test
    public void getValueServerNull() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(true);
        when(communicatorMock.send(anyString())).thenReturn(null);

        // when
        KVResult result = this.library.get(new KVItem("key"));

        // then
        verify(communicatorMock).send("get key");
        assertTrue(result.getMessage().toLowerCase().contains("empty"));
    }

    @Test
    public void getValueServerTooLong() throws SocketCommunicatorException {
        byte[] testVal = new byte[120001];
        when(communicatorMock.isConnected()).thenReturn(true);
        when(communicatorMock.send(anyString())).thenReturn("get_success key "
                + new String(Base64.getEncoder().encode(testVal)));

        // when
        KVResult res = this.library.get(new KVItem("key"));

        // then
        verify(communicatorMock).send("get key");
        assertEquals("get_success", res.getMessage());
        assertNull(res.getItem());
    }

    @Test
    public void deleteNotConnected() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(false);

        // when
        KVResult res = this.library.delete(new KVItem("key"));

        // then
        verify(communicatorMock, never()).send(anyString());
        assertNull(res.getItem());
    }

    @Test
    public void deleteValue() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(true);
        when(communicatorMock.send(anyString())).thenReturn("delete_success key");

        // when
        KVResult res = this.library.delete(new KVItem("key"));

        // then
        verify(communicatorMock).send("delete key");
        assertEquals("delete_success", res.getMessage());
        assertEquals("key", res.getItem().getKey());
    }

    @Test
    public void deleteValueError() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(true);
        when(communicatorMock.send(anyString())).thenReturn("delete_error key");

        // when
        KVResult res = this.library.delete(new KVItem("key"));

        // then
        verify(communicatorMock).send("delete key");
        assertEquals("delete_error", res.getMessage());
        assertEquals("key", res.getItem().getKey());
    }

    @Test
    public void deleteKeyTooLong() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(true);
        String testKey = new String(new byte[21]);

        // when
        KVResult res = this.library.delete(new KVItem(testKey));

        // then
        verify(communicatorMock, never()).send(anyString());
        assertNull(res.getItem());
    }

    @Test
    public void deleteServerNull() throws SocketCommunicatorException {
        when(communicatorMock.isConnected()).thenReturn(true);
        when(communicatorMock.send(anyString())).thenReturn(null);

        // when
        KVResult result = this.library.delete(new KVItem("key"));

        // then
        verify(communicatorMock).send("delete key");
        assertTrue(result.getMessage().toLowerCase().contains("empty"));
    }

    @Test
    public void disconnect() throws SocketCommunicatorException {
        // when
        this.library.disconnect();

        // then
        verify(communicatorMock).disconnect();
    }
}
