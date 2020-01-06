package de.tum.i13.client;

import de.tum.i13.TestConstants;
import de.tum.i13.client.communication.SocketCommunicator;
import de.tum.i13.client.communication.SocketCommunicatorException;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.KVResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TestKVLib {
    // contains mocks for each call of the communicator factory
    private ArrayList<SocketCommunicator> communicators;
    private int currentCommunicator;

    private KVLib library;

    protected void mockAllKeyrange() throws SocketCommunicatorException {
        for (SocketCommunicator cMock : communicators) {
            when(cMock.send("keyrange")).thenReturn(
                    "keyrange_success " +
                    TestConstants.KEYRANGE_SIMPLE);
            when(cMock.send("keyrange_read")).thenReturn(
                    "keyrange_success " +
                    TestConstants.KEYRANGE_EXT);
        }
    }

    @BeforeEach
    public void initializeMocks(TestInfo info) throws SocketCommunicatorException {
        final boolean initLib = (!info.getTags().contains("no-lib-init"));
        // reset all communicators
        communicators = new ArrayList<>();
        currentCommunicator = 0;
        // create a new mock to be used for the first server
        communicators.add(mock(SocketCommunicator.class));
        if (initLib) {
            mockAllKeyrange();
        }
        // return the corresponding mock, depending on call count
        this.library = new KVLib(() -> {
            SocketCommunicator c = communicators.get(currentCommunicator);
            currentCommunicator++;
            return c;
        });
        if (initLib) {
            // initialize the lib with one server
            this.library.connect("192.168.1.1", 80);
            // reset again to allow changing send behavior of the first server
            reset(communicators.get(0));
        }
    }

    @Test
    public void connect() throws SocketCommunicatorException {
        // given
        this.communicators.add(mock(SocketCommunicator.class));
        // a random communicator can reply when keyrange is called
        mockAllKeyrange();

        // when
        this.library.connect("localhost", 80);

        // then - check if connection is properly forwarded to new communicator
        verify(communicators.get(1)).connect("localhost", 80);
    }

    @Test
    @Tag("no-lib-init")
    public void connectInvalidKeyrange() throws SocketCommunicatorException {
        String[] invalidKeyranges = {
                "test", "keyrange_success foo",
                "keyrange_success " + TestConstants.KEYRANGE_INVALID_IP
        };
        for (String kr : invalidKeyranges) {
            // always re-use communicator across factory calls in this case
            currentCommunicator = 0;
            when(communicators.get(0).send("keyrange")).thenReturn(kr);
            // nothing should be thrown
            this.library.connect("192.168.1.1", 80);
            reset(communicators.get(0));
            // now use a valid keyrange response but an invalid one for keyrange_read
            currentCommunicator = 0;
            when(communicators.get(0).send("keyrange")).thenReturn("keyrange_success "
                    + TestConstants.KEYRANGE_SIMPLE);
            when(communicators.get(0).send("keyrange_read")).thenReturn(kr);
            this.library.connect("192.168.1.1", 80);
            reset(communicators.get(0));
        }
    }

    @Test
    public void putNotConnected() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(false);
        // when
        this.library.put(new KVItem("key", "value"));

        // then
        verify(communicators.get(0), never()).send(anyString());
    }

    @Test
    public void putValue() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);
        when(communicators.get(0).send(anyString())).thenReturn("put_success key");

        // when
        KVResult result = this.library.put(new KVItem("key", "val"));

        // then
        verify(communicators.get(0)).send("put key " + new String(Base64.getEncoder().encode("val".getBytes())));
        assertTrue(result.getMessage().contains("put_success"));
    }

    @Test
    public void putValueErr() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);
        final String encVal = new String(Base64.getEncoder().encode("val".getBytes()));
        when(communicators.get(0).send(anyString())).thenReturn("put_error key " + encVal);

        // when
        KVResult result = this.library.put(new KVItem("key", "val"));

        // then
        verify(communicators.get(0)).send("put key " + encVal);
        assertTrue(result.getMessage().contains("put_error"));
    }

    @Test
    public void putNullValue() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);

        // when
        this.library.put(new KVItem("key"));

        // then
        verify(communicators.get(0), never()).send(anyString());
    }

    @Test
    public void putNullItem() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);

        // when
        this.library.put(null);

        // then
        verify(communicators.get(0), never()).send(anyString());
    }

    @Test
    public void putKeyTooLong() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);
        String testKey = new String(new byte[21]);

        // when
        this.library.put(new KVItem(testKey, "value"));

        // then
        verify(communicators.get(0), never()).send(anyString());
    }

    @Test
    public void putValueTooLong() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);
        String testVal = new String(new byte[120001]);

        // when
        this.library.put(new KVItem("key", testVal));

        // then
        verify(communicators.get(0), never()).send(anyString());
    }

    @Test
    public void putValue64TooLong() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);
        // would be short enough, but Base 64 string of 0 bytes still exceeds the limit
        String testVal = new String(new byte[119900]);

        // when
        this.library.put(new KVItem("key", testVal));

        // then
        verify(communicators.get(0), never()).send(anyString());
    }

    @Test
    public void putValueServerNull() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);
        when(communicators.get(0).send(anyString())).thenReturn(null);

        // when
        KVResult result = this.library.put(new KVItem("key", "val"));

        // then
        verify(communicators.get(0)).send("put key " + new String(Base64.getEncoder().encode("val".getBytes())));
        assertTrue(result.getMessage().toLowerCase().contains("empty"));
    }

    @Test
    public void getNotConnected() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(false);

        // when
        KVResult res = this.library.get(new KVItem("key"));

        // then
        verify(communicators.get(0), never()).send(anyString());
        assertNull(res.getItem());
    }

    @Test
    public void getValue() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);
        when(communicators.get(0).send(anyString())).thenReturn("get_success key "
                + new String(Base64.getEncoder().encode("val".getBytes())));

        // when
        KVResult res = this.library.get(new KVItem("key"));

        // then
        verify(communicators.get(0)).send("get key");
        assertEquals("get_success", res.getMessage());
        assertEquals("key", res.getItem().getKey());
        assertEquals("val", res.getItem().getValue());
    }

    @Test
    public void getKeyTooLong() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);
        String testKey = new String(new byte[21]);

        // when
        KVResult res = this.library.get(new KVItem(testKey));

        // then
        verify(communicators.get(0), never()).send(anyString());
        assertNull(res.getItem());
    }

    @Test
    public void getValueServerNull() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);
        when(communicators.get(0).send(anyString())).thenReturn(null);

        // when
        KVResult result = this.library.get(new KVItem("key"));

        // then
        verify(communicators.get(0)).send("get key");
        assertTrue(result.getMessage().toLowerCase().contains("empty"));
    }

    @Test
    public void getValueServerNoItem() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);
        when(communicators.get(0).send(anyString())).thenReturn("get_success");

        // when
        KVResult result = this.library.get(new KVItem("key"));

        // then
        verify(communicators.get(0)).send("get key");
        assertTrue(result.getMessage().toLowerCase().contains("empty"));
    }

    @Test
    public void getValueServerError() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);
        final String errorMsg = "some Error Message with mixed Case";
        when(communicators.get(0).send(anyString())).thenReturn("get_error key " + errorMsg);

        // when
        KVResult result = this.library.get(new KVItem("key"));

        // then
        verify(communicators.get(0)).send("get key");
        assertEquals("get_error", result.getMessage());
        assertEquals("key", result.getItem().getKey());
        // value may not be de-coded in error case
        assertEquals(errorMsg, result.getItem().getValue());
    }

    @Test
    public void deleteNotConnected() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(false);

        // when
        KVResult res = this.library.delete(new KVItem("key"));

        // then
        verify(communicators.get(0), never()).send(anyString());
        assertNull(res.getItem());
    }

    @Test
    public void deleteValue() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);
        when(communicators.get(0).send(anyString())).thenReturn("delete_success key");

        // when
        KVResult res = this.library.delete(new KVItem("key"));

        // then
        verify(communicators.get(0)).send("delete key");
        assertEquals("delete_success", res.getMessage());
        assertEquals("key", res.getItem().getKey());
    }

    @Test
    public void deleteValueError() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);
        when(communicators.get(0).send(anyString())).thenReturn("delete_error key");

        // when
        KVResult res = this.library.delete(new KVItem("key"));

        // then
        verify(communicators.get(0)).send("delete key");
        assertEquals("delete_error", res.getMessage());
        assertEquals("key", res.getItem().getKey());
    }

    @Test
    public void deleteKeyTooLong() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);
        String testKey = new String(new byte[21]);

        // when
        KVResult res = this.library.delete(new KVItem(testKey));

        // then
        verify(communicators.get(0), never()).send(anyString());
        assertNull(res.getItem());
    }

    @Test
    public void deleteServerNull() throws SocketCommunicatorException {
        when(communicators.get(0).isConnected()).thenReturn(true);
        when(communicators.get(0).send(anyString())).thenReturn(null);

        // when
        KVResult result = this.library.delete(new KVItem("key"));

        // then
        verify(communicators.get(0)).send("delete key");
        assertTrue(result.getMessage().toLowerCase().contains("empty"));
    }

    @Test
    public void disconnect() throws SocketCommunicatorException {
        // given
        communicators.add(mock(SocketCommunicator.class));
        mockAllKeyrange();
        // issue connect for second server
        this.library.connect("localhost", 80);

        // when
        this.library.disconnect();

        // then
        verify(communicators.get(0)).disconnect();
        verify(communicators.get(1)).disconnect();
    }
}
