package de.tum.i13.client;

import de.tum.i13.TestConstants;
import de.tum.i13.kvtp2.KVTP2Client;
import de.tum.i13.kvtp2.Message;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.KVResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TestKVLib {
    // contains mocks for each call of the client factory
    private ArrayList<KVTP2Client> clients;
    private ArrayList<String> hosts;
    private ArrayList<Integer> ports;
    private int currentCommunicator;

    private KVLib library;

    private static Message msgWith(String command) {
        return argThat(m -> m != null && m.getCommand().equals(command));
    }

    private static Message kvMsgWith(String command, String key, String val) {
        return argThat(m -> m != null && m.getCommand().equals(command)
                && m.get("key") != null && m.get("key").equals(key)
                && (val == null || m.get("value") != null && m.get("value").equals(val)));
    }

    private static Message keyrangeMsg(String kr) {
        Message msg = new Message("keyrange_success", Message.Version.V1);
        msg.put("keyrange", kr);
        return msg;
    }

    protected void mockAllKeyrange() throws IOException {
        for (KVTP2Client cMock : clients) {
            when(cMock.send(msgWith("connected")))
                    .thenReturn(new Message("ok"));
            when(cMock.send(msgWith("keyrange")))
                    .thenReturn(keyrangeMsg(TestConstants.KEYRANGE_SIMPLE));
            when(cMock.send(msgWith("keyrange_read")))
                    .thenReturn(keyrangeMsg(TestConstants.KEYRANGE_EXT));
        }
    }

    @BeforeEach
    public void initializeMocks(TestInfo info) throws IOException {
        final boolean initLib = (!info.getTags().contains("no-lib-init"));
        // reset all clients
        clients = new ArrayList<>();
        hosts = new ArrayList<>();
        ports = new ArrayList<>();
        currentCommunicator = 0;
        // create a new mock to be used for the first server
        clients.add(mock(KVTP2Client.class));
        if (initLib) {
            mockAllKeyrange();
        }
        // return the corresponding mock, depending on call count
        this.library = new KVLib((ip, port) -> {
            KVTP2Client c = clients.get(currentCommunicator);
            // store host and port for possible verification
            hosts.add(ip);
            ports.add(port);
            currentCommunicator++;
            return c;
        });
        if (initLib) {
            // initialize the lib with one server
            this.library.connect("192.168.1.1", 80);
            // reset again to allow changing send behavior of the first server
            reset(clients.get(0));
        }
    }

    @Test
    public void connect() throws IOException {
        // given
        this.clients.add(mock(KVTP2Client.class));
        // a random communicator can reply when keyrange is called
        mockAllKeyrange();

        // when
        this.library.connect("localhost", 80);

        // then - check if connection is properly forwarded to new client
        verify(clients.get(1)).connect();
        assertEquals("localhost", hosts.get(1));
        assertEquals(80, ports.get(1));
    }

    @Test
    @Tag("no-lib-init")
    public void connectInvalidKeyrange() throws IOException {
        String[] invalidKeyranges = {
                "test", "keyrange_success foo",
                "keyrange_success " + TestConstants.KEYRANGE_INVALID_IP
        };
        for (String kr : invalidKeyranges) {
            // always re-use communicator across factory calls in this case
            currentCommunicator = 0;
            when(clients.get(0).send(msgWith("connected")))
                    .thenReturn(new Message("ok", Message.Version.V1));
            when(clients.get(0).send(msgWith("keyrange")))
                    .thenReturn(new Message(kr, Message.Version.V1));
            // nothing should be thrown
            this.library.connect("192.168.1.1", 80);
            reset(clients.get(0));
            // now use a valid keyrange response but an invalid one for keyrange_read
            currentCommunicator = 0;
            when(clients.get(0).send(msgWith("connected")))
                    .thenReturn(new Message("ok", Message.Version.V1));
            when(clients.get(0).send(msgWith("keyrange")))
                    .thenReturn(keyrangeMsg(TestConstants.KEYRANGE_SIMPLE));
            when(clients.get(0).send(msgWith("keyrange_read")))
                    .thenReturn(new Message(kr, Message.Version.V1));
            this.library.connect("192.168.1.1", 80);
            reset(clients.get(0));
        }
    }

    @Test
    public void putNotConnected() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(false);
        // when
        this.library.put(new KVItem("key", "value"));

        // then
        verify(clients.get(0), never()).send(any());
    }

    @Test
    public void putValue() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);
        Message successMsg = new Message("put_success", Message.Version.V1);
        successMsg.put("key", "key");
        when(clients.get(0).send(any())).thenReturn(successMsg);

        // when
        KVResult result = this.library.put(new KVItem("key", "val"));

        // then
        verify(clients.get(0)).send(kvMsgWith("put", "key",
                new String(Base64.getEncoder().encode("val".getBytes()))));
        assertTrue(result.getMessage().contains("put_success"));
    }

    @Test
    public void putValueErr() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);
        final String encVal = new String(Base64.getEncoder().encode("val".getBytes()));
        Message errorMsg = new Message("put_error", Message.Version.V1);
        errorMsg.put("key", "key");
        errorMsg.put("value", encVal);
        when(clients.get(0).send(any())).thenReturn(errorMsg);

        // when
        KVResult result = this.library.put(new KVItem("key", "val"));

        // then
        verify(clients.get(0)).send(kvMsgWith("put", "key", encVal));
        assertTrue(result.getMessage().contains("put_error"));
    }

    @Test
    public void putNullValue() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);

        // when
        this.library.put(new KVItem("key"));

        // then
        verify(clients.get(0), never()).send(any());
    }

    @Test
    public void putNullItem() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);

        // when
        this.library.put(null);

        // then
        verify(clients.get(0), never()).send(any());
    }

    @Test
    public void putKeyTooLong() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);
        String testKey = new String(new byte[21]);

        // when
        this.library.put(new KVItem(testKey, "value"));

        // then
        verify(clients.get(0), never()).send(any());
    }

    @Test
    public void putValueTooLong() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);
        String testVal = new String(new byte[120001]);

        // when
        this.library.put(new KVItem("key", testVal));

        // then
        verify(clients.get(0), never()).send(any());
    }

    @Test
    public void putValue64TooLong() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);
        // would be short enough, but Base 64 string of 0 bytes still exceeds the limit
        String testVal = new String(new byte[119900]);

        // when
        this.library.put(new KVItem("key", testVal));

        // then
        verify(clients.get(0), never()).send(any());
    }

    @Test
    public void putValueServerNull() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);
        when(clients.get(0).send(any())).thenReturn(null);

        // when
        KVResult result = this.library.put(new KVItem("key", "val"));

        // then
        verify(clients.get(0)).send(kvMsgWith("put", "key",
                new String(Base64.getEncoder().encode("val".getBytes()))));
        assertTrue(result.getMessage().toLowerCase().contains("empty"));
    }

    @Test
    public void getNotConnected() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(false);

        // when
        KVResult res = this.library.get(new KVItem("key"));

        // then
        verify(clients.get(0), never()).send(any());
        assertNull(res.getItem());
    }

    @Test
    public void getValue() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);
        Message successMsg = new Message("get_success", Message.Version.V1);
        successMsg.put("key", "key");
        successMsg.put("value", new String(Base64.getEncoder().encode("val".getBytes())));
        when(clients.get(0).send(any())).thenReturn(successMsg);

        // when
        KVResult res = this.library.get(new KVItem("key"));

        // then
        verify(clients.get(0)).send(kvMsgWith("get", "key", null));
        assertEquals("get_success", res.getMessage());
        assertEquals("key", res.getItem().getKey());
        assertEquals("val", res.getItem().getValue());
    }

    @Test
    public void getKeyTooLong() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);
        String testKey = new String(new byte[21]);

        // when
        KVResult res = this.library.get(new KVItem(testKey));

        // then
        verify(clients.get(0), never()).send(any());
        assertNull(res.getItem());
    }

    @Test
    public void getValueServerNull() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);
        when(clients.get(0).send(any())).thenReturn(null);

        // when
        KVResult result = this.library.get(new KVItem("key"));

        // then
        verify(clients.get(0)).send(kvMsgWith("get", "key", null));
        assertTrue(result.getMessage().toLowerCase().contains("empty"));
    }

    @Test
    public void getValueServerNoItem() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);
        when(clients.get(0).send(any())).thenReturn(new Message("get_success", Message.Version.V1));

        // when
        KVResult result = this.library.get(new KVItem("key"));

        // then
        verify(clients.get(0)).send(kvMsgWith("get", "key", null));
        assertTrue(result.getMessage().toLowerCase().contains("empty"));
    }

    @Test
    public void getValueServerError() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);
        final String errorStr = "some Error Message with mixed Case";
        Message errorMsg = new Message("get_error", Message.Version.V1);
        errorMsg.put("key", "key");
        errorMsg.put("msg", errorStr);
        when(clients.get(0).send(any())).thenReturn(errorMsg);

        // when
        KVResult result = this.library.get(new KVItem("key"));

        // then
        verify(clients.get(0)).send(kvMsgWith("get", "key", null));
        assertEquals("get_error", result.getMessage());
        assertEquals("key", result.getItem().getKey());
        // value may not be de-coded in error case
        assertEquals(errorStr, result.getItem().getValue());
    }

    @Test
    public void deleteNotConnected() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(false);

        // when
        KVResult res = this.library.delete(new KVItem("key"));

        // then
        verify(clients.get(0), never()).send(any());
        assertNull(res.getItem());
    }

    @Test
    public void deleteValue() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);
        Message successMsg = new Message("delete_success", Message.Version.V1);
        successMsg.put("key", "key");
        when(clients.get(0).send(any())).thenReturn(successMsg);

        // when
        KVResult res = this.library.delete(new KVItem("key"));

        // then
        verify(clients.get(0)).send(kvMsgWith("delete", "key", null));
        assertEquals("delete_success", res.getMessage());
        assertEquals("key", res.getItem().getKey());
    }

    @Test
    public void deleteValueError() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);
        Message errorMsg = new Message("delete_error", Message.Version.V1);
        errorMsg.put("key", "key");
        when(clients.get(0).send(any())).thenReturn(errorMsg);

        // when
        KVResult res = this.library.delete(new KVItem("key"));

        // then
        verify(clients.get(0)).send(kvMsgWith("delete", "key", null));
        assertEquals("delete_error", res.getMessage());
        assertEquals("key", res.getItem().getKey());
    }

    @Test
    public void deleteKeyTooLong() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);
        String testKey = new String(new byte[21]);

        // when
        KVResult res = this.library.delete(new KVItem(testKey));

        // then
        verify(clients.get(0), never()).send(any());
        assertNull(res.getItem());
    }

    @Test
    public void deleteServerNull() throws IOException {
        when(clients.get(0).isConnected()).thenReturn(true);
        when(clients.get(0).send(any())).thenReturn(null);

        // when
        KVResult result = this.library.delete(new KVItem("key"));

        // then
        verify(clients.get(0)).send(kvMsgWith("delete", "key", null));
        assertTrue(result.getMessage().toLowerCase().contains("empty"));
    }

    @Test
    public void disconnect() throws IOException {
        // given
        clients.add(mock(KVTP2Client.class));
        mockAllKeyrange();
        // issue connect for second server
        this.library.connect("localhost", 80);

        // when
        this.library.disconnect();

        // then
        verify(clients.get(0)).close();
        verify(clients.get(1)).close();
    }
}
