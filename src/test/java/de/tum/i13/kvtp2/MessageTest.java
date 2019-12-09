package de.tum.i13.kvtp2;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

class MessageTest {

    @Test
    public void testNewMessage() {
        Message m = new Message(Message.Type.REQUEST, "command");
        m.put("key", "value");
        m.put("key2", "value2");

        assertThat(m.getID(), is(not(equalTo(0))));

        assertThat("key:value\r\nkey2:value2\r\n", is(equalTo(m.body())));
        assertThat(m.toString(), containsString("_id:"));
        assertThat(m.toString(), containsString("_type:REQUEST\r\n"));
        assertThat(m.toString(), containsString("_command:command"));
        assertThat(m.toString(), containsString("_version:V2\r\n"));
        assertThat(m.toString(), containsString(m.body()));
    }

    @Test
    public void testParseMessage() {
        String msg = "_id:1\r\n_version:V2\r\n_type:REQUEST\r\n_command:command\r\nkey:value\r\nkey2:value2";

        Message m = Message.parse(msg);

        assertThat(m.getID(), is(equalTo(1)));
        assertThat(m.getVersion(), is(equalTo(Message.Version.V2)));
        assertThat(m.getType(), is(equalTo(Message.Type.REQUEST)));
        assertThat(m.getCommand(), is(equalTo("command")));
        assertThat(m.get("key"), is(equalTo("value")));
        assertThat(m.get("key2"), is(equalTo("value2")));
    }

    @Test
    public void testOldStylePut() {
        String msg = "put a b";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.REQUEST)));
        assertThat(m.getCommand(), is(equalTo("put")));
        assertThat(m.get("key"), is(equalTo("a")));
        assertThat(m.get("value"), is(equalTo("b")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStyleGet() {
        String msg = "get a";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.REQUEST)));
        assertThat(m.getCommand(), is(equalTo("get")));
        assertThat(m.get("key"), is(equalTo("a")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStyleDelete() {
        String msg = "delete a";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.REQUEST)));
        assertThat(m.getCommand(), is(equalTo("delete")));
        assertThat(m.get("key"), is(equalTo("a")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStyleKeyrange() {
        String msg = "keyrange";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.REQUEST)));
        assertThat(m.getCommand(), is(equalTo("keyrange")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStyleKeyrangeRead() {
        String msg = "keyrange_read";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.REQUEST)));
        assertThat(m.getCommand(), is(equalTo("keyrange_read")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStyleAny() {
        String m = "randomString";
        assertThrows(
                IllegalArgumentException.class,
                () -> Message.parse(m)
        );
    }

    @Test
    public void testOldStylePutSuccess() {
        String msg = "put_success key";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(m.getCommand(), is(equalTo("put_success")));
        assertThat(m.get("key"), is(equalTo("key")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStylePutUpdate() {
        String msg = "put_update key";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(m.getCommand(), is(equalTo("put_update")));
        assertThat(m.get("key"), is(equalTo("key")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStylePutError() {
        String msg = "put_error key value";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(m.getCommand(), is(equalTo("put_error")));
        assertThat(m.get("key"), is(equalTo("key")));
        assertThat(m.get("value"), is(equalTo("value")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStyleServerStopped() {
        String msg = "server_stopped";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(m.getCommand(), is(equalTo("server_stopped")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStyleServerWriteLock() {
        String msg = "server_write_lock";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(m.getCommand(), is(equalTo("server_write_lock")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStyleGetError() {
        String msg = "get_error key not found";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(m.getCommand(), is(equalTo("get_error")));
        assertThat(m.get("key"), is(equalTo("key")));
        assertThat(m.get("msg"), is(equalTo("not found")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStyleGetSuccess() {
        String msg = "get_success key value";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(m.getCommand(), is(equalTo("get_success")));
        assertThat(m.get("key"), is(equalTo("key")));
        assertThat(m.get("value"), is(equalTo("value")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStyleDeleteSuccess() {
        String msg = "delete_success key";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(m.getCommand(), is(equalTo("delete_success")));
        assertThat(m.get("key"), is(equalTo("key")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStyleDeleteError() {
        String msg = "delete_error key";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(m.getCommand(), is(equalTo("delete_error")));
        assertThat(m.get("key"), is(equalTo("key")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStyleKeyrangeSuccess() {
        String msg = "keyrange_success from1,to1,ip1:port1;from2,to2,ip2:port2";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(m.getCommand(), is(equalTo("keyrange_success")));
        assertThat(m.get("keyrange"), is(equalTo("from1,to1,ip1:port1;from2,to2,ip2:port2")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStyleKeyrangeReadSuccess() {
        String msg = "keyrange_read_success from1,to1,ip1:port1;from2,to2,ip2:port2";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(m.getCommand(), is(equalTo("keyrange_read_success")));
        assertThat(m.get("keyrange"), is(equalTo("from1,to1,ip1:port1;from2,to2,ip2:port2")));

        assertThat(m.toString(), is(equalTo(msg)));
    }

    @Test
    public void testOldStyleError() {
        String msg = "error description";
        Message m = Message.parse(msg);

        assertThat(m.getVersion(), is(equalTo(Message.Version.V1)));
        assertThat(m.getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(m.getCommand(), is(equalTo("error")));
        assertThat(m.get("description"), is(equalTo("description")));

        assertThat(m.toString(), is(equalTo(msg)));
    }
}