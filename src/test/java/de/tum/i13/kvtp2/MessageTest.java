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
        assertThat(m.toString(), containsString(m.body()));
        assertThat(m.toString(), containsString("_id:"));
        assertThat(m.toString(), containsString("_type:REQUEST\r\n"));
        assertThat(m.toString(), containsString("_command:command"));
        assertThat(m.toString(), containsString(m.body()));
    }

    @Test
    public void testParseMessage() {
        String msg = "_id:1\r\n_type:REQUEST\r\n_command:command\r\nkey:value\r\nkey2:value2";

        Message m = Message.parse(msg);

        assertThat(m.getID(), is(equalTo(1)));
        assertThat(m.getType(), is(equalTo(Message.Type.REQUEST)));
        assertThat(m.getCommand(), is(equalTo("command")));
        assertThat(m.get("key"), is(equalTo("value")));
        assertThat(m.get("key2"), is(equalTo("value2")));
    }
}