package de.tum.i13.server.ecs;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.server.ecs.handlers.Finish;
import de.tum.i13.server.ecs.handlers.Register;
import de.tum.i13.server.ecs.handlers.Shutdown;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

class ECSServerTest {

    @Test
    public void testRegisterHandler() {
        ServerStateMap serverStateMap = new ServerStateMap();
        Register register = new Register(serverStateMap);

        MessageWriter messageWriterMock = mock(MessageWriter.class);
        Message message = new Message("register");
        message.put("kvip", "127.0.0.1");
        message.put("kvport", "8080");
        message.put("ecsip", "127.0.0.1");
        message.put("ecsport", "8081");

        MessageWriter messageWriterMock2 = mock(MessageWriter.class);
        Message message2 = new Message( "register");
        message2.put("kvip", "127.0.0.2");
        message2.put("kvport", "8080");
        message2.put("ecsip", "127.0.0.2");
        message2.put("ecsport", "8081");

        register.accept(messageWriterMock, message);

        assertThat(serverStateMap.getKeyRanges().size(), is(equalTo(1)));

        register.accept(messageWriterMock2, message2);

        assertThat(serverStateMap.getKeyRanges().size(), is(equalTo(2)));

        ArgumentCaptor<Message> argumentCaptor = ArgumentCaptor.forClass(Message.class);
        verify(messageWriterMock, times(3)).write(argumentCaptor.capture());
        List<Message> msgs = argumentCaptor.getAllValues();

        assertThat(msgs.get(0).getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(msgs.get(0).getCommand(), is(equalTo("keyrange")));
        assertThat(msgs.get(0).get("keyrange"), is(equalTo("5958c386bf5e9109ac10d2a628645aea,5958c386bf5e9109ac10d2a628645aea,127.0.0.1:8080;")));

        assertThat(msgs.get(1).getCommand(), is(equalTo("write_lock")));
        assertThat(msgs.get(2).getCommand(), is(equalTo("keyrange")));
        assertThat(msgs.get(2).get("keyrange"), is(equalTo("1e8b918ecc7853557fc1acca2da4fa45,5958c386bf5e9109ac10d2a628645aea,127.0.0.2:8080;5958c386bf5e9109ac10d2a628645aea,1e8b918ecc7853557fc1acca2da4fa45,127.0.0.1:8080;")));

        ArgumentCaptor<Message> argumentCaptor2 = ArgumentCaptor.forClass(Message.class);
        verify(messageWriterMock2).write(argumentCaptor2.capture());
        List<Message> msgs2 = argumentCaptor2.getAllValues();

        assertThat(msgs2.get(0).getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(msgs2.get(0).getCommand(), is(equalTo("keyrange")));
        assertThat(msgs2.get(0).get("keyrange"), is(equalTo("1e8b918ecc7853557fc1acca2da4fa45,5958c386bf5e9109ac10d2a628645aea,127.0.0.2:8080;5958c386bf5e9109ac10d2a628645aea,1e8b918ecc7853557fc1acca2da4fa45,127.0.0.1:8080;")));
    }

    @Test
    public void testShutdownHandler() {
        ServerStateMap serverStateMap = new ServerStateMap();
        Register register = new Register(serverStateMap);

        MessageWriter messageWriterMock = mock(MessageWriter.class);
        Message message = new Message("register");
        message.put("kvip", "127.0.0.1");
        message.put("kvport", "8080");
        message.put("ecsip", "127.0.0.1");
        message.put("ecsport", "8081");

        MessageWriter messageWriterMock2 = mock(MessageWriter.class);
        Message message2 = new Message("register");
        message2.put("kvip", "127.0.0.2");
        message2.put("kvport", "8080");
        message2.put("ecsip", "127.0.0.2");
        message2.put("ecsport", "8081");

        register.accept(messageWriterMock, message);
        register.accept(messageWriterMock2, message2);

        Shutdown shutdown = new Shutdown(serverStateMap);

        Message shutdownMsg = new Message( "shutdown");
        shutdownMsg.put("ecsip", "127.0.0.1");
        shutdownMsg.put("ecsport", "8081");
        shutdown.accept(messageWriterMock, shutdownMsg);

        ArgumentCaptor<Message> messageArgumentCaptor = ArgumentCaptor.forClass(Message.class);
        verify(messageWriterMock, times(5)).write(messageArgumentCaptor.capture());
        List<Message> msgs = messageArgumentCaptor.getAllValues();

        assertThat(msgs.get(3).getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(msgs.get(3).getCommand(), is(equalTo("write_lock")));
        assertThat(msgs.get(4).getType(), is(equalTo(Message.Type.RESPONSE)));
        assertThat(msgs.get(4).getCommand(), is(equalTo("keyrange")));
        assertThat(msgs.get(4).get("keyrange"), is(equalTo("1e8b918ecc7853557fc1acca2da4fa45,1e8b918ecc7853557fc1acca2da4fa45,127.0.0.2:8080;")));
    }

    @Test
    public void testFinishHandler() {
        ServerStateMap serverStateMap = new ServerStateMap();
        Register register = new Register(serverStateMap);

        MessageWriter messageWriterMock = mock(MessageWriter.class);
        Message message = new Message("register");
        message.put("kvip", "127.0.0.1");
        message.put("kvport", "8080");
        message.put("ecsip", "127.0.0.1");
        message.put("ecsport", "8081");

        MessageWriter messageWriterMock2 = mock(MessageWriter.class);
        Message message2 = new Message("register");
        message2.put("kvip", "127.0.0.2");
        message2.put("kvport", "8080");
        message2.put("ecsip", "127.0.0.2");
        message2.put("ecsport", "8081");

        register.accept(messageWriterMock, message);
        register.accept(messageWriterMock2, message2);

        Finish finish = new Finish(serverStateMap);

        Message finishMsg = new Message("finish");
        finishMsg.put("ecsip", "127.0.0.1");
        finishMsg.put("ecsport", "8081");

        finish.accept(messageWriterMock, finishMsg);

        Shutdown shutdown = new Shutdown(serverStateMap);

        Message shutdownMsg = new Message("shutdown");
        shutdownMsg.put("ecsip", "127.0.0.1");
        shutdownMsg.put("ecsport", "8081");
        shutdown.accept(messageWriterMock, shutdownMsg);

        Message finishMsg2 = new Message( "finish");
        finishMsg2.put("ecsip", "127.0.0.1");
        finishMsg2.put("ecsport", "8081");

        finish.accept(messageWriterMock, finishMsg2);

        ArgumentCaptor<Message> messageArgumentCaptor = ArgumentCaptor.forClass(Message.class);
        verify(messageWriterMock, times(8)).write(messageArgumentCaptor.capture());
        List<Message> msgs = messageArgumentCaptor.getAllValues();


        assertThat(msgs.get(0).getCommand(), is(equalTo("keyrange")));
        assertThat(msgs.get(0).get("keyrange"), is(equalTo("5958c386bf5e9109ac10d2a628645aea,5958c386bf5e9109ac10d2a628645aea,127.0.0.1:8080;")));
        assertThat(msgs.get(1).getCommand(), is(equalTo("write_lock")));
        assertThat(msgs.get(2).getCommand(), is(equalTo("keyrange")));
        assertThat(msgs.get(2).get("keyrange"), is(equalTo("1e8b918ecc7853557fc1acca2da4fa45,5958c386bf5e9109ac10d2a628645aea,127.0.0.2:8080;5958c386bf5e9109ac10d2a628645aea,1e8b918ecc7853557fc1acca2da4fa45,127.0.0.1:8080;")));
        assertThat(msgs.get(3).getCommand(), is(equalTo("release_lock")));
        assertThat(msgs.get(4).getCommand(), is(equalTo("keyrange")));
        assertThat(msgs.get(4).get("keyrange"), is(equalTo("1e8b918ecc7853557fc1acca2da4fa45,5958c386bf5e9109ac10d2a628645aea,127.0.0.2:8080;5958c386bf5e9109ac10d2a628645aea,1e8b918ecc7853557fc1acca2da4fa45,127.0.0.1:8080;")));
        assertThat(msgs.get(5).getCommand(), is(equalTo("write_lock")));
        assertThat(msgs.get(6).getCommand(), is(equalTo("keyrange")));
        assertThat(msgs.get(6).get("keyrange"), is(equalTo("1e8b918ecc7853557fc1acca2da4fa45,1e8b918ecc7853557fc1acca2da4fa45,127.0.0.2:8080;")));
        assertThat(msgs.get(7).getCommand(), is(equalTo("bye")));
    }
}
