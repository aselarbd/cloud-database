package de.tum.i13.kvtp2;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class KVTP2IntegrationTest {

    @Test
    public void testBlockingClient() throws IOException, InterruptedException {
        KVTP2Server kvtp2Server = new KVTP2Server();

        kvtp2Server.handle("greeting", (w,  m) -> {
            Message greeting = new Message(Message.Type.RESPONSE, "greeting");
            greeting.put("value", "hello, world");
            w.write(greeting);
            w.flush();
        });

        Thread th = new Thread(() -> {
            try {
                kvtp2Server.start("localhost", 9999);
            } catch (IOException e) {
                assertThat(e.getMessage(), false);
            }
        });
        th.setDaemon(true);
        th.start();
        Thread.sleep(4000);

        KVTP2Client client = new KVTP2Client("localhost", 9999);
        client.connect();

        Message request = new Message(Message.Type.REQUEST, "greeting");
        Message response1 = client.send(request);
        assertThat(response1.get("value"), is(equalTo("hello, world")));

        Message response2 = client.send(request);
        assertThat(response2.get("value"), is(equalTo("hello, world")));

        KVTP2Client client2 = new KVTP2Client("localhost", 9999);
        client2.connect();

        Message response3 = client2.send(request);
        assertThat(response3.get("value"), is(equalTo("hello, world")));

        Message response4 = client.send(request);
        assertThat(response4.get("value"), is(equalTo("hello, world")));

        Message response5 = client2.send(request);
        assertThat(response5.get("value"), is(equalTo("hello, world")));
    }

    @Test
    public void testNonBlockingClient() throws IOException, InterruptedException, ExecutionException {
        KVTP2Server kvtp2Server = new KVTP2Server();

        kvtp2Server.handle("greeting", (w,  m) -> {
            Message greeting = new Message(Message.Type.RESPONSE, "greeting");
            greeting.put("value", "hello, world");
            w.write(greeting);
            w.flush();
        });

        Thread th = new Thread(() -> {
            try {
                kvtp2Server.start("localhost", 9999);
            } catch (IOException e) {
                assertThat(e.getMessage(), false);
            }
        });
        th.setDaemon(true);
        th.start();
        Thread.sleep(4000);

        BiConsumer<MessageWriter, Message> assertion = (w, r) -> {
            assertThat(r.get("value"), is(equalTo("hello, world")));
        };

        NonBlockingKVTP2Client client = new NonBlockingKVTP2Client();
        Thread ct = new Thread(() -> {
            try {
                client.start();
            } catch (IOException e) {
                assertThat(e.getMessage(), false);
            }
        });
        ct.setDaemon(true);
        Future<Boolean> connected = client.connect("localhost", 9999);
        ct.start();

        connected.get();

        Message request = new Message(Message.Type.REQUEST, "greeting");
        request.put("host", "localhost");
        request.put("port", "9999");

        client.send(request, assertion);
        client.send(request, assertion);

        NonBlockingKVTP2Client client2 = new NonBlockingKVTP2Client();
        Thread ct2 = new Thread(() -> {
            try {
                client2.start();
            } catch (IOException e) {
                assertThat(e.getMessage(), false);
            }
        });
        ct2.setDaemon(true);
        Future<Boolean> connected2 = client2.connect("localhost", 9999);
        ct2.start();

        connected2.get();

        client2.send(request, assertion);
        client.send(request, assertion);
        client2.send(request, assertion);

        Thread.sleep(4000);
    }
}