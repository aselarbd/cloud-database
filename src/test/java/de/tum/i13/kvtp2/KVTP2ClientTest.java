package de.tum.i13.kvtp2;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class KVTP2ClientTest {
    @Disabled
    @Test
    public void testNewClient() throws IOException {

        KVTP2Client client = new KVTP2Client("localhost", 7);
        client.connect();

        Message request = new Message(Message.Type.REQUEST, "echo");
        Message response = client.send(request);
        assertThat(response.toString(), is(equalTo(request.toString())));
    }

    @Test
    public void testIntegration() throws IOException, InterruptedException {
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
}