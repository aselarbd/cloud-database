package de.tum.i13.kvtp2;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class KVTP2ClientTest {
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
            try {
                String s = greeting.toString();
                w.write(s, 0, s.length());
                w.flush();
            } catch (IOException e) {
                assertThat(e.getMessage(), false);
            }
        });

        Thread th = new Thread(() -> {
            try {
                kvtp2Server.start("localhost", 9999);
            } catch (IOException e) {
                assertThat(e.getMessage(), false);
            }
        });
        th.start();
        Thread.sleep(4000);

        KVTP2Client client = new KVTP2Client("localhost", 9999);
        client.connect();

        Message request = new Message(Message.Type.REQUEST, "greeting");
        Message response = client.send(request);
        assertThat(response.get("value"), is(equalTo("hello, world")));

        th.interrupt();
        th.join(3000);
    }
}