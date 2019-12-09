package de.tum.i13.kvtp2;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;

class KVTP2ServerTest {

    @Test
    public void testNewServer() throws IOException {
        KVTP2Server kvtp2Server = new KVTP2Server();

        StringBuilder sb = new StringBuilder();
        kvtp2Server.handle("command", (w, m) -> {
            sb.append(m.get("greeting"));
        });

        Message message = new Message("command");
        message.put("greeting", "hello, world");
        kvtp2Server.serve(null, message);

        assertThat(sb.toString(), is(equalTo("hello, world")));
    }

    @Test
    public void testNewServerThread() throws IOException, InterruptedException {
        KVTP2Server kvtp2Server = new KVTP2Server();

        kvtp2Server.handle("greeting", (w,  m) -> {
            Message greeting = new Message("greeting");
            greeting.put("value", "hello, world");
            w.write(greeting);
            w.flush();
        });

        Thread t = new Thread(() -> {
            try {
                kvtp2Server.start("localhost", 9999);
            } catch (IOException e) {
                fail(e);
            }
        });
        t.setDaemon(false);
        Thread.sleep(5000);
    }

}