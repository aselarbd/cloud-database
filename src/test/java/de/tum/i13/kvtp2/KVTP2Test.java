package de.tum.i13.kvtp2;

import de.tum.i13.shared.TaskRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Disabled
public class KVTP2Test {

    private class Pair {
        private MessageWriter messageWriter;
        private Message message;

        public Pair(MessageWriter w, Message response) {
            this.messageWriter = w;
            this.message = response;
        }
    }

    @Test
    public void multiMessageTest() throws IOException, ExecutionException, InterruptedException {
        KVTP2Server server = new KVTP2Server();

        LinkedBlockingQueue<Pair> writers = new LinkedBlockingQueue<>();
        ExecutorService ponger = Executors.newSingleThreadExecutor();
        ponger.submit(() -> {
            while(true) {
                Pair take = writers.take();
                MessageWriter messageWriter = take.messageWriter;
                take.message.setCommand("pong1");
                messageWriter.write(take.message);
                messageWriter.flush();
                take.message.setCommand("pong2");
                messageWriter.write(take.message);
                messageWriter.flush();
            }
        });

        server.handle("ping", (w, m) -> {
            System.out.println("server received request: " + m);
            Message response = Message.getResponse(m);
            response.put("counter", m.get("counter"));
            response.put("client", m.get("client"));
            response.setCommand("pong");
            try {
                writers.put(new Pair(w, response));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        ExecutorService serverExecutor = Executors.newSingleThreadExecutor();
        serverExecutor.submit(() -> {
            try {
                server.start("localhost", 8080);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        Thread.sleep(50);

        startNewPingClient("1");
        startNewPingClient("2");
        startNewPingClient("3");
        startNewPingClient("4");
        startNewPingClient("5");

        Thread.sleep(20000);
    }

    private void startNewPingClient(String name) throws ExecutionException, InterruptedException, IOException {
        NonBlockingKVTP2Client client = new NonBlockingKVTP2Client();
        Future<Boolean> connected = client.connect("localhost", 8080);

        ExecutorService clientExecutor = Executors.newSingleThreadExecutor();
        clientExecutor.submit(() -> {
            try {
                client.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        connected.get();

        client.setDefaultHandler((w, m) -> {
            System.out.println("client " + name + " received response: " + m);
            assertThat(m.get("client"), is(equalTo(name)));
            if (m.getCommand().equals("pong2")) {
                Message message = new Message("ping");
                message.put("counter", "" + (Integer.parseInt(m.get("counter")) + 1));
                message.put("client", name);
                client.send(message);
            }
        });

        Message message = new Message("ping");
        message.put("counter", "" + 0);
        message.put("client", name);
        client.send(message);
    }
}
