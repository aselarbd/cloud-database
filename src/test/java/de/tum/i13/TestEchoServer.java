package de.tum.i13;

import de.tum.i13.server.kv.KVMain;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestEchoServer {

    public static Integer port = 5153;

    // TODO: replace by some useful test case
    @Disabled
    @Test
    public void smokeTest() throws InterruptedException, IOException {
        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    KVMain.main(new String[]{"-p", port.toString()});
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        th.start(); // started the server
        Thread.sleep(2000);

        Socket s = new Socket();
        s.connect(new InetSocketAddress("127.0.0.1", port));
        String welcome = RequestUtils.readMessage(s);
        assertThat(welcome, is(containsString("connected to MSRG KVServer:")));
        String command = "put k v";
        assertThat(RequestUtils.doRequest(s, command), is(equalTo(command)));
        s.close();

    }

    // TODO: replace by some useful test case
    @Disabled
    @Test
    public void enjoyTheEcho() throws IOException, InterruptedException {
        Thread th = new Thread() {
            @Override
            public void run() {
                try {
                    KVMain.main(new String[]{"-p", port.toString()});
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        th.start(); // started the server
        Thread.sleep(2000);

        for (int tcnt = 0; tcnt < 2; tcnt++){
            final int finalTcnt = tcnt;
            new Thread(){
                @Override
                public void run() {
                    try {
                        Thread.sleep(finalTcnt * 100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        for(int i = 0; i < 100; i++) {
                            Socket s = new Socket();
                            s.connect(new InetSocketAddress("127.0.0.1", port));
                            String welcome = RequestUtils.readMessage(s);

                            String command = "hello " + finalTcnt;
                            assertThat(RequestUtils.doRequest(s, command), is(equalTo(command)));
                            s.close();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }.start();
        }



        //Assert.assertThat(doRequest("GET table key"), containsString("valuetest"));

        Thread.sleep(5000);
        th.interrupt();
    }
}
