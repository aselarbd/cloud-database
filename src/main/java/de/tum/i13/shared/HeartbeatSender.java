package de.tum.i13.shared;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HeartbeatSender {

    public static final Log logger = new Log(HeartbeatSender.class);
    public static final int TIMEOUT = Constants.PING_TIMEOUT;


    private final InetSocketAddress receiver;

    public HeartbeatSender(InetSocketAddress receiver) {
        this.receiver = receiver;
    }

    public ScheduledExecutorService start(Runnable exiter) {
        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
        try {
            DatagramSocket s = new DatagramSocket();
            ses.scheduleAtFixedRate(() -> {
                try {
                    byte[] ping = "ping".getBytes(Constants.TELNET_ENCODING_CHARSET);
                    DatagramPacket p = new DatagramPacket(
                            ping,
                            ping.length,
                            receiver.getAddress(),
                            receiver.getPort()
                    );

                    logger.fine("send ping to " + receiver.getAddress() + ":" + receiver.getPort());

                    s.send(p);

                    s.setSoTimeout(TIMEOUT);
                    DatagramPacket response = new DatagramPacket(ping, ping.length);
                    s.receive(response);

                    if (!Arrays.equals(response.getData(), "pong".getBytes(Constants.TELNET_ENCODING_CHARSET))) {
                        exiter.run();
                        ses.shutdown();
                    }

                    logger.fine("got " + new String(response.getData(), Constants.TELNET_ENCODING_CHARSET) + " from " + response.getAddress() + ":" + response.getPort());

                } catch (IOException e) {
                    logger.warning("lost connection, shutting server " + receiver.getHostString() + " down", e);
                    exiter.run();
                    s.close();
                    ses.shutdown();
                }
            }, 0, 1000, TimeUnit.MILLISECONDS);
        } catch (IOException e) {
            exiter.run();
        }
        return ses;
    }

}
