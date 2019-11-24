package de.tum.i13.shared;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class HeartbeatListener {

    public static Logger logger = Logger.getLogger(HeartbeatListener.class.getName());

    public ScheduledExecutorService start(int port, InetAddress address) throws SocketException {
        DatagramSocket s = new DatagramSocket(port, address);
        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
        ses.schedule(() -> {
            try {
                while (true) {
                    byte[] pong = "pong".getBytes(Constants.TELNET_ENCODING_CHARSET);

                    byte[] ping = new byte[pong.length];
                    DatagramPacket p = new DatagramPacket(ping, ping.length);
                    s.setSoTimeout(3000);
                    s.receive(p);

                    logger.fine("Got " + new String(p.getData(), Constants.TELNET_ENCODING_CHARSET) + " from " + p.getAddress() + ":" + p.getPort());

                    if (Arrays.equals(p.getData(), "ping".getBytes(Constants.TELNET_ENCODING_CHARSET))) {
                        s.send(new DatagramPacket(pong, pong.length, p.getAddress(), p.getPort()));
                    }
                }
            } catch (IOException e) {
                logger.warning(e.getMessage());
            }
        }, 0, TimeUnit.MILLISECONDS);
        return ses;
    }
}
