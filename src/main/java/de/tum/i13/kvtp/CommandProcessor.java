package de.tum.i13.kvtp;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public interface CommandProcessor {

    String process(InetSocketAddress src, String command);

    String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress);

    void connectionClosed(InetAddress address);
}
