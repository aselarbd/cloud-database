package de.tum.i13.server.ecs;

import de.tum.i13.kvtp.CommandProcessor;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class ECSProcessor implements CommandProcessor {
    @Override
    public String process(String command) {
        return null;
    }

    @Override
    public String connectionAccepted(InetSocketAddress address, InetSocketAddress remoteAddress) {
        return null;
    }

    @Override
    public void connectionClosed(InetAddress address) {

    }
}
