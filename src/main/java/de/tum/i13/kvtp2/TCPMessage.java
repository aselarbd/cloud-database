package de.tum.i13.kvtp2;

import java.net.InetSocketAddress;

public class TCPMessage {
    private byte[] bytes;
    private InetSocketAddress remoteAddress;

    public TCPMessage(byte[] bytes, InetSocketAddress remoteAddress) {
        this.bytes = bytes;
        this.remoteAddress = remoteAddress;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }
}
