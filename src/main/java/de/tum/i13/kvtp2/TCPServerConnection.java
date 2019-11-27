package de.tum.i13.kvtp2;

import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.BiConsumer;

public class TCPServerConnection extends Connection {

    private final InetSocketAddress address;
    private final Selector selector;

    private ServerSocketChannel ssc;
    private SocketChannel sc;

    private BiConsumer<Writer, byte[]> receiver;

    public TCPServerConnection(String address, int port, Selector selector, BiConsumer<Writer, byte[]> receiver) throws IOException {
        this.address = new InetSocketAddress(address, port);
        this.selector = selector;
        this.receiver = receiver;

        ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        ssc.socket().bind(this.address);
        this.key = ssc.register(selector, SelectionKey.OP_ACCEPT, this);
    }

    @Override
    void accept() throws IOException {
        this.sc = ssc.accept();
        sc.configureBlocking(false);
        new TCPConnection(this.selector, sc, this.receiver);
    }
}
