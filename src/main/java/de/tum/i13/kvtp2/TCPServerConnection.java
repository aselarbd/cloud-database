package de.tum.i13.kvtp2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.BiConsumer;

class TCPServerConnection extends Connection {

    private final Selector selector;

    private ServerSocketChannel ssc;
    private SocketChannel sc;

    private BiConsumer<StringWriter, byte[]> receiver;

    TCPServerConnection(String address, int port, Selector selector, BiConsumer<StringWriter, byte[]> receiver) throws IOException {
        InetSocketAddress address1 = new InetSocketAddress(address, port);
        this.selector = selector;
        this.receiver = receiver;

        ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        ssc.socket().bind(address1);
        this.key = ssc.register(selector, SelectionKey.OP_ACCEPT, this);
    }

    @Override
    void accept() throws IOException {
        this.sc = ssc.accept();
        sc.configureBlocking(false);
        new TCPConnection(this.selector, sc, this.receiver);
    }
}
