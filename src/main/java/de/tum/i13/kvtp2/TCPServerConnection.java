package de.tum.i13.kvtp2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiConsumer;

class TCPServerConnection extends Connection {

    private ServerSocketChannel ssc;
    private List<Connection> connections;

    private BiConsumer<StringWriter, byte[]> receiver;

    TCPServerConnection(String address, int port, Selector selector, BiConsumer<StringWriter, byte[]> receiver) throws IOException {
        super(ServerSocketChannel.open());
        this.receiver = receiver;

        this.ssc = (ServerSocketChannel) super.channel;

        ssc.configureBlocking(false);
        ssc.socket().bind(new InetSocketAddress(address, port));
        this.key = ssc.register(selector, SelectionKey.OP_ACCEPT, this);
        this.connections = new ArrayList<>();
    }

    @Override
    void accept() throws IOException {
        SocketChannel sc = ssc.accept();
        sc.configureBlocking(false);
        this.connections.add(new TCPConnection(sc, this.receiver));
    }

    @Override
    public synchronized List<ChangeRequest> getPendingChanges() {
        List<ChangeRequest> crs = new LinkedList<>();
        connections.forEach((c) -> crs.addAll(c.getPendingChanges()));
        return crs;
    }

    @Override
    public synchronized List<ByteBuffer> getPendingWrites() {
        List<ByteBuffer> bbs = new ArrayList<>();
        connections.forEach((c) -> bbs.addAll(c.getPendingWrites()));
        return bbs;
    }
}
