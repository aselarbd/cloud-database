package de.tum.i13.kvtp2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public abstract class Connection {

    protected SelectionKey key;

    protected final List<ChangeRequest> pendingChanges;
    protected final List<ByteBuffer> pendingWrites;

    final AbstractSelectableChannel channel;

    public Connection(AbstractSelectableChannel channel) {
        this.pendingChanges = new LinkedList<>();
        this.pendingWrites = new ArrayList<>();
        this.channel = channel;
    }

    public synchronized List<ChangeRequest> getPendingChanges() {
        synchronized (pendingChanges) {
            List<ChangeRequest> pcs = new LinkedList<>(this.pendingChanges);
            this.pendingChanges.clear();
            return pcs;
        }
    }

    public synchronized List<ByteBuffer> getPendingWrites() {
        List<ByteBuffer> pws = new ArrayList<>(this.pendingWrites);
        this.pendingWrites.clear();
        return pws;
    }

    public void register(Selector selector, int ops) throws ClosedChannelException {
        this.key = channel.register(selector, ops, this);
    }


    // TODO: refactor Connection classes. Maybe use composition instead of inheritance to avoid this:
    void accept() throws IOException {
        throw new UnsupportedOperationException("accept not supported on " + this.getClass().getName());
    }

    void connect() throws IOException {
        throw new UnsupportedOperationException("accept not supported on " + this.getClass().getName());
    }

    void read() throws IOException {
        throw new UnsupportedOperationException("accept not supported on " + this.getClass().getName());
    }

    void write() throws IOException {
        throw new UnsupportedOperationException("accept not supported on " + this.getClass().getName());
    }

    StringWriter getStringWriter() {
        throw new UnsupportedOperationException("accept not supported on " + this.getClass().getName());
    }

    abstract void close() throws IOException;
}
