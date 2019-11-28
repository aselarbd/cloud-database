package de.tum.i13.kvtp2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public abstract class Connection {

    protected SelectionKey key;

    protected List<ChangeRequest> pendingChanges;
    protected List<ByteBuffer> pendingWrites;

    public Connection() {
        this.pendingChanges = new LinkedList<>();
        this.pendingWrites = new ArrayList<>();
    }

    public synchronized List<ChangeRequest> getPendingChanges() {
        List<ChangeRequest> pcs = this.pendingChanges;
        this.pendingChanges = new LinkedList<>();
        return pcs;
    }

    public synchronized List<ByteBuffer> getPendingWrites() {
        List<ByteBuffer> pws = this.pendingWrites;
        this.pendingWrites = new ArrayList<>();
        return pws;
    }

    void accept() throws IOException {
        throw new UnsupportedOperationException("accept not supported on " + this);
    }

    void connect() {
        throw new UnsupportedOperationException("accept not supported on " + this);
    }

    void read() throws IOException {
        throw new UnsupportedOperationException("accept not supported on " + this);
    }

    void write() throws IOException {
        throw new UnsupportedOperationException("accept not supported on " + this);
    }

}
