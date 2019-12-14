package de.tum.i13.kvtp2;

import java.nio.channels.SelectionKey;

public class ChangeRequest {

    public SelectionKey selectionKey;
    public final int ops;
    public Connection connection;

    public ChangeRequest(Connection connection, int ops) {
        this.connection = connection;
        this.ops = ops;
    }

    public ChangeRequest(SelectionKey selectionKey, int ops) {
        this.selectionKey = selectionKey;
        this.ops = ops;
    }
}