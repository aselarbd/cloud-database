package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.server.kv.replication.Replicator;
import de.tum.i13.shared.KVItem;
import de.tum.i13.shared.Log;

public class ReplicationHandler implements Handler {
    private static final Log logger = new Log(ReplicationHandler.class);

    private Replicator replicator;

    public ReplicationHandler(Replicator replicator) {
        this.replicator = replicator;
    }

    @Override
    public void handle(MessageWriter writer, Message message) {
        try {
            replicator.replicate(new KVItem(message.get("key"), message.get("value")));
        } catch (InterruptedException e) {
            logger.warning("failed to replicate " + message.toString(), e);
        }
    }
}
