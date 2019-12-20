package de.tum.i13.server.kv.handlers.kv;

import de.tum.i13.kvtp2.Message;
import de.tum.i13.kvtp2.MessageWriter;
import de.tum.i13.kvtp2.middleware.Handler;
import de.tum.i13.server.kv.replication.Replicator;
import de.tum.i13.shared.KVItem;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ReplicationHandler implements Handler {
    private static final Logger logger = Logger.getLogger(ReplicationHandler.class.getName());

    private Replicator replicator;

    public ReplicationHandler(Replicator replicator) {
        this.replicator = replicator;
    }

    @Override
    public void handle(MessageWriter writer, Message message) {
        try {
            replicator.replicate(new KVItem(message.get("key"), message.get("value")));
        } catch (InterruptedException e) {
            logger.log(Level.WARNING, "failed to replicate " + message.toString(), e);
        }
    }
}
